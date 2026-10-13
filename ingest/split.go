package ingest

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Split is a unit of loading work: the lines of a file that start in bytes
// [Start, End). Adjacent splits of a file share a boundary and each line
// belongs to exactly one of them, so splits can be read in parallel on any
// number of processes. A whole split covers an entire file that can't be cut
// by byte offset (gzip, or CSV with quoted newlines).
type Split struct {
	Path       string
	Start, End int64
	Whole      bool
}

func (s Split) String() string {
	if s.Whole {
		return s.Path
	}
	return fmt.Sprintf("%s[%d:%d]", s.Path, s.Start, s.End)
}

// IsGzip reports whether path is read through gzip.
func IsGzip(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".gz")
}

// ExpandInputs turns a comma-separated list of files, globs and directories
// into a sorted, de-duplicated list of files. Directories are walked
// recursively, skipping hidden files and directories. Every process given the
// same inputs gets the same list, so splits can be divided between them.
func ExpandInputs(list string) ([]string, error) {
	seen := make(map[string]bool)
	var files []string
	add := func(path string) {
		if !seen[path] {
			seen[path] = true
			files = append(files, path)
		}
	}
	for _, pattern := range strings.Split(list, ",") {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("bad input pattern %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no files match %q", pattern)
		}
		for _, m := range matches {
			info, err := os.Stat(m)
			if err != nil {
				return nil, err
			}
			if !info.IsDir() {
				add(m)
				continue
			}
			err = filepath.WalkDir(m, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if path != m && strings.HasPrefix(d.Name(), ".") {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				if d.Type().IsRegular() {
					add(path)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	sort.Strings(files)
	return files, nil
}

// PlanSplits cuts each file into splits of about size bytes, in a
// deterministic order. Files for which splittable returns false, gzip files,
// and all files when size <= 0 are one whole split each.
func PlanSplits(files []string, size int64, splittable func(path string) bool) ([]Split, error) {
	var splits []Split
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		n := info.Size()
		if size <= 0 || IsGzip(path) || !splittable(path) || n <= size {
			splits = append(splits, Split{Path: path, End: n, Whole: true})
			continue
		}
		for start := int64(0); start < n; start += size {
			splits = append(splits, Split{Path: path, Start: start, End: min(start+size, n)})
		}
	}
	return splits, nil
}

// ClientSplits returns the splits assigned to client index of count:
// every count-th split, starting at index.
func ClientSplits(splits []Split, index, count int) []Split {
	var mine []Split
	for i := index; i < len(splits); i += count {
		mine = append(mine, splits[i])
	}
	return mine
}

// OpenSplit returns a reader over the lines of s and the file offset its first
// byte is at. A whole gzip split is decompressed, and offsets are then into the
// decompressed data.
func OpenSplit(s Split) (io.ReadCloser, int64, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, 0, err
	}
	if s.Whole {
		if !IsGzip(s.Path) {
			return f, 0, nil
		}
		gz, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
		if err != nil {
			f.Close()
			return nil, 0, fmt.Errorf("%s: %w", s.Path, err)
		}
		return &gzipFile{gz, f}, 0, nil
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	start, err := lineStart(f, s.Start, info.Size())
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	end, err := lineStart(f, s.End, info.Size())
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return &sectionFile{io.NewSectionReader(f, start, end-start), f}, start, nil
}

// lineStart returns the offset of the first line starting at or after off:
// off itself when it follows a newline, else just past the next newline.
func lineStart(f *os.File, off, size int64) (int64, error) {
	if off <= 0 {
		return 0, nil
	}
	if off >= size {
		return size, nil
	}
	r := bufio.NewReaderSize(io.NewSectionReader(f, off-1, size-off+1), 64<<10)
	pos := off - 1
	for {
		chunk, err := r.ReadSlice('\n')
		pos += int64(len(chunk))
		switch {
		case err == nil:
			return pos, nil
		case errors.Is(err, io.EOF):
			return size, nil
		case !errors.Is(err, bufio.ErrBufferFull):
			return 0, err
		}
	}
}

type gzipFile struct {
	*gzip.Reader
	f *os.File
}

func (g *gzipFile) Close() error {
	g.Reader.Close()
	return g.f.Close()
}

type sectionFile struct {
	*io.SectionReader
	f *os.File
}

func (s *sectionFile) Close() error { return s.f.Close() }
