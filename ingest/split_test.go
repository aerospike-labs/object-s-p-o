package ingest

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// readAllSplits returns every line read from the splits, in order.
func readAllSplits(t *testing.T, splits []Split) []string {
	t.Helper()
	var lines []string
	for _, s := range splits {
		err := readLines(s, func(_ int64, line []byte) error {
			lines = append(lines, string(line))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return lines
}

// Every line is read exactly once whatever the split size, including lines
// longer than a split, CRLF endings, empty lines and a missing final newline.
func TestSplitsReadEachLineOnce(t *testing.T) {
	var want []string
	for i := 0; i < 60; i++ {
		want = append(want, strings.Repeat(fmt.Sprintf("%c", 'a'+i%26), i%17))
	}
	for _, tc := range []struct{ name, sep, tail string }{
		{"lf", "\n", "\n"},
		{"crlf", "\r\n", "\r\n"},
		{"no final newline", "\n", ""},
	} {
		content := strings.Join(want, tc.sep) + tc.tail
		path := writeFile(t, t.TempDir(), "f.txt", content)
		for size := int64(1); size <= int64(len(content))+1; size++ {
			splits, err := PlanSplits([]string{path}, size, func(string) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			got := readAllSplits(t, splits)
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("%s, split size %d: got %d lines, want %d\ngot  %q\nwant %q", tc.name, size, len(got), len(want), got, want)
			}
		}
	}
}

func TestLineOffsets(t *testing.T) {
	path := writeFile(t, t.TempDir(), "f.txt", "ab\ncde\nf\n")
	splits, _ := PlanSplits([]string{path}, 4, func(string) bool { return true })
	var offsets []int64
	for _, s := range splits {
		readLines(s, func(off int64, _ []byte) error {
			offsets = append(offsets, off)
			return nil
		})
	}
	if fmt.Sprint(offsets) != "[0 3 7]" {
		t.Fatalf("offsets %v, want [0 3 7]", offsets)
	}
}

func TestClientSplitsPartition(t *testing.T) {
	var all []Split
	for i := 0; i < 10; i++ {
		all = append(all, Split{Path: fmt.Sprint(i)})
	}
	seen := map[string]int{}
	for c := 0; c < 3; c++ {
		for _, s := range ClientSplits(all, c, 3) {
			seen[s.Path]++
		}
	}
	if len(seen) != 10 {
		t.Fatalf("%d splits assigned, want 10", len(seen))
	}
	for p, n := range seen {
		if n != 1 {
			t.Fatalf("split %s assigned %d times", p, n)
		}
	}
}

func TestGzipAndUnsplittableAreWhole(t *testing.T) {
	dir := t.TempDir()
	plain := writeFile(t, dir, "a.csv", strings.Repeat("x,y,z\n", 100))
	gzPath := filepath.Join(dir, "b.nt.gz")
	f, _ := os.Create(gzPath)
	gz := gzip.NewWriter(f)
	gz.Write([]byte("l1\nl2\n"))
	gz.Close()
	f.Close()

	splits, err := PlanSplits([]string{plain, gzPath}, 10, func(p string) bool { return !strings.HasSuffix(p, ".csv") })
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) != 2 || !splits[0].Whole || !splits[1].Whole {
		t.Fatalf("got %v, want two whole splits", splits)
	}
	if got := readAllSplits(t, splits[1:]); strings.Join(got, ",") != "l1,l2" {
		t.Fatalf("gzip lines %q", got)
	}
}

func TestExpandInputs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "b.csv", "")
	writeFile(t, dir, "a.csv", "")
	writeFile(t, dir, ".hidden.csv", "")
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	writeFile(t, filepath.Join(dir, "sub"), "c.csv", "")

	files, err := ExpandInputs(dir + "," + filepath.Join(dir, "a.csv"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range files {
		rel, _ := filepath.Rel(dir, f)
		names = append(names, rel)
	}
	if got := strings.Join(names, ","); got != "a.csv,b.csv,sub/c.csv" {
		t.Fatalf("got %s", got)
	}
	if _, err := ExpandInputs(filepath.Join(dir, "*.json")); err == nil {
		t.Fatal("want an error for a pattern matching nothing")
	}
}
