package ingest

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"graph-sub-pred-obj/model"
)

// CSVParser reads delimited files whose first row is a header naming the
// columns. Header names match the configured columns ignoring case and
// surrounding whitespace. With NoQuotes (TSV), each line is split at the
// delimiter and quotes are ordinary characters.
type CSVParser struct {
	Columns  Columns
	Comma    rune
	NoQuotes bool
}

// csvLayout is the header position of each configured column.
type csvLayout struct {
	subject, predicate, object int
	props                      []int
	propNames                  []string
}

// recordReader reads delimited records; InputOffset is the byte offset of the
// end of the last record read.
type recordReader interface {
	Read() ([]string, error)
	InputOffset() int64
}

func (p *CSVParser) newReader(r io.Reader) recordReader {
	if p.NoQuotes {
		return &splitReader{br: bufio.NewReaderSize(r, 1<<20), sep: string(p.Comma)}
	}
	cr := csv.NewReader(bufio.NewReaderSize(r, 1<<20))
	cr.Comma = p.Comma
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true
	return cr
}

// splitReader reads each non-empty line as a record split at sep, with no
// quoting.
type splitReader struct {
	br  *bufio.Reader
	sep string
	off int64
}

func (r *splitReader) Read() ([]string, error) {
	for {
		line, err := r.br.ReadString('\n')
		if line == "" {
			return nil, err
		}
		r.off += int64(len(line))
		if line = strings.TrimRight(line, "\r\n"); line != "" {
			return strings.Split(line, r.sep), nil
		}
	}
}

func (r *splitReader) InputOffset() int64 { return r.off }

// readHeader returns the header row of the file at path, or nil for an empty
// file.
func (p *CSVParser) readHeader(path string) ([]string, error) {
	r, _, err := OpenSplit(Split{Path: path, Whole: true})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	header, err := p.newReader(r).Read()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: header: %w", path, err)
	}
	return header, nil
}

func (p *CSVParser) layout(path string, header []string) (*csvLayout, error) {
	index := make(map[string][]int)
	for i, h := range header {
		if i == 0 {
			h = strings.TrimPrefix(h, "\uFEFF") // byte order mark
		}
		key := strings.ToLower(strings.TrimSpace(h))
		index[key] = append(index[key], i)
	}
	find := func(name string) (int, error) {
		switch idx := index[strings.ToLower(strings.TrimSpace(name))]; len(idx) {
		case 1:
			return idx[0], nil
		case 0:
			return 0, fmt.Errorf("%s: no column %q; header has %s", path, name, strings.Join(header, ", "))
		default:
			return 0, fmt.Errorf("%s: column %q appears %d times in the header", path, name, len(idx))
		}
	}
	l := &csvLayout{}
	var err error
	if l.subject, err = find(p.Columns.Subject); err != nil {
		return nil, err
	}
	if l.predicate, err = find(p.Columns.Predicate); err != nil {
		return nil, err
	}
	if l.object, err = find(p.Columns.Object); err != nil {
		return nil, err
	}
	for _, name := range p.Columns.Props {
		i, err := find(name)
		if err != nil {
			return nil, err
		}
		l.props = append(l.props, i)
		l.propNames = append(l.propNames, strings.TrimSpace(name))
	}
	return l, nil
}

func (p *CSVParser) Check(path string) (string, error) {
	header, err := p.readHeader(path)
	if err != nil {
		return "", err
	}
	if header == nil {
		return "empty file", nil
	}
	l, err := p.layout(path, header)
	if err != nil {
		return "", err
	}
	desc := fmt.Sprintf("subject=%q (column %d), predicate=%q (column %d), object=%q (column %d)",
		header[l.subject], l.subject+1, header[l.predicate], l.predicate+1, header[l.object], l.object+1)
	if len(l.propNames) > 0 {
		desc += ", props=" + strings.Join(l.propNames, ",")
	}
	return desc, nil
}

func (p *CSVParser) Parse(s Split, h Handler) error {
	r, base, err := OpenSplit(s)
	if err != nil {
		return err
	}
	defer r.Close()
	cr := p.newReader(r)

	var header []string
	if base == 0 {
		// The split starts the file, so its first row is the header
		if header, err = cr.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("%s: header: %w", s.Path, err)
		}
		header = append([]string(nil), header...)
	} else if header, err = p.readHeader(s.Path); err != nil {
		return err
	}
	l, err := p.layout(s.Path, header)
	if err != nil {
		return err
	}

	field := func(rec []string, i int) string {
		if i < len(rec) {
			return strings.TrimSpace(rec[i])
		}
		return ""
	}
	for {
		offset := base + cr.InputOffset()
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		h.Row()
		reject := func(reason string) error {
			return h.Reject(Reject{Path: s.Path, Offset: offset, Reason: reason, Record: strings.Join(rec, string(p.Comma))})
		}
		if err != nil {
			var pe *csv.ParseError
			if !errors.As(err, &pe) {
				return fmt.Errorf("%s: %w", s.Path, err)
			}
			if err := reject(pe.Err.Error()); err != nil {
				return err
			}
			continue
		}

		t := &model.Triple{
			Subject:   field(rec, l.subject),
			Predicate: field(rec, l.predicate),
			Object:    field(rec, l.object),
		}
		var missing string
		switch {
		case t.Subject == "":
			missing = p.Columns.Subject
		case t.Predicate == "":
			missing = p.Columns.Predicate
		case t.Object == "":
			missing = p.Columns.Object
		}
		if missing != "" {
			if err := reject(fmt.Sprintf("empty %s", missing)); err != nil {
				return err
			}
			continue
		}
		for j, i := range l.props {
			if v := field(rec, i); v != "" {
				if t.Props == nil {
					t.Props = make(map[string]interface{}, len(l.props))
				}
				t.Props[l.propNames[j]] = inferValue(v)
			}
		}
		if err := h.Triple(t); err != nil {
			return err
		}
	}
}
