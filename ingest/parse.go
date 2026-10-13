package ingest

import (
	"fmt"
	"strconv"
	"strings"

	"graph-sub-pred-obj/model"
)

// Columns names where a record's subject, predicate and object come from: CSV
// or TSV header names, or JSONL paths such as ".person.id". Props names
// further columns stored in the triple's props, keyed by the column name (for
// paths, without the leading dot).
type Columns struct {
	Subject, Predicate, Object string
	Props                      []string
}

// Handler receives what a Parser reads from a split. Returning an error from
// Triple or Reject stops parsing with that error.
type Handler struct {
	Triple func(*model.Triple) error
	Reject func(Reject) error
	Row    func() // called once per record read (row, document or line)
}

// Reject is a record that couldn't be turned into triples.
type Reject struct {
	Path   string
	Offset int64 // byte offset of the record in the file
	Reason string
	Record string
}

func (r Reject) String() string {
	rec := r.Record
	if len(rec) > 200 {
		rec = rec[:200] + "…"
	}
	return fmt.Sprintf("%s@%d: %s: %s", r.Path, r.Offset, r.Reason, rec)
}

// Parser turns the records of a split into triples.
type Parser interface {
	// Check validates that the file has the configured columns and describes
	// where each part of a triple comes from.
	Check(path string) (string, error)
	Parse(s Split, h Handler) error
}

// NewParser returns the parser for loadType: csv, tsv, jsonl, or nt. comma
// overrides the csv or tsv delimiter when non-zero.
func NewParser(loadType string, cols Columns, comma rune) (Parser, error) {
	needColumns := func() error {
		if cols.Subject == "" || cols.Predicate == "" || cols.Object == "" {
			return fmt.Errorf("-load_type %s needs -subject_column, -predicate_column and -object_column", loadType)
		}
		return nil
	}
	switch loadType {
	case "csv", "tsv":
		if err := needColumns(); err != nil {
			return nil, err
		}
		p := &CSVParser{Columns: cols, Comma: ',', NoQuotes: loadType == "tsv"}
		if loadType == "tsv" {
			p.Comma = '\t'
		}
		if comma != 0 {
			p.Comma = comma
		}
		return p, nil
	case "jsonl":
		if err := needColumns(); err != nil {
			return nil, err
		}
		return NewJSONLParser(cols)
	case "nt":
		if cols.Subject != "" || cols.Predicate != "" || cols.Object != "" || len(cols.Props) > 0 {
			return nil, fmt.Errorf("-load_type nt takes subject, predicate and object from fixed positions; drop the column flags")
		}
		return NTParser{}, nil
	}
	return nil, fmt.Errorf("unknown -load_type %q (want csv, tsv, jsonl, or nt)", loadType)
}

// inferValue returns s as an int64 or float64 when it is the canonical form
// of one, so "42" and "0.5" are stored as numbers while "007" and "1.50"
// stay strings.
func inferValue(s string) interface{} {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil && strconv.FormatInt(i, 10) == s {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && strconv.FormatFloat(f, 'f', -1, 64) == s {
		return f
	}
	return s
}

// SplitList splits a comma-separated flag value, dropping empty entries.
func SplitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
