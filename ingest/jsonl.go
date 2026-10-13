package ingest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"graph-sub-pred-obj/model"
)

// JSONLParser reads one JSON document per line. Columns are dotted paths such
// as ".person.id"; a path through or ending at an array yields one value per
// element, and a document yields one triple per combination of subject,
// predicate and object values.
type JSONLParser struct {
	subject, predicate, object jsonPath
	props                      []jsonPath
}

// jsonPath is a parsed dotted path: its keys, and the path as written.
type jsonPath struct {
	keys []string
	name string
}

func parseJSONPath(p string) (jsonPath, error) {
	name := strings.TrimPrefix(strings.TrimSpace(p), ".")
	keys := strings.Split(name, ".")
	for _, k := range keys {
		if k == "" {
			return jsonPath{}, fmt.Errorf("bad JSON path %q: want keys separated by dots, e.g. .person.id", p)
		}
	}
	return jsonPath{keys: keys, name: name}, nil
}

func (jp jsonPath) String() string { return "." + jp.name }

// NewJSONLParser parses the column paths.
func NewJSONLParser(cols Columns) (*JSONLParser, error) {
	p := &JSONLParser{}
	var err error
	if p.subject, err = parseJSONPath(cols.Subject); err != nil {
		return nil, err
	}
	if p.predicate, err = parseJSONPath(cols.Predicate); err != nil {
		return nil, err
	}
	if p.object, err = parseJSONPath(cols.Object); err != nil {
		return nil, err
	}
	for _, c := range cols.Props {
		jp, err := parseJSONPath(c)
		if err != nil {
			return nil, err
		}
		p.props = append(p.props, jp)
	}
	return p, nil
}

func (p *JSONLParser) Check(string) (string, error) {
	desc := fmt.Sprintf("subject=%s, predicate=%s, object=%s", p.subject, p.predicate, p.object)
	if len(p.props) > 0 {
		names := make([]string, len(p.props))
		for i, jp := range p.props {
			names[i] = jp.String()
		}
		desc += ", props=" + strings.Join(names, ",")
	}
	return desc, nil
}

func (p *JSONLParser) Parse(s Split, h Handler) error {
	return readLines(s, func(offset int64, line []byte) error {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			return nil
		}
		h.Row()
		reject := func(reason string) error {
			return h.Reject(Reject{Path: s.Path, Offset: offset, Reason: reason, Record: string(line)})
		}

		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var doc interface{}
		if err := dec.Decode(&doc); err != nil {
			return reject("invalid JSON: " + err.Error())
		}

		var parts [3][]string
		for i, jp := range []jsonPath{p.subject, p.predicate, p.object} {
			for _, v := range extractJSON(doc, jp.keys) {
				if s, ok := scalarString(v); ok && s != "" {
					parts[i] = append(parts[i], s)
				}
			}
			if len(parts[i]) == 0 {
				return reject(fmt.Sprintf("no string, number or boolean at %s", jp))
			}
		}

		var props map[string]interface{}
		for _, jp := range p.props {
			vals := extractJSON(doc, jp.keys)
			if len(vals) == 0 {
				continue
			}
			if props == nil {
				props = make(map[string]interface{}, len(p.props))
			}
			if len(vals) == 1 {
				props[jp.name] = jsonValue(vals[0])
			} else {
				props[jp.name] = jsonValue(vals)
			}
		}

		for _, sub := range parts[0] {
			for _, pred := range parts[1] {
				for _, obj := range parts[2] {
					// Triples of a document share its props, which are never modified
					t := &model.Triple{Subject: sub, Predicate: pred, Object: obj, Props: props}
					if err := h.Triple(t); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

// extractJSON returns the values at keys under v. Arrays along the way, and
// an array at the end, contribute each of their elements; null and missing
// keys contribute nothing.
func extractJSON(v interface{}, keys []string) []interface{} {
	switch t := v.(type) {
	case []interface{}:
		var out []interface{}
		for _, e := range t {
			out = append(out, extractJSON(e, keys)...)
		}
		return out
	case nil:
		return nil
	}
	if len(keys) == 0 {
		return []interface{}{v}
	}
	if m, ok := v.(map[string]interface{}); ok {
		return extractJSON(m[keys[0]], keys[1:])
	}
	return nil
}

func scalarString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t), true
	case json.Number:
		return t.String(), true
	case bool:
		return strconv.FormatBool(t), true
	}
	return "", false
}

// jsonValue converts a decoded JSON value for storing in props: numbers become
// int64 or float64, and objects and arrays are converted throughout.
func jsonValue(v interface{}) interface{} {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = jsonValue(e)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, e := range t {
			out[k] = jsonValue(e)
		}
		return out
	}
	return v
}

// readLines calls fn with each line of the split (without its line ending)
// and the line's byte offset, stopping at the first error fn returns.
func readLines(s Split, fn func(offset int64, line []byte) error) error {
	r, base, err := OpenSplit(s)
	if err != nil {
		return err
	}
	defer r.Close()
	br := bufio.NewReaderSize(r, 1<<20)
	offset := base
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			next := offset + int64(len(line))
			if err := fn(offset, bytes.TrimRight(line, "\r\n")); err != nil {
				return err
			}
			offset = next
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", s.Path, err)
		}
	}
}
