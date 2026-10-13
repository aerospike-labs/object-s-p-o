package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"graph-sub-pred-obj/model"
)

// RDF datatypes of plain and language-tagged literals.
const (
	xsdString     = "http://www.w3.org/2001/XMLSchema#string"
	rdfLangString = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
)

// NTParser reads N-Triples. IRIs are stored without their angle brackets and
// blank nodes as "_:label". A literal object is stored as its value, with its
// datatype in the "datatype" prop (xsd:string when none is given) and its
// language tag, if any, in "lang"; only literals have a datatype.
type NTParser struct{}

func (NTParser) Check(string) (string, error) {
	return "N-Triples (subject, predicate, object by position)", nil
}

func (NTParser) Parse(s Split, h Handler) error {
	return readLines(s, func(offset int64, line []byte) error {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			return nil
		}
		h.Row()
		t, err := parseNTriple(string(line))
		if err != nil {
			return h.Reject(Reject{Path: s.Path, Offset: offset, Reason: err.Error(), Record: string(line)})
		}
		return h.Triple(t)
	})
}

// ntScanner walks one N-Triples line.
type ntScanner struct {
	s   string
	pos int
}

func (sc *ntScanner) skipSpace() {
	for sc.pos < len(sc.s) && (sc.s[sc.pos] == ' ' || sc.s[sc.pos] == '\t') {
		sc.pos++
	}
}

func (sc *ntScanner) peek() byte {
	if sc.pos < len(sc.s) {
		return sc.s[sc.pos]
	}
	return 0
}

func parseNTriple(line string) (*model.Triple, error) {
	sc := &ntScanner{s: line}
	t := &model.Triple{}
	var err error

	sc.skipSpace()
	switch sc.peek() {
	case '<':
		t.Subject, err = sc.iri()
	case '_':
		t.Subject, err = sc.blankNode()
	default:
		err = errors.New("subject must be an IRI or blank node")
	}
	if err != nil {
		return nil, err
	}

	sc.skipSpace()
	if sc.peek() != '<' {
		return nil, errors.New("predicate must be an IRI")
	}
	if t.Predicate, err = sc.iri(); err != nil {
		return nil, err
	}

	sc.skipSpace()
	switch sc.peek() {
	case '<':
		t.Object, err = sc.iri()
	case '_':
		t.Object, err = sc.blankNode()
	case '"':
		t.Object, t.Props, err = sc.literal()
	default:
		err = errors.New("object must be an IRI, blank node or literal")
	}
	if err != nil {
		return nil, err
	}

	sc.skipSpace()
	if sc.peek() != '.' {
		if sc.peek() == '<' || sc.peek() == '_' {
			return nil, errors.New("expected '.' after the object (N-Quads graph labels are not supported)")
		}
		return nil, errors.New("expected '.' after the object")
	}
	sc.pos++
	sc.skipSpace()
	if sc.pos < len(sc.s) && sc.s[sc.pos] != '#' {
		return nil, fmt.Errorf("unexpected text after '.': %q", sc.s[sc.pos:])
	}
	if t.Subject == "" || t.Predicate == "" {
		return nil, errors.New("empty IRI")
	}
	return t, nil
}

func (sc *ntScanner) iri() (string, error) {
	sc.pos++ // '<'
	end := strings.IndexByte(sc.s[sc.pos:], '>')
	if end < 0 {
		return "", errors.New("unterminated IRI")
	}
	raw := sc.s[sc.pos : sc.pos+end]
	sc.pos += end + 1
	if !strings.Contains(raw, `\`) {
		return raw, nil
	}
	return unescapeNT(raw, false)
}

func (sc *ntScanner) blankNode() (string, error) {
	if !strings.HasPrefix(sc.s[sc.pos:], "_:") {
		return "", errors.New("blank node must start with _:")
	}
	start := sc.pos
	sc.pos += 2
	for sc.pos < len(sc.s) && sc.s[sc.pos] != ' ' && sc.s[sc.pos] != '\t' {
		sc.pos++
	}
	// A '.' directly after the label ends the statement
	label := strings.TrimSuffix(sc.s[start:sc.pos], ".")
	sc.pos = start + len(label)
	if len(label) == 2 {
		return "", errors.New("empty blank node label")
	}
	return label, nil
}

func (sc *ntScanner) literal() (string, map[string]interface{}, error) {
	sc.pos++ // opening quote
	start := sc.pos
	escaped := false
	for {
		if sc.pos >= len(sc.s) {
			return "", nil, errors.New("unterminated literal")
		}
		c := sc.s[sc.pos]
		if c == '\\' {
			escaped = true
			sc.pos += 2
			continue
		}
		if c == '"' {
			break
		}
		sc.pos++
	}
	value := sc.s[start:sc.pos]
	sc.pos++ // closing quote
	if escaped {
		var err error
		if value, err = unescapeNT(value, true); err != nil {
			return "", nil, err
		}
	}

	props := map[string]interface{}{"datatype": xsdString}
	switch {
	case strings.HasPrefix(sc.s[sc.pos:], "^^"):
		sc.pos += 2
		if sc.peek() != '<' {
			return "", nil, errors.New("datatype must be an IRI")
		}
		dt, err := sc.iri()
		if err != nil {
			return "", nil, err
		}
		props["datatype"] = dt
	case sc.peek() == '@':
		sc.pos++
		start := sc.pos
		for sc.pos < len(sc.s) {
			c := sc.s[sc.pos]
			if !(c == '-' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
				break
			}
			sc.pos++
		}
		if sc.pos == start {
			return "", nil, errors.New("empty language tag")
		}
		props["datatype"] = rdfLangString
		props["lang"] = sc.s[start:sc.pos]
	}
	return value, props, nil
}

var ntEscapes = map[byte]byte{'t': '\t', 'b': '\b', 'n': '\n', 'r': '\r', 'f': '\f', '"': '"', '\'': '\'', '\\': '\\'}

// unescapeNT decodes \u and \U escapes, and in literals also \t \b \n \r \f
// \" \' and \\.
func unescapeNT(s string, literal bool) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if i+1 >= len(s) {
			return "", errors.New("dangling escape")
		}
		i++
		switch e := s[i]; {
		case e == 'u' || e == 'U':
			n := 4
			if e == 'U' {
				n = 8
			}
			if i+1+n > len(s) {
				return "", fmt.Errorf("short \\%c escape", e)
			}
			r, err := strconv.ParseUint(s[i+1:i+1+n], 16, 32)
			if err != nil {
				return "", fmt.Errorf("bad \\%c escape", e)
			}
			b.WriteRune(rune(r))
			i += n
		case !literal:
			return "", fmt.Errorf("bad escape \\%c in IRI", e)
		default:
			r, ok := ntEscapes[e]
			if !ok {
				return "", fmt.Errorf("bad escape \\%c", e)
			}
			b.WriteByte(r)
		}
	}
	return b.String(), nil
}
