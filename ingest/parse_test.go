package ingest

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"graph-sub-pred-obj/model"
)

type parsed struct {
	triples []*model.Triple
	rejects []Reject
	rows    int
}

func (p *parsed) handler() Handler {
	return Handler{
		Row: func() { p.rows++ },
		Triple: func(t *model.Triple) error {
			p.triples = append(p.triples, t)
			return nil
		},
		Reject: func(r Reject) error {
			p.rejects = append(p.rejects, r)
			return nil
		},
	}
}

func (p *parsed) spo() []string {
	var out []string
	for _, t := range p.triples {
		out = append(out, t.Subject+" "+t.Predicate+" "+t.Object)
	}
	return out
}

// parseAll parses the file at path in splits of size bytes.
func parseAll(t *testing.T, parser Parser, path string, size int64) *parsed {
	t.Helper()
	splits, err := PlanSplits([]string{path}, size, func(string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	p := &parsed{}
	for _, s := range splits {
		if err := parser.Parse(s, p.handler()); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestCSVColumns(t *testing.T) {
	content := "\uFEFFCust_ID, Rel_Type ,acct_id,since,zip,score\n" +
		"c1,OWNS,a1,2024,02134,0.5\n" +
		"c2,OWNS,,2024,,\n" +
		"\"c,3\",OWNS,a3,,,\n"
	path := writeFile(t, t.TempDir(), "f.csv", content)
	parser, err := NewParser("csv", Columns{Subject: "cust_id", Predicate: "rel_type", Object: "ACCT_ID", Props: []string{"since", "zip", "score"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parser.Check(path); err != nil {
		t.Fatal(err)
	}

	// Every split size gives the same result, with the header read once
	for size := int64(1); size <= int64(len(content)); size += 3 {
		p := parseAll(t, parser, path, size)
		if got := strings.Join(p.spo(), "|"); got != "c1 OWNS a1|c,3 OWNS a3" {
			t.Fatalf("size %d: got %s", size, got)
		}
		if p.rows != 3 || len(p.rejects) != 1 || p.rejects[0].Reason != "empty ACCT_ID" {
			t.Fatalf("size %d: rows %d, rejects %v", size, p.rows, p.rejects)
		}
		want := map[string]interface{}{"since": int64(2024), "zip": "02134", "score": 0.5}
		if !reflect.DeepEqual(p.triples[0].Props, want) {
			t.Fatalf("props %v, want %v", p.triples[0].Props, want)
		}
		if p.triples[1].Props != nil {
			t.Fatalf("empty props columns should leave props unset, got %v", p.triples[1].Props)
		}
	}
}

func TestCSVHeaderErrors(t *testing.T) {
	path := writeFile(t, t.TempDir(), "f.csv", "a,b,b\n1,2,3\n")
	for predicate, want := range map[string]string{
		"b": `column "b" appears 2 times`,
		"x": `no column "x"; header has a, b, b`,
	} {
		parser, _ := NewParser("csv", Columns{Subject: "a", Predicate: predicate, Object: "a"}, 0)
		if _, err := parser.Check(path); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: got %v, want %q", predicate, err, want)
		}
	}
	if _, err := NewParser("csv", Columns{Subject: "a"}, 0); err == nil {
		t.Fatal("want an error when columns are missing")
	}
}

func TestTSVAndDelimiter(t *testing.T) {
	path := writeFile(t, t.TempDir(), "f.tsv", "s\tp\to\nx\t\"q\ty\n")
	parser, _ := NewParser("tsv", Columns{Subject: "s", Predicate: "p", Object: "o"}, 0)
	if got := parseAll(t, parser, path, 0).spo(); fmt.Sprint(got) != `[x "q y]` {
		t.Fatalf("tsv: %q", got)
	}
	path = writeFile(t, t.TempDir(), "f.psv", "s|p|o\nx|q|y\n")
	parser, _ = NewParser("csv", Columns{Subject: "s", Predicate: "p", Object: "o"}, '|')
	if got := parseAll(t, parser, path, 0).spo(); fmt.Sprint(got) != "[x q y]" {
		t.Fatalf("pipe: %q", got)
	}
}

func TestJSONLPaths(t *testing.T) {
	content := `{"person":{"id":"p1"},"link":{"kind":"OWNS"},"target":{"id":42},"meta":{"since":2024,"score":0.5,"tags":["a","b"]}}
{"person":{"id":"p2"},"link":{"kind":"USES"},"target":[{"id":"d1"},{"id":"d2"}]}

{"person":{"id":"p3"},"link":{"kind":"OWNS"},"target":{"id":{"nested":1}}}
not json
{"person":{"id":"p4"},"link":{"kind":"OWNS"}}
`
	path := writeFile(t, t.TempDir(), "f.jsonl", content)
	parser, err := NewParser("jsonl", Columns{
		Subject: ".person.id", Predicate: ".link.kind", Object: ".target.id",
		Props: []string{".meta.since", "meta.score", ".meta.tags"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for size := int64(1); size <= int64(len(content)); size += 7 {
		p := parseAll(t, parser, path, size)
		if got := strings.Join(p.spo(), "|"); got != "p1 OWNS 42|p2 USES d1|p2 USES d2" {
			t.Fatalf("size %d: got %s", size, got)
		}
		want := map[string]interface{}{"meta.since": int64(2024), "meta.score": 0.5, "meta.tags": []interface{}{"a", "b"}}
		if !reflect.DeepEqual(p.triples[0].Props, want) {
			t.Fatalf("props %#v, want %#v", p.triples[0].Props, want)
		}
		var reasons []string
		for _, r := range p.rejects {
			reasons = append(reasons, r.Reason)
		}
		sort.Strings(reasons)
		if p.rows != 5 || len(reasons) != 3 ||
			reasons[0] != "invalid JSON: invalid character 'o' in literal null (expecting 'u')" ||
			reasons[1] != "no string, number or boolean at .target.id" ||
			reasons[2] != "no string, number or boolean at .target.id" {
			t.Fatalf("size %d: rows %d, rejects %q", size, p.rows, reasons)
		}
	}
	if _, err := NewParser("jsonl", Columns{Subject: ".a..b", Predicate: "p", Object: "o"}, 0); err == nil {
		t.Fatal("want an error for an empty path key")
	}
}

func TestNTriples(t *testing.T) {
	content := `# comment
<http://ex/s> <http://ex/p> <http://ex/o> .
_:b1 <http://ex/name> "Al \"the\" ét\\n"@en-US . # trailing
<http://ex/s> <http://ex/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer>.
<http://ex/s> <http://ex/note> "plain" .
<http://ex/s> <http://ex/o> _:b2.
<http://ex/s> <http://ex/p> <http://ex/o> <http://ex/g> .
"lit" <http://ex/p> <http://ex/o> .
<http://ex/s> <http://ex/p> "open .
`
	path := writeFile(t, t.TempDir(), "f.nt", content)
	parser, err := NewParser("nt", Columns{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := parseAll(t, parser, path, 0)
	want := []string{
		"http://ex/s http://ex/p http://ex/o",
		`_:b1 http://ex/name Al "the" ét\n`,
		"http://ex/s http://ex/age 42",
		"http://ex/s http://ex/note plain",
		"http://ex/s http://ex/o _:b2",
	}
	if got := p.spo(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	if p.triples[0].Props != nil {
		t.Fatalf("IRI object has props %v", p.triples[0].Props)
	}
	for i, wantProps := range []map[string]interface{}{
		{"datatype": rdfLangString, "lang": "en-US"},
		{"datatype": "http://www.w3.org/2001/XMLSchema#integer"},
		{"datatype": xsdString},
	} {
		if got := p.triples[i+1].Props; !reflect.DeepEqual(got, wantProps) {
			t.Fatalf("triple %d props %v, want %v", i+1, got, wantProps)
		}
	}
	if p.rows != 8 || len(p.rejects) != 3 || !strings.Contains(p.rejects[0].Reason, "N-Quads") {
		t.Fatalf("rows %d, rejects %v", p.rows, p.rejects)
	}
	if _, err := NewParser("nt", Columns{Subject: "s"}, 0); err == nil {
		t.Fatal("want an error for column flags with nt")
	}
}

func TestInferValue(t *testing.T) {
	for in, want := range map[string]interface{}{
		"42": int64(42), "-7": int64(-7), "0.5": 0.5, "007": "007", "1.50": "1.50",
		"1e3": "1e3", "abc": "abc", "true": "true",
	} {
		if got := inferValue(in); got != want {
			t.Errorf("inferValue(%q) = %#v, want %#v", in, got, want)
		}
	}
}

func TestWriterBatches(t *testing.T) {
	var sizes []int
	var total int
	ch := make(chan int, 100)
	w := NewWriter(func(b []*model.Triple) error {
		ch <- len(b)
		return nil
	}, 1, 4)
	for i := 0; i < 5; i++ {
		w.Add([]*model.Triple{{}, {}, {}})
	}
	w.Close()
	close(ch)
	for n := range ch {
		sizes = append(sizes, n)
		total += n
	}
	if fmt.Sprint(sizes) != "[4 4 4 3]" || w.Written() != 15 || total != 15 {
		t.Fatalf("batches %v, written %d", sizes, w.Written())
	}
}
