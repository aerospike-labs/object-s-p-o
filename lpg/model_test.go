package lpg

import (
	"strings"
	"testing"

	"graph-sub-pred-obj/model"
)

var testVocab = Vocabulary{
	IsType:         "IS_TYPE",
	HasIdentifier:  "HAS_IDENTIFIER",
	LinkPredicates: map[string]bool{"LIVES_AT": true, "IS_PART_OF": true},
}

func TestFromTriples(t *testing.T) {
	triples := []*model.Triple{
		{Subject: "INDIVIDUAL:1", Predicate: "IS_TYPE", Object: "INDIVIDUAL", Props: map[string]interface{}{"created": int64(10)}},
		{Subject: "INDIVIDUAL:1", Predicate: "HAS_IDENTIFIER", Object: "EMAIL_E:a@x",
			Props: map[string]interface{}{"identifier_type": "EMAIL_E", "value": "a@x", "created": int64(10), "last_seen": int64(11), "decay": 0.1}},
		{Subject: "INDIVIDUAL:1", Predicate: "LIVES_AT", Object: "ADDRESS:2", Props: map[string]interface{}{"created": int64(10), "last_seen": int64(12)}},
		{Subject: "HOUSEHOLD:3", Predicate: "HAS_AN", Object: "ADDRESS:2", Props: map[string]interface{}{"created": int64(10)}},
	}
	muts := FromTriples(triples, testVocab)

	if len(muts) != 3 {
		t.Fatalf("got %d mutations, want 3 (INDIVIDUAL:1, ADDRESS:2, HOUSEHOLD:3)", len(muts))
	}
	ind, addr, hh := muts[0], muts[1], muts[2]
	if ind.ID != "INDIVIDUAL:1" || addr.ID != "ADDRESS:2" || hh.ID != "HOUSEHOLD:3" {
		t.Fatalf("mutations out of order: %s %s %s", ind.ID, addr.ID, hh.ID)
	}
	if ind.Type != "INDIVIDUAL" || ind.Created != 10 {
		t.Errorf("type %q created %d, want INDIVIDUAL 10", ind.Type, ind.Created)
	}
	if got := ind.Identifiers["EMAIL_E"]["a@x"]; got != (Identifier{10, 11, 0.1}) {
		t.Errorf("identifier = %+v", got)
	}
	if got := ind.Out["LIVES_AT"]["ADDRESS:2"]; got.Created != 10 || got.LastSeen != 12 || got.Props != nil {
		t.Errorf("out edge = %+v", got)
	}
	if _, ok := addr.In["LIVES_AT"]["INDIVIDUAL:1"]; !ok {
		t.Error("ADDRESS:2 is missing its incoming LIVES_AT edge")
	}
	if _, ok := addr.In["HAS_AN"]["HOUSEHOLD:3"]; !ok {
		t.Error("ADDRESS:2 is missing its incoming HAS_AN edge")
	}
	if !addr.LinkPreds["LIVES_AT"] || addr.LinkPreds["HAS_AN"] || addr.linkPreds()[0] != "LIVES_AT" {
		t.Errorf("ADDRESS:2 link predicates = %v, want LIVES_AT (LIVES_AT counts, HAS_AN doesn't)", addr.LinkPreds)
	}
	if addr.Type != "" {
		t.Errorf("ADDRESS:2 type = %q; a vertex only linked to must not get a type", addr.Type)
	}
	if n := ind.edgeCount() + addr.edgeCount() + hh.edgeCount(); n != 4 {
		t.Errorf("edge entries = %d, want 4 (each edge on both vertices)", n)
	}
}

func TestMerge(t *testing.T) {
	links := map[string]bool{"LIVES_AT": true}
	a := &Mutation{ID: "ADDRESS:2", In: map[string]map[string]Edge{"LIVES_AT": {"INDIVIDUAL:1": {Created: 3, LastSeen: 3}}}, LinkPreds: links}
	b := &Mutation{ID: "ADDRESS:2", Type: "ADDRESS", Created: 5,
		In: map[string]map[string]Edge{"LIVES_AT": {
			"INDIVIDUAL:7": {Created: 2, LastSeen: 2},
			"INDIVIDUAL:1": {Created: 1, LastSeen: 2, Props: map[string]interface{}{"src": "x"}},
		}}, LinkPreds: links}
	c := &Mutation{ID: "ADDRESS:2", Type: "OTHER", Created: 4}
	a.merge(b)
	a.merge(c)
	if a.Type != "ADDRESS" || a.Created != 4 || !a.LinkPreds["LIVES_AT"] || len(a.In["LIVES_AT"]) != 2 {
		t.Errorf("merged = %+v; want the first type, the earliest created, both edges", a)
	}
	if e := a.In["LIVES_AT"]["INDIVIDUAL:1"]; e.Created != 1 || e.LastSeen != 3 || e.Props["src"] != "x" {
		t.Errorf("edge written twice = %+v; want the earlier created, the later last_seen, and the props", e)
	}
}

func TestFromTriplesRDF(t *testing.T) {
	vocab := testVocab
	vocab.RDFType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	muts := FromTriples([]*model.Triple{
		{Subject: "http://x/alice", Predicate: vocab.RDFType, Object: "http://x/Person"},
		{Subject: "http://x/alice", Predicate: vocab.RDFType, Object: "http://x/Agent"},
		{Subject: "http://x/alice", Predicate: "http://x/nick", Object: "chat", Props: map[string]interface{}{"datatype": "x", "lang": "en"}},
		{Subject: "http://x/alice", Predicate: "http://x/nick", Object: "chat", Props: map[string]interface{}{"datatype": "x", "lang": "fr"}},
	}, vocab)
	if len(muts) != 1 {
		t.Fatalf("got %d mutations, want only alice: rdf:type is a type, not an edge to a class vertex", len(muts))
	}
	if muts[0].Type != "http://x/Person" {
		t.Errorf("type = %q, want the first rdf:type", muts[0].Type)
	}
	if nicks := muts[0].Identifiers["http://x/nick"]; len(nicks) != 2 {
		t.Errorf("nicks = %v, want chat@en and chat@fr kept apart", nicks)
	}
}

func TestIndexReadable(t *testing.T) {
	list := "ns=test:indexname=idx_other:set=x:bin=type:type=string:indextype=default:context=NULL:state=RW;" +
		"ns=test:indexname=idx_vertices_type:set=vertices:bin=type:type=string:indextype=default:context=NULL:state=WO;"
	if IndexReadable(list, LabelIndexName) {
		t.Error("an index still being built (state WO) counts as readable")
	}
	if !IndexReadable(strings.Replace(list, "state=WO", "state=RW", 1), LabelIndexName) {
		t.Error("a built index (state RW) doesn't count as readable")
	}
	if IndexReadable("", LabelIndexName) {
		t.Error("a missing index counts as readable")
	}
}

func TestBucketAt(t *testing.T) {
	i := 0
	for l := 0; l <= 3; l++ {
		for b := uint64(0); b < hubBuckets(l); b++ {
			if gl, gb := bucketAt(i); gl != l || gb != b {
				t.Fatalf("bucketAt(%d) = %d,%d; want %d,%d", i, gl, gb, l, b)
			}
			i++
		}
	}
	if bucketsUpTo(3) != i {
		t.Errorf("bucketsUpTo(3) = %d, want %d", bucketsUpTo(3), i)
	}
}

// TestHashGolden pins the edge bucket and identifier bucket hashes, which
// gremlin/ (AeroGraphHashTest) computes the same way to read this data.
func TestHashGolden(t *testing.T) {
	for _, c := range []struct {
		id          string
		l0, l5, l11 uint64
	}{
		{"INDIVIDUAL:1", 15, 319, 13119},
		{"HOUSEHOLD:HUB-0", 2, 130, 9346},
		{"BENCH-demo-INDIVIDUAL:4522699", 6, 6, 24582},
		{"é-ünïcode", 8, 88, 31320},
	} {
		if got := [3]uint64{hubBucket(c.id, 0), hubBucket(c.id, 5), hubBucket(c.id, 11)}; got != [3]uint64{c.l0, c.l5, c.l11} {
			t.Errorf("hubBucket(%q) at levels 0, 5, 11 = %v, want %d %d %d", c.id, got, c.l0, c.l5, c.l11)
		}
	}
	s := &Store{IDBuckets: 65536}
	if b, tag := s.identifierSlot("EMAIL_E", "EMAIL_E_100524_4c9dc9"); b != 43688 || tag != -8205386307581299651 {
		t.Errorf("identifierSlot = %d, %d; want 43688, -8205386307581299651", b, tag)
	}
}

func TestFromTriplesPropsAndLiterals(t *testing.T) {
	muts := FromTriples([]*model.Triple{
		{Subject: "C100", Predicate: "HAS_ACCOUNT", Object: "A100",
			Props: map[string]interface{}{"since": int64(2000), "score": 0.134}},
		{Subject: "http://x/alice", Predicate: "http://xmlns.com/foaf/0.1/name", Object: "Alice",
			Props: map[string]interface{}{"datatype": "xsd:string"}},
	}, testVocab)
	if len(muts) != 3 {
		t.Fatalf("got %d mutations, want 3 (C100, A100, alice)", len(muts))
	}
	e := muts[0].Out["HAS_ACCOUNT"]["A100"]
	if e.Props["since"] != int64(2000) || e.Props["score"] != 0.134 || len(e.Props) != 2 {
		t.Errorf("edge props = %v, want since and score", e.Props)
	}
	if _, ok := muts[2].Identifiers["http://xmlns.com/foaf/0.1/name"]["Alice"]; !ok || muts[2].Out != nil {
		t.Errorf("literal: identifiers %v, out %v; want a name property and no edge", muts[2].Identifiers, muts[2].Out)
	}
}
