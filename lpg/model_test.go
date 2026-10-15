package lpg

import (
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
	if addr.Links != 1 {
		t.Errorf("ADDRESS:2 links = %d, want 1 (LIVES_AT counts, HAS_AN doesn't)", addr.Links)
	}
	if addr.Type != "" {
		t.Errorf("ADDRESS:2 type = %q; a vertex only linked to must not get a type", addr.Type)
	}
	if n := ind.edgeCount() + addr.edgeCount() + hh.edgeCount(); n != 4 {
		t.Errorf("edge entries = %d, want 4 (each edge on both vertices)", n)
	}
}

func TestMerge(t *testing.T) {
	a := &Mutation{ID: "ADDRESS:2", In: map[string]map[string]Edge{"LIVES_AT": {"INDIVIDUAL:1": {Created: 1, LastSeen: 1}}}, Links: 1}
	b := &Mutation{ID: "ADDRESS:2", Type: "ADDRESS", Created: 5,
		In: map[string]map[string]Edge{"LIVES_AT": {"INDIVIDUAL:7": {Created: 2, LastSeen: 2}}}, Links: 1}
	a.merge(b)
	if a.Type != "ADDRESS" || a.Created != 5 || a.Links != 2 || len(a.In["LIVES_AT"]) != 2 {
		t.Errorf("merged = %+v", a)
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
