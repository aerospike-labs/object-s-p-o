package lpg

import (
	"fmt"
	"testing"

	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// intBin reads one integer bin of a vertex record.
func intBin(t *testing.T, s *Store, id, bin string) int {
	t.Helper()
	key, _ := s.vertexKey(id)
	rec, err := s.gs.Client.Get(nil, key, bin)
	if err != nil {
		t.Fatalf("read %s of %s: %v", bin, id, err)
	}
	n, _ := rec.Bins[bin].(int)
	return n
}

// keyStored reports whether a vertex record stores its key, which scans
// need to return it.
func keyStored(t *testing.T, s *Store, id string) bool {
	t.Helper()
	qp := aero.NewQueryPolicy()
	qp.FilterExpression = aero.ExpAnd(aero.ExpKeyExists(), aero.ExpEq(aero.ExpKey(aero.ExpTypeSTRING), aero.ExpStringVal(id)))
	rs, err := s.gs.Client.Query(qp, aero.NewStatement(s.gs.Namespace, VertexSet))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	n := 0
	for r := range rs.Results() {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		n++
	}
	return n == 1
}

// allEdges returns a vertex's edges in a direction, inline and bucketed,
// each as many times as it is stored.
func allEdges(t *testing.T, s *Store, id string, dir Direction) []EdgeRef {
	t.Helper()
	v, err := s.GetVertex(id)
	if err != nil || v == nil {
		t.Fatalf("GetVertex(%s): %v %v", id, v, err)
	}
	edges := inlineEdges(v, dir, nil)
	more, err := s.HubEdges(v, dir, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return append(edges, more...)
}

// TestRefreshKeepsCounts writes the same edges again and again with newer
// last_seen times, as an identity graph's daily refresh does. The vertex
// must not fill up and become a hub, its counts must stay exact, and each
// edge must keep its first created time and its latest last_seen.
func TestRefreshKeepsCounts(t *testing.T) {
	s, p := testStore(t)
	s.SetMaxInlineEdges(10)
	ind, addr := p+"INDIVIDUAL:1", p+"ADDRESS:1"
	for day := int64(1); day <= 20; day++ {
		triples := []*model.Triple{
			{Subject: ind, Predicate: "IS_TYPE", Object: "INDIVIDUAL", Props: map[string]interface{}{"created": 100 + day}},
			{Subject: ind, Predicate: "LIVES_AT", Object: addr, Props: map[string]interface{}{"created": 100 + day, "last_seen": 100 + day, "src": fmt.Sprint("d", day)}},
		}
		for j := 0; j < 3; j++ {
			triples = append(triples, &model.Triple{Subject: ind, Predicate: "HAS_AN", Object: fmt.Sprintf("%sEMAIL:%d", p, j),
				Props: map[string]interface{}{"created": 100 + day, "last_seen": 100 + day}})
		}
		if day == 7 {
			triples[1].Props = map[string]interface{}{"created": int64(50), "last_seen": int64(60), "first": true} // a late, older observation
		}
		if err := s.Apply(FromTriples(triples, testVocab)); err != nil {
			t.Fatal(err)
		}
	}

	v, err := s.GetVertex(ind)
	if err != nil || v == nil {
		t.Fatalf("GetVertex: %v %v", v, err)
	}
	if v.Hub {
		t.Errorf("INDIVIDUAL with 4 edges refreshed 20 times became a hub")
	}
	if n := intBin(t, s, ind, binEdges); n != 4 {
		t.Errorf("edge_count = %d, want 4", n)
	}
	if v.Created != 101 {
		t.Errorf("vertex created = %d, want the earliest, 101", v.Created)
	}
	e := v.Out["LIVES_AT"][addr]
	if e.Created != 50 || e.LastSeen != 120 || e.Props["src"] != "d20" || e.Props["first"] != true {
		t.Errorf("refreshed edge = %+v; want created 50, last_seen 120, props merged", e)
	}
	a, err := s.GetVertex(addr)
	if err != nil || a == nil {
		t.Fatalf("GetVertex: %v %v", a, err)
	}
	if a.Links != 1 || intBin(t, s, addr, binEdges) != 1 {
		t.Errorf("ADDRESS links %d, edge_count %d; want 1 and 1", a.Links, intBin(t, s, addr, binEdges))
	}
}

// TestNewVertexManyEdges gives a vertex that doesn't exist yet, and gets
// nothing but edges, more edges in one batch than fit inline.
func TestNewVertexManyEdges(t *testing.T) {
	s, p := testStore(t)
	acct := p + "ACCOUNT:1"
	var triples []*model.Triple
	for i := 0; i < 300; i++ {
		triples = append(triples, &model.Triple{Subject: fmt.Sprintf("%sCUST:%d", p, i), Predicate: "HAS_ACCOUNT", Object: acct,
			Props: map[string]interface{}{"created": int64(i)}})
	}
	if err := s.Apply(FromTriples(triples, testVocab)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n := len(allEdges(t, s, acct, In)); n != 300 {
		t.Errorf("account has %d incoming edges, want 300", n)
	}
	if !keyStored(t, s, acct) {
		t.Error("hub vertex record doesn't store its key, so scans skip it")
	}

	pre := p + "HOUSEHOLD:pre"
	if err := s.PromoteHub(pre); err != nil {
		t.Fatal(err)
	}
	if !keyStored(t, s, pre) {
		t.Error("vertex created by PromoteHub doesn't store its key")
	}
}

// TestHubRewriteNoDuplicates writes a hub's edges, lets it move up levels,
// then writes the same edges again: each must still be stored once, and the
// degree must be exact.
func TestHubRewriteNoDuplicates(t *testing.T) {
	s, p := testStore(t)
	s.SetMaxInlineEdges(5)
	hub := p + "HOUSEHOLD:1"
	write := func(from, to int, day int64) {
		var triples []*model.Triple
		for i := from; i < to; i++ {
			triples = append(triples, &model.Triple{Subject: fmt.Sprintf("%sINDIVIDUAL:%d", p, i), Predicate: "IS_PART_OF", Object: hub,
				Props: map[string]interface{}{"created": day, "last_seen": day}})
		}
		if err := s.Apply(FromTriples(triples, testVocab)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 6000; i += 100 {
		write(i, i+100, 1)
	}
	v, err := s.GetVertex(hub)
	if err != nil || v == nil || v.HubLevels["in"] < 1 {
		t.Fatalf("hub %+v, %v; want incoming edges past level 0", v, err)
	}
	for i := 0; i < 6000; i += 500 { // inline and every level, after the moves
		write(i, i+500, 2)
	}
	edges := allEdges(t, s, hub, In)
	seen := map[string]int{}
	for _, e := range edges {
		seen[e.From]++
	}
	if len(edges) != 6000 || len(seen) != 6000 {
		t.Errorf("hub stores %d edges for %d neighbors, want 6000 and 6000", len(edges), len(seen))
	}
	if n, err := s.Degree(hub, In); err != nil || n != 6000 {
		t.Errorf("Degree = %d, %v; want exactly 6000", n, err)
	}
	if v, _ = s.GetVertex(hub); v.Links != 6000 {
		t.Errorf("hub links = %d, want 6000", v.Links)
	}
}

// TestIdentifierFanOut shares one identifier among more vertices than the
// lookup set keeps for it. Writes must keep working, the bucket must stay
// bounded, and a lookup must still find every vertex.
func TestIdentifierFanOut(t *testing.T) {
	s, p := testStore(t)
	value := p + "000-000-0000"
	var muts []*Mutation
	n := MaxIdentifierShare + 50
	for i := 0; i < n; i++ {
		m := &Mutation{ID: fmt.Sprintf("%sINDIVIDUAL:%d", p, i)}
		m.addIdentifier("PHONE", value, Identifier{Created: 1, LastSeen: 1})
		muts = append(muts, m)
		if len(muts) == 200 || i == n-1 {
			if err := s.Apply(muts); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			muts = nil
		}
	}
	bucket, tag := s.identifierSlot("PHONE", value)
	key, _ := s.bucketKey(bucket)
	rec, aerr := s.gs.Client.Operate(nil, key, aero.MapSizeOp(binBucket, aero.CtxMapKey(aero.NewIntegerValue(int(tag)))))
	if aerr != nil {
		t.Fatal(aerr)
	}
	if size, _ := rec.Bins[binBucket].(int); size != MaxIdentifierShare {
		t.Errorf("lookup entry holds %d vertices, want it capped at %d", size, MaxIdentifierShare)
	}
	ids, err := s.LookupIdentifier("PHONE", value)
	if err != nil || len(ids) != n {
		t.Errorf("LookupIdentifier found %d vertices, %v; want %d", len(ids), err, n)
	}
}

// TestIdentifierIndexOff writes identifiers without the lookup set. Lookups
// must then scan rather than miss them.
func TestIdentifierIndexOff(t *testing.T) {
	s, p := testStore(t)
	t.Cleanup(func() {
		key, _ := aero.NewKey(s.gs.Namespace, store.MetaSetName, IDIndexOffName)
		s.gs.Client.Delete(nil, key)
	})
	if err := s.SetIndexIdentifiers(false); err != nil {
		t.Fatal(err)
	}
	m := &Mutation{ID: p + "INDIVIDUAL:1", Type: "INDIVIDUAL"}
	m.addIdentifier("EMAIL_E", p+"a@x", Identifier{Created: 1})
	if err := s.Apply([]*Mutation{m}); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.IdentifierIndexComplete(); err != nil || ok {
		t.Errorf("IdentifierIndexComplete = %v, %v; want false once a writer skipped the index", ok, err)
	}
	if ids, err := s.LookupIdentifier("EMAIL_E", p+"a@x"); err != nil || len(ids) != 1 {
		t.Errorf("LookupIdentifier = %v, %v; want the vertex, found by scanning", ids, err)
	}
}

// TestHasEdgeAbsent asks about edges whose predicate, or whole edge bin,
// the vertex doesn't have.
func TestHasEdgeAbsent(t *testing.T) {
	s, p := testStore(t)
	ind := p + "INDIVIDUAL:1"
	if err := s.Apply(FromTriples([]*model.Triple{{Subject: ind, Predicate: "LIVES_AT", Object: p + "ADDRESS:1"}}, testVocab)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasEdge(ind, "HAS_AN", p+"ADDRESS:1"); err != nil || ok {
		t.Errorf("HasEdge with an absent predicate = %v, %v; want false, nil", ok, err)
	}
	if ok, err := s.HasInEdge(ind, "LIVES_AT", p+"ADDRESS:1"); err != nil || ok {
		t.Errorf("HasInEdge on a vertex without incoming edges = %v, %v; want false, nil", ok, err)
	}
	if ok, err := s.HasEdge(ind, "LIVES_AT", p+"ADDRESS:1"); err != nil || !ok {
		t.Errorf("HasEdge = %v, %v; want true", ok, err)
	}
}
