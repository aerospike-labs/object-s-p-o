package lpg

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
)

// testStore connects to the cluster in $LPG_TEST_HOST (host:port), skipping
// the test when it is unset. Tests write vertex IDs under a per-run prefix,
// so they can share a namespace.
func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	seed := os.Getenv("LPG_TEST_HOST")
	if seed == "" {
		t.Skip("set LPG_TEST_HOST=host:port to run against Aerospike")
	}
	ns := os.Getenv("LPG_TEST_NAMESPACE")
	if ns == "" {
		ns = "test"
	}
	gs, err := (&store.ConnConfig{Hosts: seed, Port: 3000}).Connect(ns)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gs.Close)
	s, err := New(gs, 0)
	if err != nil {
		t.Fatal(err)
	}
	return s, fmt.Sprintf("t%d-", time.Now().UnixNano())
}

func TestRoundTrip(t *testing.T) {
	s, p := testStore(t)
	triples := []*model.Triple{
		{Subject: p + "INDIVIDUAL:1", Predicate: "IS_TYPE", Object: "INDIVIDUAL", Props: map[string]interface{}{"created": int64(10)}},
		{Subject: p + "INDIVIDUAL:1", Predicate: "HAS_IDENTIFIER", Object: "x",
			Props: map[string]interface{}{"identifier_type": "EMAIL_E", "value": p + "a@x", "created": int64(10), "last_seen": int64(11), "decay": 0.1}},
		{Subject: p + "INDIVIDUAL:1", Predicate: "LIVES_AT", Object: p + "ADDRESS:2", Props: map[string]interface{}{"created": int64(10), "last_seen": int64(12)}},
		{Subject: p + "ADDRESS:2", Predicate: "IS_TYPE", Object: "ADDRESS", Props: map[string]interface{}{"created": int64(10)}},
		{Subject: p + "HOUSEHOLD:3", Predicate: "IS_TYPE", Object: "HOUSEHOLD", Props: map[string]interface{}{"created": int64(10)}},
		{Subject: p + "HOUSEHOLD:3", Predicate: "HAS_AN", Object: p + "ADDRESS:2", Props: map[string]interface{}{"created": int64(10), "source": "crm", "score": 0.5}},
	}
	// Two INDIVIDUALs' worth in one batch, the second linking to the same
	// ADDRESS, so the batch merges two mutations of one vertex
	second := []*model.Triple{
		{Subject: p + "INDIVIDUAL:4", Predicate: "IS_TYPE", Object: "INDIVIDUAL", Props: map[string]interface{}{"created": int64(20)}},
		{Subject: p + "INDIVIDUAL:4", Predicate: "LIVES_AT", Object: p + "ADDRESS:2", Props: map[string]interface{}{"created": int64(20)}},
	}
	muts := append(FromTriples(triples, testVocab), FromTriples(second, testVocab)...)
	if err := s.Apply(muts); err != nil {
		t.Fatal(err)
	}

	ind, err := s.GetVertex(p + "INDIVIDUAL:1")
	if err != nil || ind == nil {
		t.Fatalf("GetVertex: %v %v", ind, err)
	}
	if ind.Type != "INDIVIDUAL" || ind.Created != 10 {
		t.Errorf("INDIVIDUAL type %q created %d", ind.Type, ind.Created)
	}
	if got := ind.Identifiers["EMAIL_E"][p+"a@x"]; got != (Identifier{10, 11, 0.1}) {
		t.Errorf("identifier = %+v", got)
	}
	if got := ind.Out["LIVES_AT"][p+"ADDRESS:2"]; got.Created != 10 || got.LastSeen != 12 {
		t.Errorf("out edge = %+v", got)
	}

	addr, err := s.GetVertex(p + "ADDRESS:2")
	if err != nil || addr == nil {
		t.Fatalf("GetVertex: %v %v", addr, err)
	}
	if addr.Type != "ADDRESS" || addr.Links != 2 || len(addr.In["LIVES_AT"]) != 2 || len(addr.In["HAS_AN"]) != 1 {
		t.Errorf("ADDRESS = %+v", addr)
	}
	if e := addr.In["HAS_AN"][p+"HOUSEHOLD:3"]; e.Props["source"] != "crm" || e.Props["score"] != 0.5 {
		t.Errorf("HAS_AN edge props = %v, want source and score", e.Props)
	}

	if missing, err := s.GetVertex(p + "NOPE:0"); err != nil || missing != nil {
		t.Errorf("missing vertex = %v, %v; want nil, nil", missing, err)
	}

	ids, err := s.LookupIdentifier("EMAIL_E", p+"a@x")
	if err != nil || len(ids) != 1 || ids[0] != p+"INDIVIDUAL:1" {
		t.Errorf("LookupIdentifier = %v, %v", ids, err)
	}
	if ids, err := s.LookupIdentifier("EMAIL_E", p+"nobody@x"); err != nil || len(ids) != 0 {
		t.Errorf("LookupIdentifier of an unknown value = %v, %v", ids, err)
	}

	// INDIVIDUAL:1 -> ADDRESS:2 <- {HOUSEHOLD:3, INDIVIDUAL:4}
	g, err := s.Traverse([]string{p + "INDIVIDUAL:1"}, 2, Both, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Vertices) != 4 || len(g.Edges) != 3 {
		t.Errorf("2-hop traversal: %d vertices, %d edges; want 4 and 3: %+v", len(g.Vertices), len(g.Edges), g.Edges)
	}
	g, err = s.Traverse([]string{p + "INDIVIDUAL:1"}, 2, Both, map[string]bool{"LIVES_AT": true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Edges) != 2 {
		t.Errorf("LIVES_AT-only traversal: %d edges, want 2: %+v", len(g.Edges), g.Edges)
	}
}

// TestHub writes a vertex's edges from concurrent writers in small batches,
// past its inline limit and through several levels of edge buckets, and
// checks every edge is found.
func TestHub(t *testing.T) {
	s, p := testStore(t)
	s.SetMaxInlineEdges(5)
	hub := p + "HOUSEHOLD:1"
	if err := s.Apply([]*Mutation{{ID: hub, Type: "HOUSEHOLD", Created: 1}}); err != nil {
		t.Fatal(err)
	}

	const writers, perWriter, perBatch = 8, 600, 20 // 4,800 edges: levels 0-1 at least
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i += perBatch {
				var triples []*model.Triple
				for j := i; j < i+perBatch; j++ {
					triples = append(triples, &model.Triple{Subject: fmt.Sprintf("%sINDIVIDUAL:%d", p, w*perWriter+j),
						Predicate: "IS_PART_OF", Object: hub, Props: map[string]interface{}{"created": int64(j)}})
				}
				if err := s.Apply(FromTriples(triples, testVocab)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	v, err := s.GetVertex(hub)
	if err != nil || v == nil {
		t.Fatalf("GetVertex: %v %v", v, err)
	}
	if !v.Hub || v.HubLevels["in"] < 1 {
		t.Errorf("hub = %v at levels %v; want incoming edges past level 0", v.Hub, v.HubLevels)
	}
	if _, ok := v.HubLevels["out"]; ok {
		t.Errorf("hub has an outgoing level %v, but no outgoing edges", v.HubLevels)
	}
	if v.Type != "HOUSEHOLD" || v.Links != writers*perWriter {
		t.Errorf("hub type %q, %d links; want HOUSEHOLD, %d", v.Type, v.Links, writers*perWriter)
	}

	edges, err := s.HubEdges(v, In, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range inlineEdges(v, In, nil) {
		seen[e.From] = true
	}
	for _, e := range edges {
		seen[e.From] = true
	}
	if len(seen) != writers*perWriter {
		t.Errorf("hub has %d distinct incoming edges, want %d", len(seen), writers*perWriter)
	}
	if n, err := s.Degree(hub, In); err != nil || n < writers*perWriter {
		t.Errorf("Degree = %d, %v; want at least %d", n, err, writers*perWriter)
	}

	page, err := s.HubEdges(v, In, nil, 100)
	if err != nil || len(page) != 100 {
		t.Errorf("HubEdges limit 100 = %d edges, %v", len(page), err)
	}

	// The edge is found from the INDIVIDUAL's side, and from the hub's
	ind := p + "INDIVIDUAL:4321"
	if ok, err := s.HasEdge(ind, "IS_PART_OF", hub); err != nil || !ok {
		t.Errorf("HasEdge from the INDIVIDUAL = %v, %v", ok, err)
	}
	if ok, err := s.HasEdge(ind, "IS_PART_OF", p+"HOUSEHOLD:2"); err != nil || ok {
		t.Errorf("HasEdge to another HOUSEHOLD = %v, %v; want false", ok, err)
	}
	for i := 0; i < writers*perWriter; i += 97 {
		ind := fmt.Sprintf("%sINDIVIDUAL:%d", p, i)
		if ok, err := s.HasInEdge(hub, "IS_PART_OF", ind); err != nil || !ok {
			t.Fatalf("HasInEdge on the hub from %s = %v, %v", ind, ok, err)
		}
	}
	if ok, err := s.HasInEdge(hub, "IS_PART_OF", p+"INDIVIDUAL:nobody"); err != nil || ok {
		t.Errorf("HasInEdge on the hub from a stranger = %v, %v; want false", ok, err)
	}

	// Hubs are reported, not expanded
	g, err := s.Traverse([]string{hub}, 1, Both, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Hubs) != 1 || len(g.Edges) != 0 {
		t.Errorf("traversal from a hub over max degree: hubs %v, %d edges", g.Hubs, len(g.Edges))
	}
	g, err = s.Traverse([]string{hub}, 1, In, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Vertices) != writers*perWriter+1 {
		t.Errorf("unlimited traversal from the hub reached %d vertices, want %d", len(g.Vertices), writers*perWriter+1)
	}
}

func TestScanShareable(t *testing.T) {
	s, p := testStore(t)
	typ := "T" + strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36)) // unique to this run
	var triples []*model.Triple
	for i, links := range []int{1, 2, 3} {
		id := fmt.Sprintf("%s%s:%d", p, typ, i)
		triples = append(triples, &model.Triple{Subject: id, Predicate: "IS_TYPE", Object: typ, Props: map[string]interface{}{"created": int64(1)}})
		for j := 0; j < links; j++ {
			triples = append(triples, &model.Triple{Subject: fmt.Sprintf("%sINDIVIDUAL:%d", p, j), Predicate: "LIVES_AT", Object: id})
		}
	}
	triples = append(triples, &model.Triple{Subject: p + typ + ":unlinked", Predicate: "IS_TYPE", Object: typ})
	for i := 0; i < 2; i++ { // writing the same links again doesn't count them again
		if err := s.Apply(FromTriples(triples, testVocab)); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	err := s.ScanShareable(0, store.PartitionCount, typ, 3, 0, func(e store.EntityLinks) error {
		got = append(got, fmt.Sprintf("%s=%d", strings.TrimPrefix(e.ID, p), e.Links))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{typ + ":0=1", typ + ":1=2"} // under the cap of 3, and linked
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("ScanShareable = %v, want %v", got, want)
	}
}

func TestPromoteHubAndTypes(t *testing.T) {
	s, p := testStore(t)
	hub := p + "HOUSEHOLD:pre"
	if err := s.Apply([]*Mutation{{ID: hub, Type: "HOUSEHOLD", Created: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PromoteHub(hub); err != nil {
		t.Fatal(err)
	}
	if err := s.PromoteHub(hub); err != nil { // repeatable
		t.Fatal(err)
	}
	muts := FromTriples([]*model.Triple{{Subject: p + "INDIVIDUAL:1", Predicate: "IS_PART_OF", Object: hub}}, testVocab)
	if err := s.Apply(muts); err != nil {
		t.Fatal(err)
	}
	v, err := s.GetVertex(hub)
	if err != nil || v == nil {
		t.Fatalf("GetVertex: %v %v", v, err)
	}
	if !v.Hub || len(v.In) != 0 {
		t.Errorf("promoted hub: hub %v, inline in edges %v; want its first edge bucketed", v.Hub, v.In)
	}
	if edges, err := s.HubEdges(v, In, nil, 0); err != nil || len(edges) != 1 {
		t.Errorf("HubEdges = %v, %v; want the one edge", edges, err)
	}

	typ := "T" + strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
	if err := s.Apply([]*Mutation{{ID: p + typ + ":1", Type: typ}, {ID: p + typ + ":2", Type: typ}}); err != nil {
		t.Fatal(err)
	}
	ids, err := s.VerticesByType(typ, 0)
	sort.Strings(ids)
	if err != nil || strings.Join(ids, ",") != p+typ+":1,"+p+typ+":2" {
		t.Errorf("VerticesByType = %v, %v", ids, err)
	}
}
