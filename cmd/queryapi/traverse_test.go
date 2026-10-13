package main

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"graph-sub-pred-obj/model"
)

// memFetcher answers Fetch from an in-memory list of triples.
type memFetcher struct {
	triples []*model.Triple
	calls   atomic.Int64
}

func (m *memFetcher) Fetch(role, value string, predicates []string, limit int64) ([]*model.Triple, error) {
	m.calls.Add(1)
	var out []*model.Triple
	for _, t := range m.triples {
		v := map[string]string{RoleSubject: t.Subject, RolePredicate: t.Predicate, RoleObject: t.Object}[role]
		if v != value || (len(predicates) > 0 && !contains(predicates, t.Predicate)) {
			continue
		}
		out = append(out, t)
		if int64(len(out)) == limit {
			break
		}
	}
	return out, nil
}

func tr(s, p, o string) *model.Triple { return &model.Triple{Subject: s, Predicate: p, Object: o} }

// a -knows-> b -knows-> c -knows-> d, b -LIVES_AT-> addr, a IS_TYPE PERSON
var chain = []*model.Triple{
	tr("a", "knows", "b"), tr("b", "knows", "c"), tr("c", "knows", "d"),
	tr("b", "LIVES_AT", "addr"), tr("a", "IS_TYPE", "PERSON"),
}

func run(t *testing.T, f Fetcher, req TraverseRequest) *TraverseResult {
	t.Helper()
	if req.MaxDegree == 0 {
		req.MaxDegree = 100
	}
	if req.MaxTriples == 0 {
		req.MaxTriples = 1000
	}
	if req.Direction == "" {
		req.Direction = "out"
	}
	res, err := (&Traverser{Fetch: f, Threads: 4}).Run(context.Background(), &req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func spo(res *TraverseResult) []string {
	var out []string
	for _, t := range res.Triples {
		out = append(out, fmt.Sprintf("%d:%s-%s-%s", t.Hop, t.Subject, t.Predicate, t.Object))
	}
	sort.Strings(out)
	return out
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got  %v\nwant %v", got, want)
	}
}

func TestTraverseHops(t *testing.T) {
	f := &memFetcher{triples: chain}
	eq(t, spo(run(t, f, TraverseRequest{Start: []string{"a"}, Hops: 1})),
		[]string{"1:a-IS_TYPE-PERSON", "1:a-knows-b"})
	eq(t, spo(run(t, f, TraverseRequest{Start: []string{"a"}, Hops: 3, Predicates: []string{"knows"}})),
		[]string{"1:a-knows-b", "2:b-knows-c", "3:c-knows-d"})
}

func TestTraverseInboundAndBoth(t *testing.T) {
	f := &memFetcher{triples: chain}
	eq(t, spo(run(t, f, TraverseRequest{Start: []string{"c"}, Hops: 2, Direction: "in"})),
		[]string{"1:b-knows-c", "2:a-knows-b"})
	eq(t, spo(run(t, f, TraverseRequest{Start: []string{"c"}, Hops: 1, Direction: "both"})),
		[]string{"1:b-knows-c", "1:c-knows-d"})
}

func TestTraverseAsPredicate(t *testing.T) {
	// The start value as a predicate: every LIVES_AT triple, then out from
	// both of its ends
	res := run(t, &memFetcher{triples: chain}, TraverseRequest{Start: []string{"LIVES_AT"}, As: []string{RolePredicate}, Hops: 2})
	eq(t, spo(res), []string{"1:b-LIVES_AT-addr", "2:b-knows-c"})
	if res.Triples[0].Via != RolePredicate || res.Triples[0].From != "LIVES_AT" {
		t.Errorf("first hop reached via %s from %s", res.Triples[0].Via, res.Triples[0].From)
	}
}

func TestTraverseHubNotExpanded(t *testing.T) {
	triples := []*model.Triple{tr("a", "knows", "hub")}
	for i := range 5 {
		triples = append(triples, tr("hub", "knows", fmt.Sprintf("n%d", i)))
	}
	res := run(t, &memFetcher{triples: triples}, TraverseRequest{Start: []string{"a"}, Hops: 3, MaxDegree: 3})
	eq(t, spo(res), []string{"1:a-knows-hub"})
	for _, n := range res.Nodes {
		if n.ID == "hub" && !n.Hub {
			t.Error("hub not flagged")
		}
	}

	// A start value past max_degree is followed for its first max_degree
	res = run(t, &memFetcher{triples: triples}, TraverseRequest{Start: []string{"hub"}, Hops: 1, MaxDegree: 3})
	if len(res.Triples) != 3 || !res.Nodes[0].Truncated {
		t.Errorf("start node: %d triples, truncated %v", len(res.Triples), res.Nodes[0].Truncated)
	}
}

func TestTraverseMaxTriplesAndCycles(t *testing.T) {
	cycle := []*model.Triple{tr("a", "p", "b"), tr("b", "p", "c"), tr("c", "p", "a")}
	res := run(t, &memFetcher{triples: cycle}, TraverseRequest{Start: []string{"a"}, Hops: 6})
	eq(t, spo(res), []string{"1:a-p-b", "2:b-p-c", "3:c-p-a"})

	res = run(t, &memFetcher{triples: cycle}, TraverseRequest{Start: []string{"a"}, Hops: 6, MaxTriples: 2})
	if len(res.Triples) != 2 || !res.Truncated {
		t.Errorf("%d triples, truncated %v", len(res.Triples), res.Truncated)
	}
}

func TestTraverseTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	res, err := (&Traverser{Fetch: &memFetcher{triples: chain}, Threads: 1}).Run(ctx,
		&TraverseRequest{Start: []string{"a"}, Hops: 2, Direction: "out", MaxDegree: 10, MaxTriples: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut || len(res.Triples) != 0 {
		t.Errorf("timed out %v with %d triples", res.TimedOut, len(res.Triples))
	}
}

func TestTraverseParallelWide(t *testing.T) {
	// 200 nodes on the second hop, all queried and returned
	var triples []*model.Triple
	for i := range 200 {
		n := fmt.Sprintf("n%d", i)
		triples = append(triples, tr("root", "p", n), tr(n, "q", n+"x"))
	}
	f := &memFetcher{triples: triples}
	res := run(t, f, TraverseRequest{Start: []string{"root"}, Hops: 2, MaxDegree: 500})
	if len(res.Triples) != 400 || f.calls.Load() != 201 {
		t.Errorf("%d triples from %d queries", len(res.Triples), f.calls.Load())
	}
}
