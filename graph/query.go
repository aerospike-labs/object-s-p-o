package graph

import (
	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
)

// PatternQuery represents an RDF query pattern where nil means "any".
type PatternQuery struct {
	Subject   *string
	Predicate *string
	Object    *string
}

// S is a helper to set the subject in a pattern query.
func S(v string) *string { return &v }

// P is a helper to set the predicate in a pattern query.
func P(v string) *string { return &v }

// O is a helper to set the object in a pattern query.
func O(v string) *string { return &v }

// Execute runs the pattern query, selecting the optimal index strategy automatically.
func (pq *PatternQuery) Execute(gs *store.GraphStore) ([]*model.Triple, error) {
	s := pq.Subject != nil
	p := pq.Predicate != nil
	o := pq.Object != nil

	switch {
	case s && p && o: // SPO - direct PK lookup
		t, err := gs.GetTriple(*pq.Subject, *pq.Predicate, *pq.Object)
		if err != nil || t == nil {
			return nil, err
		}
		return []*model.Triple{t}, nil

	case s && p: // SP?
		return gs.QuerySP(*pq.Subject, *pq.Predicate)

	case s && o: // S?O - use subject index, filter on object
		return gs.QuerySO(*pq.Subject, *pq.Object)

	case s: // S??
		return gs.QueryBySubject(*pq.Subject)

	case p && o: // ?PO
		return gs.QueryPO(*pq.Predicate, *pq.Object)

	case p: // ?P?
		return gs.QueryByPredicate(*pq.Predicate)

	case o: // ??O
		return gs.QueryByObject(*pq.Object)

	default: // ??? - full scan
		return gs.ScanAll()
	}
}
