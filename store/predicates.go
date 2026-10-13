package store

import (
	"sort"

	"graph-sub-pred-obj/model"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// PredicatesName is the meta record whose "names" map bin holds every
// distinct predicate written to the triples set, as stored (hashed when
// predicates are hashed), so readers can list them without a scan.
const PredicatesName = "predicates"

const predicatesBin = "names"

// recordNewPredicates adds the predicates of triples that this client hasn't
// recorded yet to the predicates meta record. Called before the triples are
// written, so every stored predicate is listed even if a write fails. Writers
// racing on a new predicate both record it, which is harmless.
func (gs *GraphStore) recordNewPredicates(triples []*model.Triple) error {
	var fresh []string
	for _, t := range triples {
		if _, ok := gs.predicates.Load(t.Predicate); !ok {
			fresh = append(fresh, t.Predicate)
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	if err := gs.RecordPredicates(fresh); err != nil {
		return err
	}
	for _, p := range fresh {
		gs.predicates.Store(p, true)
	}
	return nil
}

// RecordPredicates adds predicates to the predicates meta record. Safe across
// hosts: entries are only ever added.
func (gs *GraphStore) RecordPredicates(predicates []string) error {
	key, err := gs.counterKey(PredicatesName)
	if err != nil {
		return err
	}
	items := make(map[interface{}]interface{}, len(predicates))
	for _, p := range predicates {
		items[p] = 1
	}
	_, aerr := gs.Client.Operate(gs.WritePolicy, key,
		aero.MapPutItemsOp(aero.DefaultMapPolicy(), predicatesBin, items))
	return aerr
}

// Predicates returns the recorded predicates, sorted, or none if nothing has
// been recorded.
func (gs *GraphStore) Predicates() ([]string, error) {
	key, err := gs.counterKey(PredicatesName)
	if err != nil {
		return nil, err
	}
	rec, aerr := gs.Client.Get(nil, key, predicatesBin)
	if aerr != nil {
		if aerr.Matches(aero.ErrKeyNotFound.ResultCode) {
			return nil, nil
		}
		return nil, aerr
	}
	m, _ := rec.Bins[predicatesBin].(map[interface{}]interface{})
	names := make([]string, 0, len(m))
	for k := range m {
		if s, ok := k.(string); ok {
			names = append(names, s)
		}
	}
	sort.Strings(names)
	return names, nil
}

// RebuildPredicates scans every triple and records its predicate, for data
// written before predicates were recorded. Returns the triples scanned.
func (gs *GraphStore) RebuildPredicates() (int64, error) {
	seen := map[string]bool{}
	var scanned int64
	err := gs.ScanEach(func(t *model.Triple) error {
		scanned++
		seen[t.Predicate] = true
		return nil
	})
	if err != nil {
		return scanned, err
	}
	predicates := make([]string, 0, len(seen))
	for p := range seen {
		predicates = append(predicates, p)
	}
	if len(predicates) == 0 {
		return scanned, nil
	}
	return scanned, gs.RecordPredicates(predicates)
}
