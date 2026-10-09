package main

import (
	"math/rand"
	"sync"

	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
)

// maxPoolPerType bounds memory; once full, new entities replace random slots.
const maxPoolPerType = 100000

// sharePool holds entities that individuals may link to instead of creating a
// new one. Sharing is expressed only by triples reusing the same entity ID, so
// relations form naturally when the RDF is loaded. An entity leaves the pool
// once it reaches its cardinality cap.
//
// Each generator process loads a disjoint partition range of the entities
// set, so caps hold across hosts without a database round trip per link.
// Link counts are tracked in memory and persisted as additive deltas.
type sharePool struct {
	mu     sync.Mutex
	rate   float64
	limits map[string]int      // entity type -> max INDIVIDUALs per entity, 0 = unlimited
	avail  map[string][]string // entity type -> IDs below cap
	counts map[string]int      // entity ID -> INDIVIDUALs linked
	dirty  map[string]store.EntityLinks
}

// newSharePool shares entities of the types in caps, each by at most its cap
// of INDIVIDUALs, and HOUSEHOLDs without limit.
func newSharePool(rate float64, caps map[string]int) *sharePool {
	p := &sharePool{
		rate:   rate,
		limits: map[string]int{"HOUSEHOLD": 0},
		avail:  make(map[string][]string),
		counts: make(map[string]int),
		dirty:  make(map[string]store.EntityLinks),
	}
	for t, c := range caps {
		p.limits[t] = c
	}
	return p
}

// tracked reports whether links to entities of this type are counted.
func (p *sharePool) tracked(entityType string) bool {
	_, ok := p.limits[entityType]
	return ok
}

func (p *sharePool) full(entityType string, count int) bool {
	limit := p.limits[entityType]
	return limit > 0 && count >= limit
}

// add offers an entity already linked to count individuals for sharing.
func (p *sharePool) add(entityType, id string, count int, rng *rand.Rand) {
	if !p.tracked(entityType) || p.full(entityType, count) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	list := p.avail[entityType]
	if len(list) < maxPoolPerType {
		p.avail[entityType] = append(list, id)
	} else {
		i := rng.Intn(len(list))
		delete(p.counts, list[i])
		list[i] = id
	}
	p.counts[id] = count
}

// created records a new entity linked to one individual.
func (p *sharePool) created(entityType, id string, rng *rand.Rand) {
	if !p.tracked(entityType) {
		return
	}
	p.add(entityType, id, 1, rng)
	p.mu.Lock()
	p.markLinked(entityType, id)
	p.mu.Unlock()
}

// markLinked records one new INDIVIDUAL link to persist. Caller holds mu.
func (p *sharePool) markLinked(entityType, id string) {
	d := p.dirty[id]
	d.ID, d.Type = id, entityType
	d.Links++
	p.dirty[id] = d
}

// pick returns, with probability rate, an existing entity of entityType that
// is below its cap and not in exclude, recording the new link.
func (p *sharePool) pick(entityType string, exclude map[string]bool, rng *rand.Rand) (string, bool) {
	if !p.tracked(entityType) || p.limits[entityType] == 1 || rng.Float64() >= p.rate {
		return "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	list := p.avail[entityType]
	for attempt := 0; attempt < 3 && len(list) > 0; attempt++ {
		i := rng.Intn(len(list))
		id := list[i]
		if exclude[id] {
			continue
		}
		p.counts[id]++
		p.markLinked(entityType, id)
		if p.full(entityType, p.counts[id]) {
			list[i] = list[len(list)-1]
			p.avail[entityType] = list[:len(list)-1]
			delete(p.counts, id)
		}
		return id, true
	}
	return "", false
}

// takeDirty returns and clears the link deltas not yet persisted.
func (p *sharePool) takeDirty() []store.EntityLinks {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]store.EntityLinks, 0, len(p.dirty))
	for _, d := range p.dirty {
		out = append(out, d)
	}
	p.dirty = make(map[string]store.EntityLinks)
	return out
}

// partitionRange returns the slice of the 4096 partitions owned by client
// index of count clients.
func partitionRange(index, count int) (begin, n int) {
	begin = index * store.PartitionCount / count
	end := (index + 1) * store.PartitionCount / count
	return begin, end - begin
}

// loadShareable seeds the pool from the entity records in this client's
// partitions. Records of untracked types are skipped.
func loadShareable(scan func(begin, count int, fn func(store.EntityLinks) error) error, begin, count int, pool *sharePool, rng *rand.Rand) (int, error) {
	loaded := 0
	err := scan(begin, count, func(e store.EntityLinks) error {
		if pool.tracked(e.Type) {
			pool.add(e.Type, e.ID, e.Links, rng)
			loaded++
		}
		return nil
	})
	return loaded, err
}

// individualPredicates returns the predicates only ever used with an
// INDIVIDUAL subject, so links can be counted without knowing subject types.
func individualPredicates(ontology []OntologyRelation) map[string]bool {
	preds := make(map[string]bool)
	for _, rel := range ontology {
		if rel.Sub == "INDIVIDUAL" {
			preds[rel.Pred] = true
		}
	}
	for _, rel := range ontology {
		if rel.Sub != "INDIVIDUAL" {
			delete(preds, rel.Pred)
		}
	}
	return preds
}

// rebuildEntities scans all triples to recount INDIVIDUAL links for every
// tracked entity and to find the highest ID. Entity types come from IS_TYPE
// triples; typeName maps a stored type name back to the readable one. The
// highest ID is 0 when entity IDs and identifiers are hashed.
func rebuildEntities(scan func(func(*model.Triple) error) error, pool *sharePool, ontology []OntologyRelation, typeName func(string) string) ([]store.EntityLinks, int64, int, error) {
	linkPreds := make(map[string]bool)
	for p := range individualPredicates(ontology) {
		linkPreds[hashPredicate(p)] = true
	}
	isType := hashPredicate("IS_TYPE")

	var maxID int64
	var scanned int
	types := make(map[string]string) // entity -> type
	links := make(map[string]int)

	err := scan(func(t *model.Triple) error {
		scanned++
		for _, id := range []string{t.Subject, t.Object} {
			if n := idCounterValue(id); n > maxID {
				maxID = n
			}
		}
		switch {
		case t.Predicate == isType:
			if typ := typeName(t.Object); pool.tracked(typ) {
				types[t.Subject] = typ
			}
		case linkPreds[t.Predicate]:
			links[t.Object]++
		}
		return nil
	})
	if err != nil {
		return nil, 0, 0, err
	}

	var entities []store.EntityLinks
	for id, n := range links {
		if typ, ok := types[id]; ok {
			entities = append(entities, store.EntityLinks{ID: id, Type: typ, Links: n})
		}
	}
	return entities, maxID, scanned, nil
}
