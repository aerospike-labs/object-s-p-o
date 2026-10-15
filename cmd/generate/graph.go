package main

import (
	"fmt"
	"math/rand"

	"graph-sub-pred-obj/model"
)

// ---------------------------------------------------------------------------
// Entity generation
// ---------------------------------------------------------------------------

func generateEntityTriples(entityID string, contentType string, propDefs map[string][]PropertyMeta, now int64) []*model.Triple {
	return entityTriples(entityID, contentType, propDefs, now, generateIdentifierValue)
}

// entityTriples returns an entity's IS_TYPE and HAS_IDENTIFIER triples, with
// identifier values from value.
func entityTriples(entityID string, contentType string, propDefs map[string][]PropertyMeta, now int64, value func(propName string) string) []*model.Triple {
	var triples []*model.Triple

	for propName, metas := range propDefs {
		if len(metas) == 0 {
			continue
		}
		meta := metas[0]
		value := value(propName)

		triples = append(triples, &model.Triple{
			Subject:   entityID,
			Predicate: hashPredicate("HAS_IDENTIFIER"),
			Object:    hashContent(fmt.Sprintf("%s:%s", propName, value)),
			Props: map[string]interface{}{
				"identifier_type": hashTypeName(propName),
				"value":           hashContent(value),
				"created":         now,
				"last_seen":       now,
				"decay":           meta.Decay,
			},
		})
	}

	// Entity type triple
	triples = append(triples, &model.Triple{
		Subject:   entityID,
		Predicate: hashPredicate("IS_TYPE"),
		Object:    hashTypeName(contentType),
		Props: map[string]interface{}{
			"created": now,
		},
	})

	return triples
}

// ---------------------------------------------------------------------------
// Graph generation
// ---------------------------------------------------------------------------

func relationCount(pred string, rng *rand.Rand) int {
	return rangeCount(predicateCardinality[pred], rng)
}

// rangeCount picks a count uniformly from card.min..card.max.
func rangeCount(card cardinalityRange, rng *rand.Rand) int {
	if card.max > card.min {
		return card.min + rng.Intn(card.max-card.min+1)
	}
	return card.min
}

func relationTriple(sub, pred, obj string, now int64) *model.Triple {
	return &model.Triple{
		Subject:   sub,
		Predicate: hashPredicate(pred),
		Object:    obj,
		Props: map[string]interface{}{
			"created":   now,
			"last_seen": now,
		},
	}
}

// generateIndividualGraph returns the triples for one INDIVIDUAL and the
// entities it creates or links to. linkRanges sets how many entities of a
// type the INDIVIDUAL links to; other types use their predicate's range.
func generateIndividualGraph(
	individualID string,
	contentTypes map[string]map[string][]PropertyMeta,
	ontology []OntologyRelation,
	linkRanges map[string]cardinalityRange,
	now int64,
	rng *rand.Rand,
	pool *sharePool,
) []*model.Triple {
	triples := generateEntityTriples(individualID, "INDIVIDUAL", contentTypes["INDIVIDUAL"], now)

	// Entities linked to this individual (new or shared), for relationship wiring
	generated := map[string][]string{
		"INDIVIDUAL": {individualID},
	}
	// Entities created by this individual. Only these get their own outgoing
	// relations; shared entities already have theirs.
	created := map[string][]string{}
	linked := map[string]bool{}

	newEntity := func(entityType string) string {
		id := generateEntityID(entityType)
		if props, ok := contentTypes[entityType]; ok {
			triples = append(triples, generateEntityTriples(id, entityType, props, now)...)
		}
		return id
	}

	// Ontology rules originating from INDIVIDUAL: link to a shared entity
	// when the pool offers one, otherwise create a new one
	for _, rel := range ontology {
		if rel.Sub != "INDIVIDUAL" {
			continue
		}
		count := relationCount(rel.Pred, rng)
		if r, ok := linkRanges[rel.Obj]; ok {
			count = rangeCount(r, rng)
		}
		for i := count; i > 0; i-- {
			objID, shared := pool.pick(rel.Obj, linked, rng)
			if !shared {
				objID = newEntity(rel.Obj)
				created[rel.Obj] = append(created[rel.Obj], objID)
				pool.created(rel.Obj, objID, rng)
			}
			linked[objID] = true
			generated[rel.Obj] = append(generated[rel.Obj], objID)
			triples = append(triples, relationTriple(individualID, rel.Pred, objID, now))
		}
	}

	// Rules from entities this individual created (e.g., ACCOUNT -> HAS ->
	// CREDIT_DEVICE, HOUSEHOLD -> HAS_AN -> ADDRESS), reusing this individual's
	// entities where possible
	for _, rel := range ontology {
		if rel.Sub == "INDIVIDUAL" || rel.Obj == "INDIVIDUAL" {
			// HOUSEHOLD -> INDIVIDUAL is the reverse of IS_PART_OF
			continue
		}
		reuse := 0.5
		if rel.Sub == "HOUSEHOLD" {
			reuse = 0.7
		}
		for _, subID := range created[rel.Sub] {
			existing := generated[rel.Obj]
			count := relationCount(rel.Pred, rng)
			for i := 0; i < count; i++ {
				var objID string
				if i < len(existing) && rng.Float64() < reuse {
					objID = existing[i]
				} else {
					objID = newEntity(rel.Obj)
					generated[rel.Obj] = append(generated[rel.Obj], objID)
				}
				triples = append(triples, relationTriple(subID, rel.Pred, objID, now))
			}
		}
	}

	return triples
}

// ---------------------------------------------------------------------------
// Supernodes
// ---------------------------------------------------------------------------

// hubSet is a fixed set of entities that INDIVIDUALs link to without a
// sharing cap, to grow supernodes on purpose.
type hubSet struct {
	ids  []string
	pred string // predicate from INDIVIDUAL to the hub type, as stored
	rate float64
}

// newHubSet returns count hubs of entityType, linked from INDIVIDUALs by the
// ontology's predicate to that type. Hub IDs and identifiers are fixed, so
// every run and every host links to the same hubs.
func newHubSet(count int, entityType string, rate float64, ontology []OntologyRelation) (*hubSet, error) {
	pred := ""
	for _, rel := range ontology {
		if rel.Sub == "INDIVIDUAL" && rel.Obj == entityType {
			pred = rel.Pred
			break
		}
	}
	if pred == "" {
		return nil, fmt.Errorf("-hub-type %s: the ontology has no INDIVIDUAL relation to it", entityType)
	}
	h := &hubSet{pred: hashPredicate(pred), rate: rate}
	for i := 0; i < count; i++ {
		h.ids = append(h.ids, hashEntityID(fmt.Sprintf("%s:HUB-%d", entityType, i)))
	}
	return h, nil
}

// triples returns the hubs' own IS_TYPE and HAS_IDENTIFIER triples.
func (h *hubSet) triples(entityType string, propDefs map[string][]PropertyMeta, now int64) []*model.Triple {
	var triples []*model.Triple
	for i, id := range h.ids {
		triples = append(triples, entityTriples(id, entityType, propDefs, now, func(propName string) string {
			return fmt.Sprintf("%s_HUB_%d", propName, i)
		})...)
	}
	return triples
}

// link returns, with probability rate, a triple linking an INDIVIDUAL to a
// random hub.
func (h *hubSet) link(individualID string, now int64, rng *rand.Rand) *model.Triple {
	if len(h.ids) == 0 || rng.Float64() >= h.rate {
		return nil
	}
	return &model.Triple{
		Subject:   individualID,
		Predicate: h.pred,
		Object:    h.ids[rng.Intn(len(h.ids))],
		Props:     map[string]interface{}{"created": now, "last_seen": now},
	}
}
