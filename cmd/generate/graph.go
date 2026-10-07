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
	var triples []*model.Triple

	for propName, metas := range propDefs {
		if len(metas) == 0 {
			continue
		}
		meta := metas[0]
		value := generateIdentifierValue(propName)

		triples = append(triples, &model.Triple{
			Subject:   entityID,
			Predicate: "HAS_IDENTIFIER",
			Object:    hashContent(fmt.Sprintf("%s:%s", propName, value)),
			Props: map[string]interface{}{
				"identifier_type": propName,
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
		Predicate: "IS_TYPE",
		Object:    contentType,
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
	card := predicateCardinality[pred]
	if card.max > card.min {
		return card.min + rng.Intn(card.max-card.min+1)
	}
	return card.min
}

func relationTriple(sub, pred, obj string, now int64) *model.Triple {
	return &model.Triple{
		Subject:   sub,
		Predicate: pred,
		Object:    obj,
		Props: map[string]interface{}{
			"created":   now,
			"last_seen": now,
		},
	}
}

// generateIndividualGraph returns the triples for one INDIVIDUAL and the
// entities it creates or links to.
func generateIndividualGraph(
	individualID string,
	contentTypes map[string]map[string][]PropertyMeta,
	ontology []OntologyRelation,
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
		for i := relationCount(rel.Pred, rng); i > 0; i-- {
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
