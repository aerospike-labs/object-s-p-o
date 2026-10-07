package store

import (
	"fmt"

	"graph-sub-pred-obj/model"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// PutTriple writes a single triple to Aerospike with upsert semantics.
func (gs *GraphStore) PutTriple(t *model.Triple) error {
	key, err := aero.NewKey(gs.Namespace, SetName, t.Key())
	if err != nil {
		return err
	}

	bins := aero.BinMap{
		"subject":   t.Subject,
		"predicate": t.Predicate,
		"object":    t.Object,
	}
	if t.Props != nil {
		bins["props"] = t.Props
	}

	return gs.Client.Put(gs.WritePolicy, key, bins)
}

// BatchPutTriples writes multiple triples in a single batch operation.
func (gs *GraphStore) BatchPutTriples(triples []*model.Triple) error {
	records := make([]aero.BatchRecordIfc, 0, len(triples))

	for _, t := range triples {
		key, err := aero.NewKey(gs.Namespace, SetName, t.Key())
		if err != nil {
			return err
		}

		ops := []*aero.Operation{
			aero.PutOp(aero.NewBin("subject", t.Subject)),
			aero.PutOp(aero.NewBin("predicate", t.Predicate)),
			aero.PutOp(aero.NewBin("object", t.Object)),
		}
		if t.Props != nil {
			ops = append(ops, aero.PutOp(aero.NewBin("props", t.Props)))
		}

		bwp := aero.NewBatchWritePolicy()
		bwp.RecordExistsAction = aero.UPDATE

		bw := aero.NewBatchWrite(bwp, key, ops...)
		records = append(records, bw)
	}

	if err := gs.Client.BatchOperate(gs.BatchPolicy, records); err != nil {
		return err
	}
	return batchRecordErrors(records)
}

// batchRecordErrors reports per-record failures, which BatchOperate does not
// return as an error.
func batchRecordErrors(records []aero.BatchRecordIfc) error {
	failed := 0
	var first aero.Error
	for _, r := range records {
		if br := r.BatchRec(); br.Err != nil {
			if first == nil {
				first = br.Err
			}
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d batch records failed, first: %w", failed, len(records), first)
	}
	return nil
}

// DeleteTriple removes a triple by its SPO key.
func (gs *GraphStore) DeleteTriple(subject, predicate, object string) (bool, error) {
	t := &model.Triple{Subject: subject, Predicate: predicate, Object: object}
	key, err := aero.NewKey(gs.Namespace, SetName, t.Key())
	if err != nil {
		return false, err
	}
	return gs.Client.Delete(gs.WritePolicy, key)
}

// UpdateTripleProperty atomically updates a single property in the props map
// without reading the entire record.
func (gs *GraphStore) UpdateTripleProperty(t *model.Triple, propKey string, propVal interface{}) error {
	key, err := aero.NewKey(gs.Namespace, SetName, t.Key())
	if err != nil {
		return err
	}
	op := aero.MapPutOp(aero.DefaultMapPolicy(), "props", propKey, propVal)
	_, err = gs.Client.Operate(gs.WritePolicy, key, op)
	return err
}
