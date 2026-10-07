package store

import (
	aero "github.com/aerospike/aerospike-client-go/v8"
)

// CreateIndexes creates secondary indexes on subject, predicate, and object bins.
// Safe to call multiple times; existing indexes are skipped.
func (gs *GraphStore) CreateIndexes() error {
	indexes := []struct {
		bin  string
		name string
	}{
		{"subject", "idx_subject"},
		{"predicate", "idx_predicate"},
		{"object", "idx_object"},
	}

	for _, idx := range indexes {
		task, err := gs.Client.CreateIndex(
			nil,
			gs.Namespace,
			SetName,
			idx.name,
			idx.bin,
			aero.STRING,
		)
		if err != nil {
			// ResultCode 200 means index already exists
			if aeroErr, ok := err.(*aero.AerospikeError); ok && aeroErr.ResultCode == 200 {
				continue
			}
			return err
		}
		<-task.OnComplete()
	}
	return nil
}
