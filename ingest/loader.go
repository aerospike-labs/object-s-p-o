package ingest

import (
	"fmt"
	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
)

const DefaultBatchSize = 256

// Loader handles bulk ingestion of triples into the graph store.
type Loader struct {
	store     *store.GraphStore
	batchSize int
}

// NewLoader creates a loader with the specified batch size.
func NewLoader(gs *store.GraphStore, batchSize int) *Loader {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &Loader{store: gs, batchSize: batchSize}
}

// LoadTriples ingests a slice of triples in batches.
func (l *Loader) LoadTriples(triples []*model.Triple) error {
	for i := 0; i < len(triples); i += l.batchSize {
		end := i + l.batchSize
		if end > len(triples) {
			end = len(triples)
		}
		if err := l.store.BatchPutTriples(triples[i:end]); err != nil {
			return fmt.Errorf("batch write failed at offset %d: %w", i, err)
		}
	}
	return nil
}

// LoadFromChannel reads triples from a channel and writes in batches.
// Useful for streaming ingestion from parsers.
func (l *Loader) LoadFromChannel(ch <-chan *model.Triple) error {
	batch := make([]*model.Triple, 0, l.batchSize)
	for t := range ch {
		batch = append(batch, t)
		if len(batch) >= l.batchSize {
			if err := l.store.BatchPutTriples(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return l.store.BatchPutTriples(batch)
	}
	return nil
}
