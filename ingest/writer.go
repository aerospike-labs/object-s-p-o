package ingest

import (
	"log"
	"sync"
	"sync/atomic"

	"graph-sub-pred-obj/model"
)

// Writer writes triples concurrently: producers Add triples in any amount and
// worker goroutines regroup them into batches of batchSize for write. A failed
// batch is logged and counted, and writing continues.
type Writer struct {
	ch      chan []*model.Triple
	wg      sync.WaitGroup
	written atomic.Int64
	errors  atomic.Int64
}

// NewWriter starts workers goroutines writing batches with write, typically
// GraphStore.BatchPutTriples.
func NewWriter(write func([]*model.Triple) error, workers, batchSize int) *Writer {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	w := &Writer{ch: make(chan []*model.Triple, workers*4)}
	writeBatch := func(batch []*model.Triple) {
		if err := write(batch); err != nil {
			w.errors.Add(1)
			log.Printf("batch write error: %v", err)
			return
		}
		w.written.Add(int64(len(batch)))
	}
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			buffer := make([]*model.Triple, 0, batchSize)
			for triples := range w.ch {
				buffer = append(buffer, triples...)
				for len(buffer) >= batchSize {
					writeBatch(buffer[:batchSize])
					buffer = append(buffer[:0], buffer[batchSize:]...)
				}
			}
			if len(buffer) > 0 {
				writeBatch(buffer)
			}
		}()
	}
	return w
}

// Add queues triples for writing. The caller must not modify the slice
// afterwards.
func (w *Writer) Add(triples []*model.Triple) {
	w.ch <- triples
}

// Close writes the remaining triples and waits for the workers to finish.
func (w *Writer) Close() {
	close(w.ch)
	w.wg.Wait()
}

// Written returns the number of triples written so far.
func (w *Writer) Written() int64 { return w.written.Load() }

// Errors returns the number of batches that failed.
func (w *Writer) Errors() int64 { return w.errors.Load() }
