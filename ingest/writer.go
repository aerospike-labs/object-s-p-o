package ingest

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// writeAttempts is how many times a batch is written before it counts as
// failed. Every write function used with a Writer can be repeated with the
// same result, so a batch that failed partway, on a timeout for example, is
// written again whole.
const writeAttempts = 3

// Writer writes items (triples, or vertex mutations) concurrently: producers
// Add items in any amount and worker goroutines regroup them into batches of
// batchSize for write. A batch that keeps failing is logged and counted, and
// writing continues.
type Writer[T any] struct {
	ch      chan []T
	wg      sync.WaitGroup
	written atomic.Int64
	errors  atomic.Int64
}

// NewWriter starts workers goroutines writing batches with write, typically
// GraphStore.BatchPutTriples.
func NewWriter[T any](write func([]T) error, workers, batchSize int) *Writer[T] {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	w := &Writer[T]{ch: make(chan []T, workers*4)}
	writeBatch := func(batch []T) {
		var err error
		for attempt := 1; attempt <= writeAttempts; attempt++ {
			if err = write(batch); err == nil {
				w.written.Add(int64(len(batch)))
				return
			}
			if attempt < writeAttempts {
				time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			}
		}
		w.errors.Add(1)
		log.Printf("batch write error after %d attempts: %v", writeAttempts, err)
	}
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			buffer := make([]T, 0, batchSize)
			for items := range w.ch {
				buffer = append(buffer, items...)
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

// Add queues items for writing. The caller must not modify the slice
// afterwards.
func (w *Writer[T]) Add(items []T) {
	w.ch <- items
}

// Close writes the remaining items and waits for the workers to finish.
func (w *Writer[T]) Close() {
	close(w.ch)
	w.wg.Wait()
}

// Written returns the number of items written so far.
func (w *Writer[T]) Written() int64 { return w.written.Load() }

// Errors returns the number of batches that failed.
func (w *Writer[T]) Errors() int64 { return w.errors.Load() }
