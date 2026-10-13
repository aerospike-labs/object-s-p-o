package main

import (
	"fmt"
	"log"
	"math/rand"
	"regexp"
	"strconv"
	"sync"
)

// ---------------------------------------------------------------------------
// Content hashing
// ---------------------------------------------------------------------------

// Hash functions for each part, set from -hash-type and -hash-parts; identity
// until then.
var (
	hashContent   = identity
	hashEntityID  = identity
	hashPredicate = identity
	hashTypeName  = identity
)

func identity(s string) string { return s }

// typeNames maps each type name as stored (hashed or not) back to the
// readable name, for reading entity types written by hashTypeName.
func typeNames(names ...[]string) map[string]string {
	m := make(map[string]string)
	for _, list := range names {
		for _, n := range list {
			m[hashTypeName(n)] = n
		}
	}
	return m
}

// ---------------------------------------------------------------------------
// ID allocation
// ---------------------------------------------------------------------------

// idCounterName is the meta record holding the highest ID reserved.
const idCounterName = "id_counter"

// idBlockSize is how many IDs a process reserves from the shared counter at a
// time, keeping counter round trips rare at high insert rates.
const idBlockSize = 100000

// idAllocator hands out IDs from blocks reserved atomically from a shared
// counter, so any number of generator processes on any hosts never collide.
type idAllocator struct {
	mu      sync.Mutex
	next    int64 // next ID to hand out
	end     int64 // last ID in the current block
	reserve func(n int64) (int64, error)
}

// newLocalReserve returns an in-process reserve function, for runs that
// don't share a counter (tests).
func newLocalReserve() func(int64) (int64, error) {
	var mu sync.Mutex
	var hi int64
	return func(n int64) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		hi += n
		return hi, nil
	}
}

var ids = &idAllocator{next: 1, reserve: newLocalReserve()}

func (a *idAllocator) Next() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next > a.end {
		hi, err := a.reserve(idBlockSize)
		if err != nil {
			log.Fatalf("failed to reserve IDs: %v", err)
		}
		a.next, a.end = hi-idBlockSize+1, hi
	}
	id := a.next
	a.next++
	return id
}

func generateEntityID(contentType string) string {
	return hashEntityID(fmt.Sprintf("%s:%d", contentType, ids.Next()))
}

func generateIdentifierValue(propName string) string {
	return fmt.Sprintf("%s_%d_%x", propName, ids.Next(), rand.Int63()&0xFFFFFF)
}

// Counter values embedded in unhashed entity IDs ("ACCOUNT:123") and
// identifier values ("EMAIL_E:EMAIL_E_123_abc"), used by -rebuild-entities to
// raise the counter above data written before the counter existed.
var (
	entityIDPattern     = regexp.MustCompile(`^[A-Z_]+:(\d+)$`)
	identifierIDPattern = regexp.MustCompile(`_(\d+)_[0-9a-f]+$`)
)

func idCounterValue(id string) int64 {
	m := entityIDPattern.FindStringSubmatch(id)
	if m == nil {
		m = identifierIDPattern.FindStringSubmatch(id)
	}
	if m == nil {
		return 0
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n
}
