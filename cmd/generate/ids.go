package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Content hashing
// ---------------------------------------------------------------------------

// Parts that -hash-parts can hash. Identifiers are the identifier data: each
// identifier node (the HAS_IDENTIFIER object) and its value prop. The other
// three are the navigation parts the graph is walked by: entity IDs (subjects
// and objects), predicates, and type names (IS_TYPE objects and the
// identifier_type prop).
const (
	partIdentifiers = "identifiers"
	partEntityIDs   = "entity-ids"
	partPredicates  = "predicates"
	partTypes       = "types"
)

var navigationParts = []string{partEntityIDs, partPredicates, partTypes}

// readableNavigation is the navigation signature when no navigation part is
// hashed.
const readableNavigation = "readable"

// Hash functions for each part; identity unless that part is hashed. Hashing
// is deterministic, so identical content still links.
var (
	hashContent   = identity
	hashEntityID  = identity
	hashPredicate = identity
	hashTypeName  = identity
)

func identity(s string) string { return s }

// configureHashing sets the hash function of each part. It returns the
// navigation signature, which runs must share for their data to link
// ("readable", or the hash type and hashed navigation parts, e.g.
// "sha-512:entity-ids,predicates"), and a description of what is hashed.
func configureHashing(hashType, partList string) (navigation, desc string, err error) {
	h, err := newHashFunc(hashType)
	if err != nil {
		return "", "", err
	}
	hashContent, hashEntityID, hashPredicate, hashTypeName = identity, identity, identity, identity
	parts := map[string]bool{}
	for _, p := range strings.Split(partList, ",") {
		switch p = strings.TrimSpace(p); p {
		case partIdentifiers, partEntityIDs, partPredicates, partTypes:
			parts[p] = true
		case "navigation", "all":
			for _, n := range navigationParts {
				parts[n] = true
			}
			if p == "all" {
				parts[partIdentifiers] = true
			}
		case "":
		default:
			return "", "", fmt.Errorf("unknown hash part %q (want identifiers, entity-ids, predicates, types, navigation, or all)", p)
		}
	}
	var hashedNav []string
	for _, n := range navigationParts {
		if parts[n] {
			hashedNav = append(hashedNav, n)
		}
	}
	if hashType == "no-hash" {
		if len(hashedNav) > 0 {
			return "", "", fmt.Errorf("-hash-parts %s needs -hash-type sha-256 or sha-512", strings.Join(hashedNav, ","))
		}
		return readableNavigation, "no-hash", nil
	}

	pick := func(part string) func(string) string {
		if parts[part] {
			return h
		}
		return identity
	}
	hashContent = pick(partIdentifiers)
	hashEntityID = pick(partEntityIDs)
	hashPredicate = pick(partPredicates)
	hashTypeName = pick(partTypes)

	var hashed []string
	if parts[partIdentifiers] {
		hashed = append(hashed, partIdentifiers)
	}
	hashed = append(hashed, hashedNav...)
	if len(hashed) == 0 {
		return readableNavigation, "no-hash (no -hash-parts)", nil
	}
	desc = hashType + " on " + strings.Join(hashed, ",")
	if len(hashedNav) == 0 {
		return readableNavigation, desc, nil
	}
	return hashType + ":" + strings.Join(hashedNav, ","), desc, nil
}

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

func newHashFunc(hashType string) (func(string) string, error) {
	switch hashType {
	case "no-hash":
		return identity, nil
	case "sha-256":
		return func(s string) string {
			sum := sha256.Sum256([]byte(s))
			return hex.EncodeToString(sum[:])
		}, nil
	case "sha-512":
		return func(s string) string {
			sum := sha512.Sum512([]byte(s))
			return hex.EncodeToString(sum[:])
		}, nil
	}
	return nil, fmt.Errorf("unknown hash type %q (want no-hash, sha-256, or sha-512)", hashType)
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
