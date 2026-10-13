// Package hashing hashes triple content for -hash-type and -hash-parts, the
// same way in the generator and the file loader, and keeps runs with
// different navigation hashing out of the same namespace.
package hashing

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strings"

	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
)

// Parts that -hash-parts can hash. Identifiers are the identifier data: each
// identifier node (the HAS_IDENTIFIER object) and its value prop. The other
// three are the navigation parts the graph is walked by: entity IDs (subjects
// and objects), predicates, and type names (IS_TYPE objects and the
// identifier_type prop).
const (
	PartIdentifiers = "identifiers"
	PartEntityIDs   = "entity-ids"
	PartPredicates  = "predicates"
	PartTypes       = "types"
)

var navigationParts = []string{PartEntityIDs, PartPredicates, PartTypes}

// ReadableNavigation is the navigation signature when no navigation part is
// hashed.
const ReadableNavigation = "readable"

// FlagHelp describes the values -hash-parts takes.
const FlagHelp = "Comma list of what -hash-type hashes: identifiers, entity-ids, predicates, types, navigation (entity-ids,predicates,types), or all"

// Hasher holds the hash function of each part; identity unless that part is
// hashed. Hashing is deterministic, so identical content still links.
type Hasher struct {
	Content, EntityID, Predicate, TypeName func(string) string

	// Navigation is the signature runs must share for their data to link:
	// "readable", or the hash type and hashed navigation parts, e.g.
	// "sha-512:entity-ids,predicates".
	Navigation string
	// Desc describes what is hashed, e.g. "sha-512 on identifiers".
	Desc string

	active bool // some part is hashed
}

// Active reports whether any part is hashed.
func (h *Hasher) Active() bool { return h.active }

func identity(s string) string { return s }

// New returns the Hasher for a -hash-type and -hash-parts.
func New(hashType, partList string) (*Hasher, error) {
	h, err := newHashFunc(hashType)
	if err != nil {
		return nil, err
	}
	hs := &Hasher{Content: identity, EntityID: identity, Predicate: identity, TypeName: identity}
	parts := map[string]bool{}
	for _, p := range strings.Split(partList, ",") {
		switch p = strings.TrimSpace(p); p {
		case PartIdentifiers, PartEntityIDs, PartPredicates, PartTypes:
			parts[p] = true
		case "navigation", "all":
			for _, n := range navigationParts {
				parts[n] = true
			}
			if p == "all" {
				parts[PartIdentifiers] = true
			}
		case "":
		default:
			return nil, fmt.Errorf("unknown hash part %q (want identifiers, entity-ids, predicates, types, navigation, or all)", p)
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
			return nil, fmt.Errorf("-hash-parts %s needs -hash-type sha-256 or sha-512", strings.Join(hashedNav, ","))
		}
		hs.Navigation, hs.Desc = ReadableNavigation, "no-hash"
		return hs, nil
	}

	pick := func(part string) func(string) string {
		if parts[part] {
			return h
		}
		return identity
	}
	hs.Content = pick(PartIdentifiers)
	hs.EntityID = pick(PartEntityIDs)
	hs.Predicate = pick(PartPredicates)
	hs.TypeName = pick(PartTypes)

	var hashed []string
	if parts[PartIdentifiers] {
		hashed = append(hashed, PartIdentifiers)
	}
	hashed = append(hashed, hashedNav...)
	if len(hashed) == 0 {
		hs.Navigation, hs.Desc = ReadableNavigation, "no-hash (no -hash-parts)"
		return hs, nil
	}
	hs.Desc = hashType + " on " + strings.Join(hashed, ",")
	hs.active = true
	hs.Navigation = ReadableNavigation
	if len(hashedNav) > 0 {
		hs.Navigation = hashType + ":" + strings.Join(hashedNav, ",")
	}
	return hs, nil
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

// Loaded hashes a triple read from a file, applying the parts the way the
// generator does to the triples it writes: the subject is an entity ID; the
// object is a type name for IS_TYPE, identifier data for HAS_IDENTIFIER and
// for literals, and an entity ID otherwise; a HAS_IDENTIFIER triple's
// identifier_type and value props are a type name and identifier data. Other
// props are kept as they are. The triple's props map may be shared, so it is
// copied rather than modified.
func (h *Hasher) Loaded(t *model.Triple, literal bool) *model.Triple {
	out := &model.Triple{Subject: h.EntityID(t.Subject), Predicate: h.Predicate(t.Predicate), Props: t.Props}
	switch {
	case t.Predicate == "IS_TYPE":
		out.Object = h.TypeName(t.Object)
	case t.Predicate == "HAS_IDENTIFIER" || literal:
		out.Object = h.Content(t.Object)
	default:
		out.Object = h.EntityID(t.Object)
	}
	if t.Predicate == "HAS_IDENTIFIER" && t.Props != nil {
		out.Props = make(map[string]interface{}, len(t.Props))
		for k, v := range t.Props {
			if s, ok := v.(string); ok {
				switch k {
				case "identifier_type":
					v = h.TypeName(s)
				case "value":
					v = h.Content(s)
				}
			}
			out.Props[k] = v
		}
	}
	return out
}

// CheckStored records this run's navigation signature on the first load and
// refuses a run whose signature differs from the data already loaded, since
// its entities, predicates and types would not link with that data.
// Identifier hashing is not part of the signature: runs that differ only in
// it still link.
func CheckStored(gs *store.GraphStore, navigation string) error {
	stored, err := gs.GetMetaString(store.HashConfigName, "navigation")
	if err != nil {
		return fmt.Errorf("failed to read hash config: %w", err)
	}
	if stored == "" {
		existing, err := gs.SetRecordCount(store.SetName)
		if err != nil {
			return fmt.Errorf("failed to check for existing triples: %w", err)
		}
		want := navigation
		if existing > 0 {
			// Loaded before the signature was recorded, when navigation
			// parts were never hashed
			want = ReadableNavigation
		}
		if _, err := gs.CreateMetaString(store.HashConfigName, "navigation", want); err != nil {
			return fmt.Errorf("failed to record hash config: %w", err)
		}
		// Another run may have recorded its signature first
		if stored, err = gs.GetMetaString(store.HashConfigName, "navigation"); err != nil {
			return fmt.Errorf("failed to read hash config: %w", err)
		}
	}
	if stored != navigation {
		return fmt.Errorf("existing data has navigation %s but this run would write %s; "+
			"runs with different navigation hashing don't link. Use the same -hash-type and "+
			"navigation -hash-parts as the existing data, or load into an empty namespace",
			DescribeNavigation(stored), DescribeNavigation(navigation))
	}
	return nil
}

// DescribeNavigation describes a navigation signature for messages.
func DescribeNavigation(signature string) string {
	if signature == ReadableNavigation {
		return "not hashed"
	}
	hashType, parts, _ := strings.Cut(signature, ":")
	return fmt.Sprintf("hashed with %s (%s)", hashType, parts)
}
