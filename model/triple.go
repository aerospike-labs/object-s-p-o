package model

import (
	"crypto/sha256"
	"encoding/hex"
)

// Triple represents a single RDF Subject-Predicate-Object triple with optional metadata.
type Triple struct {
	Subject   string
	Predicate string
	Object    string
	Props     map[string]interface{}
}

// Key returns a deterministic primary key by hashing S+P+O.
// Null byte separators prevent collisions between adjacent values.
func (t *Triple) Key() string {
	h := sha256.New()
	h.Write([]byte(t.Subject))
	h.Write([]byte{0})
	h.Write([]byte(t.Predicate))
	h.Write([]byte{0})
	h.Write([]byte(t.Object))
	return hex.EncodeToString(h.Sum(nil))
}

// ConvertMapKeys converts map[interface{}]interface{} (as returned by Aerospike)
// to map[string]interface{}.
func ConvertMapKeys(m map[interface{}]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		if ks, ok := k.(string); ok {
			result[ks] = v
		}
	}
	return result
}
