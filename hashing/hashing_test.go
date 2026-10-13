package hashing

import (
	"reflect"
	"strings"
	"testing"

	"graph-sub-pred-obj/model"
)

func TestNewSignatures(t *testing.T) {
	for _, tc := range []struct {
		hashType, parts, navigation, desc string
		active                            bool
	}{
		{"no-hash", "identifiers", "readable", "no-hash", false},
		{"sha-512", "identifiers", "readable", "sha-512 on identifiers", true},
		{"sha-256", "", "readable", "no-hash (no -hash-parts)", false},
		{"sha-512", "navigation", "sha-512:entity-ids,predicates,types", "sha-512 on entity-ids,predicates,types", true},
		{"sha-512", "all", "sha-512:entity-ids,predicates,types", "sha-512 on identifiers,entity-ids,predicates,types", true},
		{"sha-256", "types, entity-ids", "sha-256:entity-ids,types", "sha-256 on entity-ids,types", true},
	} {
		h, err := New(tc.hashType, tc.parts)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.hashType, tc.parts, err)
		}
		if h.Navigation != tc.navigation || h.Desc != tc.desc || h.Active() != tc.active {
			t.Errorf("%s %s: got %q %q %v, want %q %q %v", tc.hashType, tc.parts,
				h.Navigation, h.Desc, h.Active(), tc.navigation, tc.desc, tc.active)
		}
	}
	for _, bad := range [][2]string{{"no-hash", "navigation"}, {"md5", "identifiers"}, {"sha-512", "ids"}} {
		if _, err := New(bad[0], bad[1]); err == nil {
			t.Errorf("New(%q, %q): want an error", bad[0], bad[1])
		}
	}
}

// A readable triple loaded from a file and hashed matches the triple the
// generator writes with the same settings, so loaded and generated data link.
func TestLoadedMatchesGenerator(t *testing.T) {
	h, _ := New("sha-512", "all")
	generated := []*model.Triple{
		{Subject: h.EntityID("INDIVIDUAL:1"), Predicate: h.Predicate("IS_TYPE"), Object: h.TypeName("INDIVIDUAL"),
			Props: map[string]interface{}{"created": int64(5)}},
		{Subject: h.EntityID("INDIVIDUAL:1"), Predicate: h.Predicate("HAS_IDENTIFIER"), Object: h.Content("EMAIL_E:x@y"),
			Props: map[string]interface{}{"identifier_type": h.TypeName("EMAIL_E"), "value": h.Content("x@y"), "decay": 0.1}},
		{Subject: h.EntityID("INDIVIDUAL:1"), Predicate: h.Predicate("LIVES_AT"), Object: h.EntityID("ADDRESS:2")},
	}
	readable := []*model.Triple{
		{Subject: "INDIVIDUAL:1", Predicate: "IS_TYPE", Object: "INDIVIDUAL", Props: map[string]interface{}{"created": int64(5)}},
		{Subject: "INDIVIDUAL:1", Predicate: "HAS_IDENTIFIER", Object: "EMAIL_E:x@y",
			Props: map[string]interface{}{"identifier_type": "EMAIL_E", "value": "x@y", "decay": 0.1}},
		{Subject: "INDIVIDUAL:1", Predicate: "LIVES_AT", Object: "ADDRESS:2"},
	}
	for i, r := range readable {
		got := h.Loaded(r, false)
		if !reflect.DeepEqual(got, generated[i]) {
			t.Errorf("triple %d: got %+v\nwant %+v", i, got, generated[i])
		}
		if got.Key() != generated[i].Key() {
			t.Errorf("triple %d: keys differ", i)
		}
	}
	if readable[1].Props["value"] != "x@y" {
		t.Error("Loaded modified the input props")
	}
}

func TestLoadedParts(t *testing.T) {
	in := &model.Triple{Subject: "s", Predicate: "p", Object: "o"}
	h, _ := New("sha-256", "identifiers")
	if got := h.Loaded(in, false); !reflect.DeepEqual(got, in) {
		t.Errorf("identifiers only: entity object hashed: %+v", got)
	}
	if got := h.Loaded(in, true); got.Object == "o" || got.Subject != "s" || got.Predicate != "p" {
		t.Errorf("identifiers only: literal object not hashed alone: %+v", got)
	}
	h, _ = New("sha-256", "entity-ids")
	if got := h.Loaded(in, true); got.Object != "o" || got.Subject == "s" {
		t.Errorf("entity-ids: got %+v, want subject hashed and literal kept", got)
	}
	if got := h.Loaded(in, false); got.Object == "o" || len(got.Object) != 64 || strings.ContainsAny(got.Object, "ABCDEF") {
		t.Errorf("entity-ids: object %q, want lowercase sha-256 hex", got.Object)
	}
}
