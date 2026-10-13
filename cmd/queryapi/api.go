package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// TripleJSON is a triple as the API returns it.
type TripleJSON struct {
	Key       string                 `json:"key"`
	Subject   string                 `json:"subject"`
	Predicate string                 `json:"predicate"`
	Object    string                 `json:"object"`
	Props     map[string]interface{} `json:"props,omitempty"`
}

func tripleJSON(t *model.Triple, key string, props bool) TripleJSON {
	j := TripleJSON{Key: key, Subject: t.Subject, Predicate: t.Predicate, Object: t.Object}
	if props {
		j.Props = t.Props
	}
	return j
}

// storeFetcher queries the triples set, holding a slot of a semaphore shared
// by all requests so the service never runs more than its limit of
// concurrent queries against the cluster.
type storeFetcher struct {
	gs    *store.GraphStore
	slots chan struct{}
}

func (f *storeFetcher) Fetch(role, value string, predicates []string, limit int64) ([]*model.Triple, error) {
	filter := store.PredicatesFilter(predicates)
	if role == RolePredicate && len(predicates) > 0 {
		// The predicate index already pins the predicate
		filter = nil
		if !contains(predicates, value) {
			return nil, nil
		}
	}
	return f.query(role, value, filter, limit)
}

func (f *storeFetcher) query(bin, value string, filter *aero.Expression, limit int64) ([]*model.Triple, error) {
	f.slots <- struct{}{}
	defer func() { <-f.slots }()
	return f.gs.QueryByBinLimit(bin, value, filter, limit)
}

func (f *storeFetcher) get(key string) (*model.Triple, error) {
	f.slots <- struct{}{}
	defer func() { <-f.slots }()
	return f.gs.GetTripleByKey(key)
}

// Limits caps what a request may ask for.
type Limits struct {
	MaxHops        int
	MaxDegree      int64 // default per-node fetch for traversals
	MaxLimit       int64 // largest limit / max_degree a request may set
	MaxTriples     int   // largest max_triples a request may set
	DefaultLimit   int64
	DefaultHops    int
	DefaultTotal   int
	RequestTimeout time.Duration
}

// API serves the HTTP endpoints.
type API struct {
	gs        *store.GraphStore
	fetch     *storeFetcher
	traverser *Traverser
	limits    Limits
	namespace string
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", a.health)
	mux.HandleFunc("GET /v1/info", a.info)
	mux.HandleFunc("GET /v1/triple", a.triple)
	mux.HandleFunc("GET /v1/roles", a.roles)
	mux.HandleFunc("GET /v1/lookup", a.lookup)
	mux.HandleFunc("GET /v1/match", a.match)
	mux.HandleFunc("GET /v1/traverse", a.traverse)
	mux.HandleFunc("POST /v1/traverse", a.traverse)
	return logRequests(mux)
}

// apiError is a client error, reported with its status.
type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func badRequest(format string, args ...interface{}) error {
	return &apiError{http.StatusBadRequest, fmt.Sprintf(format, args...)}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeErr(w http.ResponseWriter, err error) {
	var ae *apiError
	if errors.As(err, &ae) {
		writeJSON(w, ae.status, map[string]string{"error": ae.msg})
		return
	}
	log.Printf("error: %v", err)
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if !a.gs.Client.IsConnected() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "disconnected"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) info(w http.ResponseWriter, r *http.Request) {
	navigation, err := a.gs.GetMetaString(store.HashConfigName, "navigation")
	if err != nil {
		writeErr(w, err)
		return
	}
	predicates, err := a.gs.Predicates()
	if err != nil {
		writeErr(w, err)
		return
	}
	triples, err := a.gs.SetRecordCount(store.SetName)
	if err != nil {
		writeErr(w, err)
		return
	}
	if navigation == "" {
		navigation = "readable"
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"version":    version.String(),
		"namespace":  a.namespace,
		"navigation": navigation,
		// Replicas included, from server statistics
		"triple_records": triples,
		"predicates":     predicates,
		"limits": map[string]interface{}{
			"max_hops":    a.limits.MaxHops,
			"max_limit":   a.limits.MaxLimit,
			"max_triples": a.limits.MaxTriples,
		},
	})
}

// triple is the key/value lookup: by triple key, or by subject, predicate
// and object together.
func (a *API) triple(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := q.Get("key")
	if key == "" {
		if !q.Has("s") || !q.Has("p") || !q.Has("o") {
			writeErr(w, badRequest("give key, or all of s, p and o"))
			return
		}
		key = (&model.Triple{Subject: q.Get("s"), Predicate: q.Get("p"), Object: q.Get("o")}).Key()
	}
	t, err := a.fetch.get(key)
	if err != nil {
		writeErr(w, err)
		return
	}
	if t == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such triple", "key": key})
		return
	}
	writeJSON(w, http.StatusOK, tripleJSON(t, key, true))
}

// roleQueries runs one index query per role concurrently.
func (a *API) roleQueries(value string, roles, predicates []string, limit int64) (map[string][]*model.Triple, error) {
	found := make(map[string][]*model.Triple, len(roles))
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	for _, role := range roles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts, err := a.fetch.Fetch(role, value, predicates, limit)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("query %s=%q: %w", role, value, err)
			}
			found[role] = ts
		}()
	}
	wg.Wait()
	return found, firstErr
}

// roles reports which of subject, predicate and object a value appears as,
// with one single-record query per role.
func (a *API) roles(w http.ResponseWriter, r *http.Request) {
	value, err := requireValue(r.URL.Query())
	if err != nil {
		writeErr(w, err)
		return
	}
	found, err := a.roleQueries(value, allRoles, nil, 1)
	if err != nil {
		writeErr(w, err)
		return
	}
	roles := map[string]bool{}
	for _, role := range allRoles {
		roles[role] = len(found[role]) > 0
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"value": value, "roles": roles})
}

type roleCount struct {
	Count int `json:"count"`
	// More: the role has more triples than limit
	More bool `json:"more"`
}

type matchedTriple struct {
	TripleJSON
	// Matched is the roles the value plays in this triple
	Matched []string `json:"matched"`
}

// lookup returns the triples a value appears in, in any combination of
// roles, each tagged with the roles it matched.
func (a *API) lookup(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	value, err := requireValue(q)
	if err != nil {
		writeErr(w, err)
		return
	}
	roles, err := parseRoles(q.Get("as"), allRoles)
	if err != nil {
		writeErr(w, err)
		return
	}
	limit, err := a.limitParam(q, "limit")
	if err != nil {
		writeErr(w, err)
		return
	}
	props, err := boolParam(q, "props", true)
	if err != nil {
		writeErr(w, err)
		return
	}
	found, err := a.roleQueries(value, roles, q["predicate"], limit+1)
	if err != nil {
		writeErr(w, err)
		return
	}
	counts := map[string]roleCount{}
	triples := []*matchedTriple{}
	byKey := map[string]*matchedTriple{}
	for _, role := range roles {
		ts := found[role]
		c := roleCount{Count: len(ts)}
		if int64(len(ts)) > limit {
			c = roleCount{Count: int(limit), More: true}
			ts = ts[:limit]
		}
		counts[role] = c
		for _, t := range ts {
			key := t.Key()
			if m := byKey[key]; m != nil {
				m.Matched = append(m.Matched, role)
				continue
			}
			m := &matchedTriple{TripleJSON: tripleJSON(t, key, props), Matched: []string{role}}
			byKey[key] = m
			triples = append(triples, m)
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"value": value, "roles": counts, "triples": triples,
	})
}

// match returns the triples matching any combination of subject, predicate
// and object, using the most selective index. A query naming none would be a
// full scan, so it is refused.
func (a *API) match(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := a.limitParam(q, "limit")
	if err != nil {
		writeErr(w, err)
		return
	}
	props, err := boolParam(q, "props", true)
	if err != nil {
		writeErr(w, err)
		return
	}
	s, p, o := q.Get("s"), q.Get("p"), q.Get("o")
	hasS, hasP, hasO := q.Has("s"), q.Has("p"), q.Has("o")
	if hasS && hasP && hasO {
		a.triple(w, r)
		return
	}

	// Query the most selective index named; filter the rest on the server
	var bin, value string
	switch {
	case hasS:
		bin, value = RoleSubject, s
	case hasO:
		bin, value = RoleObject, o
	case hasP:
		bin, value = RolePredicate, p
	default:
		writeErr(w, badRequest("give at least one of s, p and o"))
		return
	}
	var terms []*aero.Expression
	if hasP && bin != RolePredicate {
		terms = append(terms, store.PredicateFilter(p))
	}
	if hasO && bin != RoleObject {
		terms = append(terms, aero.ExpEq(aero.ExpStringBin(RoleObject), aero.ExpStringVal(o)))
	}
	var filter *aero.Expression
	switch len(terms) {
	case 1:
		filter = terms[0]
	case 2:
		filter = aero.ExpAnd(terms...)
	}
	ts, err := a.fetch.query(bin, value, filter, limit+1)
	if err != nil {
		writeErr(w, err)
		return
	}
	more := int64(len(ts)) > limit
	if more {
		ts = ts[:limit]
	}
	out := make([]TripleJSON, len(ts))
	for i, t := range ts {
		out[i] = tripleJSON(t, t.Key(), props)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"triples": out, "more": more})
}

// traverse walks 1 or more hops from start values. GET takes repeated start
// and predicate parameters; POST takes a TraverseRequest as JSON.
func (a *API) traverse(w http.ResponseWriter, r *http.Request) {
	req, err := a.traverseRequest(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.limits.RequestTimeout)
	defer cancel()
	began := time.Now()
	res, err := a.traverser.Run(ctx, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	res.Elapsed = time.Since(began).Round(time.Microsecond).String()
	writeJSON(w, http.StatusOK, res)
}

func (a *API) traverseRequest(r *http.Request) (*TraverseRequest, error) {
	req := &TraverseRequest{}
	if r.Method == http.MethodPost {
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(req); err != nil {
			return nil, badRequest("invalid JSON body: %v", err)
		}
	} else {
		q := r.URL.Query()
		req.Start = q["start"]
		req.Predicates = q["predicate"]
		req.Direction = q.Get("direction")
		var err error
		if req.As, err = parseRoles(q.Get("as"), nil); err != nil {
			return nil, err
		}
		if v := q.Get("hops"); v != "" {
			if req.Hops, err = strconv.Atoi(v); err != nil {
				return nil, badRequest("hops: %v", err)
			}
		}
		if v := q.Get("max_degree"); v != "" {
			if req.MaxDegree, err = strconv.ParseInt(v, 10, 64); err != nil {
				return nil, badRequest("max_degree: %v", err)
			}
		}
		if v := q.Get("max_triples"); v != "" {
			if req.MaxTriples, err = strconv.Atoi(v); err != nil {
				return nil, badRequest("max_triples: %v", err)
			}
		}
		if q.Has("props") {
			props, err := boolParam(q, "props", true)
			if err != nil {
				return nil, err
			}
			req.Props = &props
		}
	}

	if len(req.Start) == 0 {
		return nil, badRequest("give at least one start value")
	}
	if _, err := parseRoles(strings.Join(req.As, ","), nil); err != nil {
		return nil, err
	}
	if req.Direction == "" {
		req.Direction = "out"
	}
	if req.Hops == 0 {
		req.Hops = a.limits.DefaultHops
	}
	if req.Hops < 1 || req.Hops > a.limits.MaxHops {
		return nil, badRequest("hops must be 1 to %d", a.limits.MaxHops)
	}
	if req.MaxDegree == 0 {
		req.MaxDegree = a.limits.MaxDegree
	}
	if req.MaxDegree < 1 || req.MaxDegree > a.limits.MaxLimit {
		return nil, badRequest("max_degree must be 1 to %d", a.limits.MaxLimit)
	}
	if req.MaxTriples == 0 {
		req.MaxTriples = a.limits.DefaultTotal
	}
	if req.MaxTriples < 1 || req.MaxTriples > a.limits.MaxTriples {
		return nil, badRequest("max_triples must be 1 to %d", a.limits.MaxTriples)
	}
	return req, nil
}

func requireValue(q url.Values) (string, error) {
	if !q.Has("value") {
		return "", badRequest("value is required")
	}
	return q.Get("value"), nil
}

// parseRoles parses a comma list of roles; empty gives def.
func parseRoles(list string, def []string) ([]string, error) {
	if strings.TrimSpace(list) == "" {
		return def, nil
	}
	var roles []string
	for _, r := range strings.Split(list, ",") {
		r = strings.ToLower(strings.TrimSpace(r))
		switch r {
		case "s":
			r = RoleSubject
		case "p":
			r = RolePredicate
		case "o":
			r = RoleObject
		}
		if !contains(allRoles, r) {
			return nil, badRequest("unknown role %q: want subject, predicate or object", r)
		}
		if !contains(roles, r) {
			roles = append(roles, r)
		}
	}
	return roles, nil
}

func (a *API) limitParam(q url.Values, name string) (int64, error) {
	v := q.Get(name)
	if v == "" {
		return a.limits.DefaultLimit, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 || n > a.limits.MaxLimit {
		return 0, badRequest("%s must be 1 to %d", name, a.limits.MaxLimit)
	}
	return n, nil
}

func boolParam(q url.Values, name string, def bool) (bool, error) {
	v := q.Get(name)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, badRequest("%s must be true or false", name)
	}
	return b, nil
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// logRequests logs each request's method, URL, status and duration.
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.RequestURI(), sw.status, time.Since(began).Round(time.Microsecond))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
