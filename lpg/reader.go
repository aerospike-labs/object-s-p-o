package lpg

import (
	"fmt"
	"sort"
	"strings"

	"graph-sub-pred-obj/store"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// Vertex is a stored vertex with its inline edges. A hub's further edges are
// in edge buckets, read with HubEdges, HasEdge and Degree.
type Vertex struct {
	ID          string                           `json:"id"`
	Type        string                           `json:"type,omitempty"`
	Created     int64                            `json:"created,omitempty"`
	Identifiers map[string]map[string]Identifier `json:"identifiers,omitempty"`
	Out         map[string]map[string]Edge       `json:"out,omitempty"`
	In          map[string]map[string]Edge       `json:"in,omitempty"`
	Links       int                              `json:"individual_links,omitempty"`
	Hub         bool                             `json:"hub,omitempty"`
	// HubLevels holds, for each direction ("out", "in") with bucketed
	// edges, its newest edge bucket level.
	HubLevels map[string]int `json:"hub_levels,omitempty"`
}

// hubLevel returns the vertex's newest edge bucket level in a direction, if
// it has bucketed edges in that direction.
func (v *Vertex) hubLevel(outgoing bool) (int, bool) {
	l, ok := v.HubLevels[dirName(outgoing)]
	return l, ok
}

// InlineDegree returns the number of edges in the vertex record, in both
// directions; for a vertex that isn't a hub, all of them.
func (v *Vertex) InlineDegree() int {
	return countEdges(v.Out) + countEdges(v.In)
}

// GetVertex returns a vertex, or nil if it doesn't exist.
func (s *Store) GetVertex(id string) (*Vertex, error) {
	vs, err := s.GetVertices([]string{id})
	if err != nil {
		return nil, err
	}
	return vs[0], nil
}

// GetVertices batch-reads vertices. The result is in the order of ids, nil
// where a vertex doesn't exist.
func (s *Store) GetVertices(ids []string) ([]*Vertex, error) {
	keys := make([]*aero.Key, len(ids))
	for i, id := range ids {
		k, err := s.vertexKey(id)
		if err != nil {
			return nil, err
		}
		keys[i] = k
	}
	recs, err := s.gs.Client.BatchGet(s.gs.BatchPolicy, keys)
	if err != nil {
		return nil, err
	}
	out := make([]*Vertex, len(ids))
	for i, r := range recs {
		if r == nil {
			continue
		}
		v := &Vertex{
			ID:          ids[i],
			Identifiers: decodeIdentifiers(r.Bins[binIDs]),
			Out:         decodeEdges(r.Bins[binOut]),
			In:          decodeEdges(r.Bins[binIn]),
		}
		v.Type, _ = r.Bins[binType].(string)
		v.Created = toInt64(r.Bins[binCreated])
		v.Links, _ = r.Bins[binLinks].(int)
		for _, outgoing := range []bool{true, false} {
			if l, ok := r.Bins[levelBin(outgoing)].(int); ok {
				if v.HubLevels == nil {
					v.HubLevels = make(map[string]int, 2)
				}
				v.HubLevels[dirName(outgoing)] = l
				v.Hub = true
			}
		}
		out[i] = v
	}
	return out, nil
}

// bucketsUpTo returns the number of edge buckets in levels 0 to level.
func bucketsUpTo(level int) int {
	return int(hubBuckets(level+1)) - HubBaseBuckets
}

// bucketAt returns the level and bucket of the i-th edge bucket, counting
// level by level.
func bucketAt(i int) (level int, bucket uint64) {
	for uint64(i) >= hubBuckets(level) {
		i -= int(hubBuckets(level))
		level++
	}
	return level, uint64(i)
}

// hubKeys returns the edge bucket keys of a hub in one direction, level by
// level up to level.
func (s *Store) hubKeys(id string, outgoing bool, level int) ([]*aero.Key, error) {
	var keys []*aero.Key
	for l := 0; l <= level; l++ {
		for b := uint64(0); b < hubBuckets(l); b++ {
			k, err := s.edgeBucketKey(id, outgoing, l, b)
			if err != nil {
				return nil, err
			}
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// hubReadChunk is the most edge buckets HubEdges reads per batch.
const hubReadChunk = 64

// HubEdges returns up to limit (0 = all) of a hub's bucketed edges in dir
// whose predicate is in preds (all when empty), reading its edge buckets a
// chunk at a time and stopping once it has enough. The hub's inline edges
// are on its Vertex.
func (s *Store) HubEdges(v *Vertex, dir Direction, preds map[string]bool, limit int) ([]EdgeRef, error) {
	id := v.ID
	var out []EdgeRef
	for _, outgoing := range []bool{true, false} {
		if (outgoing && dir == In) || (!outgoing && dir == Out) {
			continue
		}
		level, ok := v.hubLevel(outgoing)
		if !ok {
			continue
		}
		// A limited read starts with about the buckets it needs and doubles,
		// so a page of a large hub reads a few buckets, not all of them. Keys
		// are made per chunk, since each costs a digest.
		chunk := hubReadChunk
		if limit > 0 {
			chunk = min(hubReadChunk, (limit-len(out))/HubBucketEdges+1)
		}
		total := bucketsUpTo(level)
		for i := 0; i < total; i += chunk {
			if i > 0 {
				chunk = min(hubReadChunk, chunk*2)
			}
			keys := make([]*aero.Key, 0, chunk)
			for j := i; j < min(i+chunk, total); j++ {
				l, b := bucketAt(j)
				k, err := s.edgeBucketKey(id, outgoing, l, b)
				if err != nil {
					return nil, err
				}
				keys = append(keys, k)
			}
			recs, aerr := s.gs.Client.BatchGet(s.gs.BatchPolicy, keys, binBucketE)
			if aerr != nil {
				return nil, aerr
			}
			for _, r := range recs {
				if r == nil {
					continue
				}
				for p, nbrs := range decodeEdges(r.Bins[binBucketE]) {
					if len(preds) > 0 && !preds[p] {
						continue
					}
					for n := range nbrs {
						e := EdgeRef{From: id, Predicate: p, To: n}
						if !outgoing {
							e = EdgeRef{From: n, Predicate: p, To: id}
						}
						out = append(out, e)
						if limit > 0 && len(out) >= limit {
							return out, nil
						}
					}
				}
			}
		}
	}
	return out, nil
}

// HasEdge reports whether the edge from -[pred]-> to exists, looking on the
// from vertex. See hasEdge.
func (s *Store) HasEdge(from, pred, to string) (bool, error) {
	return s.hasEdge(from, true, pred, to)
}

// HasInEdge is HasEdge looking on the to vertex, which is cheaper when from
// is the hub, or the from vertex is unknown to be cheaper.
func (s *Store) HasInEdge(to, pred, from string) (bool, error) {
	return s.hasEdge(to, false, pred, from)
}

// hasEdge reports whether vertex id has an edge to or from nbr: in its
// record, and if it is a hub, in the one bucket per level that nbr hashes
// to. Cost grows with the hub's levels, not its edges.
func (s *Store) hasEdge(id string, outgoing bool, pred, nbr string) (bool, error) {
	bin := binOut
	if !outgoing {
		bin = binIn
	}
	key, err := s.vertexKey(id)
	if err != nil {
		return false, err
	}
	rec, aerr := s.gs.Client.Operate(nil, key,
		aero.ExpReadOp("e", hasEdgeExp(bin, pred, nbr), aero.ExpReadFlagEvalNoFail),
		aero.GetBinOp(levelBin(outgoing)))
	if aerr != nil {
		if aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return false, nil
		}
		return false, aerr
	}
	if found, _ := rec.Bins["e"].(bool); found {
		return true, nil
	}
	level, hub := rec.Bins[levelBin(outgoing)].(int)
	if !hub {
		return false, nil
	}
	keys := make([]*aero.Key, level+1)
	for l := 0; l <= level; l++ {
		if keys[l], err = s.edgeBucketKey(id, outgoing, l, hubBucket(nbr, l)); err != nil {
			return false, err
		}
	}
	recs, aerr := s.gs.Client.BatchGetOperate(s.gs.BatchPolicy, keys,
		aero.ExpReadOp("e", hasEdgeExp(binBucketE, pred, nbr), aero.ExpReadFlagEvalNoFail))
	if aerr != nil {
		return false, aerr
	}
	for _, r := range recs {
		if r == nil {
			continue
		}
		if found, _ := r.Bins["e"].(bool); found {
			return true, nil
		}
	}
	return false, nil
}

// hasEdgeExp is true when the {predicate: {neighbor: tuple}} map bin holds
// the edge, and false, not an error, when the bin or predicate is absent.
func hasEdgeExp(bin, pred, nbr string) *aero.Expression {
	return aero.ExpCond(hasKey(bin, pred),
		aero.ExpMapGetByKey(aero.MapReturnType.EXISTS, aero.ExpTypeBOOL, aero.ExpStringVal(nbr), aero.ExpMapBin(bin), strCtx(pred)),
		aero.ExpBoolVal(false))
}

// Degree returns the number of edges of a vertex in dir: its inline edges
// plus, for a hub, the edge counts of all its buckets. Reads every bucket of
// a hub, so it grows with the hub.
func (s *Store) Degree(id string, dir Direction) (int, error) {
	v, err := s.GetVertex(id)
	if err != nil || v == nil {
		return 0, err
	}
	n := 0
	if dir != In {
		n += countEdges(v.Out)
	}
	if dir != Out {
		n += countEdges(v.In)
	}
	if !v.Hub {
		return n, nil
	}
	for _, outgoing := range []bool{true, false} {
		if (outgoing && dir == In) || (!outgoing && dir == Out) {
			continue
		}
		level, ok := v.hubLevel(outgoing)
		if !ok {
			continue
		}
		keys, err := s.hubKeys(id, outgoing, level)
		if err != nil {
			return 0, err
		}
		recs, aerr := s.gs.Client.BatchGet(s.gs.BatchPolicy, keys, binEdges)
		if aerr != nil {
			return 0, aerr
		}
		for _, r := range recs {
			if r != nil {
				c, _ := r.Bins[binEdges].(int)
				n += c
			}
		}
	}
	return n, nil
}

// IdentifierIndexComplete reports whether the identifier lookup set holds
// every identifier: false once any writer has run without it.
func (s *Store) IdentifierIndexComplete() (bool, error) {
	off, err := s.gs.GetMetaString(IDIndexOffName, "off")
	return off == "", err
}

// LookupIdentifier returns the IDs of the vertices holding an identifier, by
// its type and value as stored. One read of the identifier's bucket finds the
// candidates; a batch read of them confirms each holds the identifier. An
// identifier the lookup set can't answer for, because it is over-shared or
// the set is incomplete, is found by a scan filtered on the server.
func (s *Store) LookupIdentifier(idType, value string) ([]string, error) {
	complete, err := s.IdentifierIndexComplete()
	if err != nil {
		return nil, err
	}
	if !complete {
		return s.scanIdentifier(idType, value)
	}
	bucket, tag := s.identifierSlot(idType, value)
	key, err := s.bucketKey(bucket)
	if err != nil {
		return nil, err
	}
	rec, aerr := s.gs.Client.Operate(nil, key,
		aero.MapGetByKeyOp(binBucket, aero.NewIntegerValue(int(tag)), aero.MapReturnType.VALUE))
	if aerr != nil {
		if aerr.Matches(aero.ErrKeyNotFound.ResultCode) {
			return nil, nil
		}
		return nil, aerr
	}
	candidates := pairs(rec.Bins[binBucket])
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) >= MaxIdentifierShare {
		return s.scanIdentifier(idType, value)
	}
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if s, ok := c.Key.(string); ok {
			ids = append(ids, s)
		}
	}
	sort.Strings(ids)

	keys := make([]*aero.Key, len(ids))
	for i, id := range ids {
		if keys[i], err = s.vertexKey(id); err != nil {
			return nil, err
		}
	}
	recs, aerr := s.gs.Client.BatchGetOperate(s.gs.BatchPolicy, keys,
		aero.MapGetByKeyOp(binIDs, aero.NewStringValue(idType), aero.MapReturnType.VALUE))
	if aerr != nil {
		return nil, aerr
	}
	var found []string
	for i, r := range recs {
		if r == nil {
			continue
		}
		for _, v := range pairs(r.Bins[binIDs]) {
			if v.Key == value {
				found = append(found, ids[i])
				break
			}
		}
	}
	return found, nil
}

// scanIdentifier returns the IDs of the vertices holding an identifier, from
// a scan of all vertices filtered on the server.
func (s *Store) scanIdentifier(idType, value string) ([]string, error) {
	qp := aero.NewQueryPolicy()
	qp.FilterExpression = aero.ExpCond(hasKey(binIDs, idType),
		aero.ExpMapGetByKey(aero.MapReturnType.EXISTS, aero.ExpTypeBOOL, aero.ExpStringVal(value), aero.ExpMapBin(binIDs), strCtx(idType)),
		aero.ExpBoolVal(false))
	qp.IncludeBinData = false
	rs, aerr := s.gs.Client.Query(qp, aero.NewStatement(s.gs.Namespace, VertexSet))
	if aerr != nil {
		return nil, aerr
	}
	defer rs.Close()
	var ids []string
	for r := range rs.Results() {
		if r.Err != nil {
			return nil, r.Err
		}
		if id, ok := r.Record.Key.Value().GetObject().(string); ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// Direction selects which edges a traversal follows.
type Direction int

const (
	Out Direction = iota
	In
	Both
)

// ParseDirection parses "out", "in" or "both".
func ParseDirection(s string) (Direction, error) {
	switch s {
	case "out":
		return Out, nil
	case "in":
		return In, nil
	case "both":
		return Both, nil
	}
	return 0, fmt.Errorf("unknown direction %q (want out, in, or both)", s)
}

// EdgeRef is an edge found by a traversal, always from subject to object.
type EdgeRef struct {
	From      string `json:"from"`
	Predicate string `json:"predicate"`
	To        string `json:"to"`
}

// Subgraph is the result of a traversal.
type Subgraph struct {
	Vertices map[string]*Vertex `json:"vertices"`
	Edges    []EdgeRef          `json:"edges"`
	// Hubs are vertices with more than maxDegree edges, which were not
	// expanded.
	Hubs []string `json:"hubs,omitempty"`
}

// Traverse walks hops hops from start, following edges in dir whose
// predicate is in preds (all when empty). Each hop is one batch read of the
// frontier: no secondary-index query and no fan-out to every node. Vertices
// with more than maxDegree edges (0 = no limit) are returned as hubs but not
// expanded; finding that out reads at most maxDegree+1 of a hub's edges, so
// it costs the same however large the hub grows.
func (s *Store) Traverse(start []string, hops int, dir Direction, preds map[string]bool, maxDegree int) (*Subgraph, error) {
	g := &Subgraph{Vertices: make(map[string]*Vertex)}
	seenEdge := make(map[EdgeRef]bool)
	frontier := start
	for hop := 0; len(frontier) > 0; hop++ {
		vs, err := s.GetVertices(frontier)
		if err != nil {
			return nil, err
		}
		var next []string
		for _, v := range vs {
			if v == nil {
				continue
			}
			g.Vertices[v.ID] = v
			if hop == hops {
				continue
			}
			edges := inlineEdges(v, dir, preds)
			if v.Hub && (maxDegree == 0 || len(edges) <= maxDegree) {
				limit := 0 // all
				if maxDegree > 0 {
					limit = maxDegree + 1 - len(edges)
				}
				more, err := s.HubEdges(v, dir, preds, limit)
				if err != nil {
					return nil, err
				}
				edges = append(edges, more...)
			}
			if maxDegree > 0 && len(edges) > maxDegree {
				g.Hubs = append(g.Hubs, v.ID)
				continue
			}
			for _, e := range edges {
				if !seenEdge[e] {
					seenEdge[e] = true
					g.Edges = append(g.Edges, e)
				}
				n := e.To
				if n == v.ID {
					n = e.From
				}
				if _, ok := g.Vertices[n]; !ok {
					g.Vertices[n] = nil // claimed for the next hop
					next = append(next, n)
				}
			}
		}
		frontier = next
	}
	for id, v := range g.Vertices {
		if v == nil {
			delete(g.Vertices, id) // edge to a vertex with no record
		}
	}
	return g, nil
}

// inlineEdges returns a vertex's inline edges in dir whose predicate is in
// preds (all when empty).
func inlineEdges(v *Vertex, dir Direction, preds map[string]bool) []EdgeRef {
	var out []EdgeRef
	if dir != In {
		for p, nbrs := range v.Out {
			if len(preds) == 0 || preds[p] {
				for n := range nbrs {
					out = append(out, EdgeRef{From: v.ID, Predicate: p, To: n})
				}
			}
		}
	}
	if dir != Out {
		for p, nbrs := range v.In {
			if len(preds) == 0 || preds[p] {
				for n := range nbrs {
					out = append(out, EdgeRef{From: n, Predicate: p, To: v.ID})
				}
			}
		}
	}
	return out
}

// ScanShareable streams the vertices of entityType in partitions
// [begin, begin+count) that INDIVIDUALs link to, fewer than shareMax times
// (0 = no limit), up to about maxRecords. The filter runs on the server, so
// only matching vertices cross the network, and the scan stops early once
// enough are found.
func (s *Store) ScanShareable(begin, count int, entityType string, shareMax int, maxRecords int64, fn func(store.EntityLinks) error) error {
	filter := aero.ExpAnd(
		aero.ExpEq(aero.ExpStringBin(binType), aero.ExpStringVal(entityType)),
		aero.ExpBinExists(binLinks),
	)
	if shareMax > 0 {
		filter = aero.ExpAnd(filter, aero.ExpLess(aero.ExpIntBin(binLinks), aero.ExpIntVal(int64(shareMax))))
	}
	sp := aero.NewScanPolicy()
	sp.FilterExpression = filter
	sp.MaxRecords = maxRecords
	rs, err := s.gs.Client.ScanPartitions(sp, aero.NewPartitionFilterByRange(begin, count),
		s.gs.Namespace, VertexSet, binType, binLinks)
	if err != nil {
		return err
	}
	defer rs.Close()
	for r := range rs.Results() {
		if r.Err != nil {
			return r.Err
		}
		links, _ := r.Record.Bins[binLinks].(int)
		id, ok := r.Record.Key.Value().GetObject().(string)
		if !ok {
			return fmt.Errorf("vertex record without a stored key; vertex keys must be sent")
		}
		if err := fn(store.EntityLinks{ID: id, Type: entityType, Links: links}); err != nil {
			return err
		}
	}
	return nil
}

// pairs returns the entries of a map read from the server, which the client
// returns as []MapPair for ordered maps and as a Go map otherwise.
func pairs(v interface{}) []aero.MapPair {
	switch m := v.(type) {
	case []aero.MapPair:
		return m
	case map[interface{}]interface{}:
		out := make([]aero.MapPair, 0, len(m))
		for k, v := range m {
			out = append(out, aero.MapPair{Key: k, Value: v})
		}
		return out
	}
	return nil
}

func decodeEdges(v interface{}) map[string]map[string]Edge {
	m := pairs(v)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]map[string]Edge, len(m))
	for _, p := range m {
		pred, _ := p.Key.(string)
		nm := pairs(p.Value)
		edges := make(map[string]Edge, len(nm))
		for _, n := range nm {
			id, _ := n.Key.(string)
			t, _ := n.Value.([]interface{})
			e := Edge{Created: listInt(t, 0), LastSeen: listInt(t, 1)}
			if len(t) > 2 {
				for _, p := range pairs(t[2]) {
					if k, ok := p.Key.(string); ok {
						if e.Props == nil {
							e.Props = make(map[string]interface{})
						}
						e.Props[k] = p.Value
					}
				}
			}
			edges[id] = e
		}
		out[pred] = edges
	}
	return out
}

func decodeIdentifiers(v interface{}) map[string]map[string]Identifier {
	m := pairs(v)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]map[string]Identifier, len(m))
	for _, t := range m {
		idType, _ := t.Key.(string)
		vm := pairs(t.Value)
		ids := make(map[string]Identifier, len(vm))
		for _, val := range vm {
			value, _ := val.Key.(string)
			l, _ := val.Value.([]interface{})
			id := Identifier{Created: listInt(l, 0), LastSeen: listInt(l, 1)}
			if len(l) > 2 {
				id.Decay, _ = l[2].(float64)
			}
			ids[value] = id
		}
		out[idType] = ids
	}
	return out
}

func listInt(l []interface{}, i int) int64 {
	if i < len(l) {
		return toInt64(l[i])
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

// LabelIndexed reports whether the namespace has the vertex type index, built
// and readable on every node: a query on an index still being built returns
// only part of the vertices.
func (s *Store) LabelIndexed() (bool, error) {
	cmd := "sindex-list:ns=" + s.gs.Namespace
	nodes := s.gs.Client.GetNodes()
	for _, node := range nodes {
		info, err := node.RequestInfo(aero.NewInfoPolicy(), cmd)
		if err != nil {
			return false, err
		}
		if !IndexReadable(info[cmd], LabelIndexName) {
			return false, nil
		}
	}
	return len(nodes) > 0, nil
}

// IndexReadable reports whether a sindex-list response lists the named
// index in state RW.
func IndexReadable(sindexList, name string) bool {
	for _, entry := range strings.Split(sindexList, ";") {
		if strings.Contains(entry, "indexname="+name+":") {
			return strings.Contains(entry, "state=RW")
		}
	}
	return false
}

// VerticesByType returns the IDs of up to about limit (0 = all) vertices of a
// type: from the type index when it exists, otherwise from a scan filtered on
// the server.
func (s *Store) VerticesByType(entityType string, limit int64) ([]string, error) {
	indexed, err := s.LabelIndexed()
	if err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.gs.Namespace, VertexSet, binType)
	qp := aero.NewQueryPolicy()
	qp.MaxRecords = limit
	if indexed {
		stmt.SetFilter(aero.NewEqualFilter(binType, entityType))
	} else {
		qp.FilterExpression = aero.ExpEq(aero.ExpStringBin(binType), aero.ExpStringVal(entityType))
	}
	rs, aerr := s.gs.Client.Query(qp, stmt)
	if aerr != nil {
		return nil, aerr
	}
	defer rs.Close()
	var ids []string
	for r := range rs.Results() {
		if r.Err != nil {
			return nil, r.Err
		}
		if id, ok := r.Record.Key.Value().GetObject().(string); ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
