// Package lpg stores the graph as a labeled property graph: one record per
// vertex holding its type, its identifiers and its adjacency, instead of one
// record per triple. IS_TYPE triples become the vertex type, HAS_IDENTIFIER
// triples become vertex properties, and every other triple becomes an edge
// kept on both of its vertices. A vertex with more edges than fit inline is
// promoted to a hub, whose further edges are hashed across edge bucket
// records, and identifiers are found through a bucketed lookup set rather
// than a secondary index.
package lpg

import (
	"graph-sub-pred-obj/model"
)

// Sets.
const (
	VertexSet     = "vertices"         // one record per vertex, keyed by entity ID
	EdgeBucketSet = "edge_buckets"     // hub edges, keyed "<id>|<out or in>|<level>|<bucket>"
	IDIndexSet    = "identifier_index" // identifier lookup buckets, keyed by bucket number
)

// Bins. Aerospike limits bin names to 15 characters.
const (
	binType    = "type"            // vertex type (IS_TYPE object)
	binCreated = "created"         // vertex creation time
	binIDs     = "identifiers"     // {identifier type: {value: [created, last_seen, decay]}}
	binOut     = "out_edges"       // {predicate: {neighbor: [created, last_seen]}}
	binIn      = "in_edges"        // {predicate: {neighbor: [created, last_seen]}}
	binLinks   = "indiv_links"     // INDIVIDUALs linked to this vertex, for sharing caps
	binEdges   = "edge_count"      // edge entries written to this record, for its limit
	binHubOut  = "hub_out_level"   // highest outgoing edge bucket level; absent until out edges overflow
	binHubIn   = "hub_in_level"    // highest incoming edge bucket level; absent until in edges overflow
	binVertex  = "vertex_id"       // edge bucket: the hub it belongs to
	binBucketE = "edges"           // edge bucket: {predicate: {neighbor: [created, last_seen]}}
	binBucket  = "vertices_by_tag" // identifier bucket: {tag: {vertex: created}}
)

// Edge holds an edge's properties. It is stored as [created, last_seen] or,
// with other properties, [created, last_seen, {name: value}]: created comes
// first so that a value range over the stored lists selects edges by it.
type Edge struct {
	Created  int64                  `json:"created"`
	LastSeen int64                  `json:"last_seen"`
	Props    map[string]interface{} `json:"props,omitempty"`
}

// tuple returns the stored form of an edge.
func (e Edge) tuple() []interface{} {
	if len(e.Props) == 0 {
		return []interface{}{e.Created, e.LastSeen}
	}
	return []interface{}{e.Created, e.LastSeen, e.Props}
}

// Identifier holds an identifier's properties.
type Identifier struct {
	Created  int64   `json:"created"`
	LastSeen int64   `json:"last_seen"`
	Decay    float64 `json:"decay"`
}

// Mutation is what to add to one vertex. Everything in it is merged into the
// stored vertex: maps gain entries, and an edge or identifier already stored
// keeps the earlier created and the later last_seen of the two.
type Mutation struct {
	ID          string
	Type        string                           // kept only if the vertex has no type yet
	Created     int64                            // the earlier of this and the stored one is kept
	Identifiers map[string]map[string]Identifier // type -> value -> props
	Out, In     map[string]map[string]Edge       // predicate -> neighbor -> props
	// LinkPreds are the predicates of In whose edges count as INDIVIDUAL
	// links, for sharing caps. The stored count is the number of distinct
	// such edges, so writing an edge again doesn't count it again.
	LinkPreds map[string]bool
}

func (m *Mutation) edgeCount() int {
	n := 0
	for _, nbrs := range m.Out {
		n += len(nbrs)
	}
	for _, nbrs := range m.In {
		n += len(nbrs)
	}
	return n
}

// merge adds o, a mutation of the same vertex, to m.
func (m *Mutation) merge(o *Mutation) {
	if m.Type == "" {
		m.Type = o.Type
	}
	if o.Created != 0 && (m.Created == 0 || o.Created < m.Created) {
		m.Created = o.Created
	}
	for t, vals := range o.Identifiers {
		for v, id := range vals {
			m.addIdentifier(t, v, id)
		}
	}
	m.Out = mergeEdges(m.Out, o.Out)
	m.In = mergeEdges(m.In, o.In)
	for p := range o.LinkPreds {
		m.addLinkPred(p)
	}
}

func (m *Mutation) addLinkPred(pred string) {
	if m.LinkPreds == nil {
		m.LinkPreds = make(map[string]bool)
	}
	m.LinkPreds[pred] = true
}

// linkPreds returns the predicates of In that count as INDIVIDUAL links.
func (m *Mutation) linkPreds() []string {
	var out []string
	for p := range m.In {
		if m.LinkPreds[p] {
			out = append(out, p)
		}
	}
	return out
}

func mergeEdges(dst, src map[string]map[string]Edge) map[string]map[string]Edge {
	for p, nbrs := range src {
		for n, e := range nbrs {
			dst = addEdge(dst, p, n, e)
		}
	}
	return dst
}

func addEdge(m map[string]map[string]Edge, pred, nbr string, e Edge) map[string]map[string]Edge {
	if m == nil {
		m = make(map[string]map[string]Edge)
	}
	if m[pred] == nil {
		m[pred] = make(map[string]Edge)
	}
	if prev, ok := m[pred][nbr]; ok {
		e = prev.mergedWith(e)
	}
	m[pred][nbr] = e
	return m
}

// mergedWith returns e and o merged as the store merges them: the earlier
// created and the later last_seen, ignoring zeros, and both sets of props,
// o's winning.
func (e Edge) mergedWith(o Edge) Edge {
	e.Created = earliest(e.Created, o.Created)
	e.LastSeen = max(e.LastSeen, o.LastSeen)
	if len(o.Props) > 0 {
		props := make(map[string]interface{}, len(e.Props)+len(o.Props))
		for k, v := range e.Props {
			props[k] = v
		}
		for k, v := range o.Props {
			props[k] = v
		}
		e.Props = props
	}
	return e
}

// earliest returns the earlier of two creation times, 0 meaning unknown.
func earliest(a, b int64) int64 {
	if a == 0 || (b != 0 && b < a) {
		return b
	}
	return a
}

func (m *Mutation) addIdentifier(idType, value string, id Identifier) {
	if m.Identifiers == nil {
		m.Identifiers = make(map[string]map[string]Identifier)
	}
	if m.Identifiers[idType] == nil {
		m.Identifiers[idType] = make(map[string]Identifier)
	}
	if prev, ok := m.Identifiers[idType][value]; ok {
		id.Created = earliest(prev.Created, id.Created)
		id.LastSeen = max(prev.LastSeen, id.LastSeen)
	}
	m.Identifiers[idType][value] = id
}

// Vocabulary names the predicates that FromTriples treats specially, as
// stored (hashed when predicates are hashed).
type Vocabulary struct {
	IsType string
	// RDFType, when set, is a second type predicate, rdf:type in loaded
	// N-Triples, so RDF classes become vertex types rather than edges to a
	// class vertex that every instance links to.
	RDFType       string
	HasIdentifier string
	// LinkPredicates are the INDIVIDUAL-to-entity predicates whose objects
	// count INDIVIDUAL links, for sharing caps.
	LinkPredicates map[string]bool
}

// FromTriples turns triples into vertex mutations, one per vertex touched, in
// order of first appearance:
//
//   - IS_TYPE (or RDFType) sets the subject's type, the first one seen, and
//     its creation time.
//   - HAS_IDENTIFIER adds the identifier_type and value props to the
//     subject's identifiers. The identifier node itself (the object) is not
//     kept: the lookup set finds the vertex by type and value instead.
//   - A literal (a triple with a datatype prop, as N-Triples literals are
//     loaded) is a vertex property: the predicate is its key, like an
//     identifier type, so the identifier lookup set finds it too. A
//     language-tagged literal's value is "<text>@<lang>".
//   - Any other triple is an edge, added as outgoing on the subject and
//     incoming on the object, with its created and last_seen props and any
//     others.
func FromTriples(triples []*model.Triple, vocab Vocabulary) []*Mutation {
	byID := make(map[string]*Mutation)
	var order []*Mutation
	vertex := func(id string) *Mutation {
		m := byID[id]
		if m == nil {
			m = &Mutation{ID: id}
			byID[id] = m
			order = append(order, m)
		}
		return m
	}

	for _, t := range triples {
		isType := t.Predicate == vocab.IsType || (vocab.RDFType != "" && t.Predicate == vocab.RDFType)
		switch {
		case isType:
			m := vertex(t.Subject)
			if m.Type == "" {
				m.Type = t.Object
			}
			m.Created = earliest(m.Created, propInt(t.Props, "created"))
		case t.Predicate == vocab.HasIdentifier:
			idType, _ := t.Props["identifier_type"].(string)
			value, ok := t.Props["value"].(string)
			if !ok {
				value = t.Object
			}
			vertex(t.Subject).addIdentifier(idType, value, Identifier{
				Created:  propInt(t.Props, "created"),
				LastSeen: propInt(t.Props, "last_seen"),
				Decay:    propFloat(t.Props, "decay"),
			})
		default:
			if _, literal := t.Props["datatype"]; literal {
				value := t.Object
				if lang, _ := t.Props["lang"].(string); lang != "" {
					value += "@" + lang // "chat"@en and "chat"@fr are different values
				}
				vertex(t.Subject).addIdentifier(t.Predicate, value, Identifier{
					Created:  propInt(t.Props, "created"),
					LastSeen: propInt(t.Props, "last_seen"),
				})
				continue
			}
			e := Edge{Created: propInt(t.Props, "created"), LastSeen: propInt(t.Props, "last_seen")}
			for k, v := range t.Props {
				if k != "created" && k != "last_seen" {
					if e.Props == nil {
						e.Props = make(map[string]interface{})
					}
					e.Props[k] = v
				}
			}
			sub := vertex(t.Subject)
			sub.Out = addEdge(sub.Out, t.Predicate, t.Object, e)
			obj := vertex(t.Object)
			obj.In = addEdge(obj.In, t.Predicate, t.Subject, e)
			if vocab.LinkPredicates[t.Predicate] {
				obj.addLinkPred(t.Predicate)
			}
		}
	}
	return order
}

func propInt(props map[string]interface{}, key string) int64 {
	switch v := props[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}

func propFloat(props map[string]interface{}, key string) float64 {
	switch v := props[key].(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	}
	return 0
}
