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
// stored vertex: maps gain entries and Links is added to the stored count.
type Mutation struct {
	ID          string
	Type        string // set only when the vertex is created
	Created     int64
	Identifiers map[string]map[string]Identifier // type -> value -> props
	Out, In     map[string]map[string]Edge       // predicate -> neighbor -> props
	Links       int                              // INDIVIDUAL links added
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
	if o.Type != "" {
		m.Type, m.Created = o.Type, o.Created
	}
	for t, vals := range o.Identifiers {
		for v, id := range vals {
			m.addIdentifier(t, v, id)
		}
	}
	m.Out = mergeEdges(m.Out, o.Out)
	m.In = mergeEdges(m.In, o.In)
	m.Links += o.Links
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
	m[pred][nbr] = e
	return m
}

func (m *Mutation) addIdentifier(idType, value string, id Identifier) {
	if m.Identifiers == nil {
		m.Identifiers = make(map[string]map[string]Identifier)
	}
	if m.Identifiers[idType] == nil {
		m.Identifiers[idType] = make(map[string]Identifier)
	}
	m.Identifiers[idType][value] = id
}

// Vocabulary names the predicates that FromTriples treats specially, as
// stored (hashed when predicates are hashed).
type Vocabulary struct {
	IsType        string
	HasIdentifier string
	// LinkPredicates are the INDIVIDUAL-to-entity predicates whose objects
	// count INDIVIDUAL links, for sharing caps.
	LinkPredicates map[string]bool
}

// FromTriples turns triples into vertex mutations, one per vertex touched, in
// order of first appearance:
//
//   - IS_TYPE sets the subject's type and creation time.
//   - HAS_IDENTIFIER adds the identifier_type and value props to the
//     subject's identifiers. The identifier node itself (the object) is not
//     kept: the lookup set finds the vertex by type and value instead.
//   - A literal (a triple with a datatype prop, as N-Triples literals are
//     loaded) is a vertex property: the predicate is its key, like an
//     identifier type, so the identifier lookup set finds it too.
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
		switch t.Predicate {
		case vocab.IsType:
			m := vertex(t.Subject)
			m.Type, m.Created = t.Object, propInt(t.Props, "created")
		case vocab.HasIdentifier:
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
				vertex(t.Subject).addIdentifier(t.Predicate, t.Object, Identifier{
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
				obj.Links++
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
