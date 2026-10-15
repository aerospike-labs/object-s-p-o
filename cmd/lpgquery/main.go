// Command lpgquery reads the vertex model written by generate -model lpg: a
// vertex by ID, the vertices holding an identifier, a traversal of one or
// more hops, or a sample of vertex IDs to query. Output is JSON.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"graph-sub-pred-obj/hashing"
	"graph-sub-pred-obj/lpg"
	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	vertex := flag.String("vertex", "", "Vertex ID to read, or to start a traversal from (readable; hashed per -hash-parts)")
	idType := flag.String("id-type", "", "Identifier type to look up, e.g. EMAIL_E (with -id-value)")
	idValue := flag.String("id-value", "", "Identifier value to look up (with -id-type)")
	hops := flag.Int("hops", 0, "Traverse this many hops from -vertex or the identifier's vertices; 0 reads the vertex only")
	dirName := flag.String("dir", "both", "Edges a traversal follows: out, in, or both")
	preds := flag.String("preds", "", "Comma-separated predicates a traversal follows (default all)")
	maxDegree := flag.Int("max-degree", 1000, "Vertices with more edges are returned but not expanded; 0 = no limit")
	sample := flag.Int("sample", 0, "Print this many vertex IDs from a scan, to use as -vertex")
	label := flag.String("label", "", "Print the IDs of vertices of this type (from the type index if it exists, else a filtered scan), up to -limit")
	limit := flag.Int64("limit", 100, "With -label: most IDs to print; 0 = all")
	hubEdges := flag.Int("edges", 20, "With -vertex on a hub: how many of its bucketed edges to show")
	degree := flag.Bool("degree", false, "With -vertex: also count all its edges (reads every edge bucket of a hub)")
	hasEdge := flag.String("has-edge", "", "With -vertex: PREDICATE,TO; report whether the vertex has that outgoing edge")
	hashType := flag.String("hash-type", "no-hash", "Hash function the data was generated with")
	hashParts := flag.String("hash-parts", hashing.PartIdentifiers, hashing.FlagHelp)
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("lpgquery", version.String())
		return
	}
	hasher, err := hashing.New(*hashType, *hashParts)
	if err != nil {
		log.Fatal(err)
	}
	dir, err := lpg.ParseDirection(*dirName)
	if err != nil {
		log.Fatal(err)
	}

	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()
	ls, err := lpg.Open(gs)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	var result interface{}
	switch {
	case *sample > 0:
		result, err = sampleIDs(gs, *sample)
	case *label != "":
		var ids []string
		ids, err = ls.VerticesByType(hasher.TypeName(*label), *limit)
		indexed, _ := ls.LabelIndexed()
		result = map[string]interface{}{"type_index": indexed, "vertices": ids}
	case *idType != "" && *idValue != "":
		var ids []string
		ids, err = ls.LookupIdentifier(hasher.TypeName(*idType), hasher.Content(*idValue))
		if err == nil && *hops > 0 && len(ids) > 0 {
			result, err = traverse(ls, ids, *hops, dir, *preds, hasher, *maxDegree)
		} else {
			result = map[string]interface{}{"vertices": ids}
		}
	case *vertex != "":
		id := hasher.EntityID(*vertex)
		if *hops > 0 {
			result, err = traverse(ls, []string{id}, *hops, dir, *preds, hasher, *maxDegree)
		} else if *hasEdge != "" {
			pred, to, ok := strings.Cut(*hasEdge, ",")
			if !ok {
				log.Fatal("-has-edge takes PREDICATE,TO")
			}
			var found bool
			found, err = ls.HasEdge(id, hasher.Predicate(pred), hasher.EntityID(to))
			result = map[string]interface{}{"from": id, "predicate": pred, "to": hasher.EntityID(to), "exists": found}
		} else {
			result, err = readVertex(ls, id, *hubEdges, *degree)
		}
	default:
		log.Fatal("give -vertex, -id-type and -id-value, -label, or -sample")
	}
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "took %s\n", time.Since(start).Round(time.Microsecond))
}

func traverse(ls *lpg.Store, start []string, hops int, dir lpg.Direction, predList string, hasher *hashing.Hasher, maxDegree int) (interface{}, error) {
	preds := map[string]bool{}
	for _, p := range strings.Split(predList, ",") {
		if p = strings.TrimSpace(p); p != "" {
			preds[hasher.Predicate(p)] = true
		}
	}
	g, err := ls.Traverse(start, hops, dir, preds, maxDegree)
	if err != nil {
		return nil, err
	}
	// Summarize vertices to keep the output readable; read one with -vertex
	type summary struct {
		Type   string `json:"type"`
		Degree int    `json:"inline_degree"`
		Hub    bool   `json:"hub,omitempty"`
		IDs    int    `json:"identifiers"`
	}
	vertices := make(map[string]summary, len(g.Vertices))
	for id, v := range g.Vertices {
		n := 0
		for _, vals := range v.Identifiers {
			n += len(vals)
		}
		vertices[id] = summary{Type: v.Type, Degree: v.InlineDegree(), Hub: v.Hub, IDs: n}
	}
	return map[string]interface{}{
		"vertex_count": len(g.Vertices),
		"edge_count":   len(g.Edges),
		"vertices":     vertices,
		"edges":        g.Edges,
		"hubs":         g.Hubs,
	}, nil
}

// readVertex returns a vertex; for a hub, with the first edges of its edge
// buckets, and with its full degree when degree.
func readVertex(ls *lpg.Store, id string, edges int, degree bool) (interface{}, error) {
	v, err := ls.GetVertex(id)
	if err != nil {
		return nil, err
	}
	if v == nil {
		log.Fatalf("vertex %s not found", id)
	}
	out := map[string]interface{}{"vertex": v}
	if v.Hub && edges > 0 {
		page, err := ls.HubEdges(v, lpg.Both, nil, edges)
		if err != nil {
			return nil, err
		}
		out["hub_edges_first"] = page
	}
	if degree {
		n, err := ls.Degree(id, lpg.Both)
		if err != nil {
			return nil, err
		}
		out["degree"] = n
	}
	return out, nil
}

// sampleIDs returns up to n vertex IDs from a scan.
func sampleIDs(gs *store.GraphStore, n int) ([]string, error) {
	sp := aero.NewScanPolicy()
	sp.MaxRecords = int64(n)
	rs, err := gs.Client.ScanAll(sp, gs.Namespace, lpg.VertexSet, "type")
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var ids []string
	for r := range rs.Results() {
		if r.Err != nil {
			return nil, r.Err
		}
		if id, ok := r.Record.Key.Value().GetObject().(string); ok && len(ids) < n {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
