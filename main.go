package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"graph-sub-pred-obj/graph"
	"graph-sub-pred-obj/ingest"
	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
)

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	flag.Parse()

	// Connect to Aerospike
	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()

	// Create secondary indexes
	if err := gs.CreateIndexes(); err != nil {
		log.Fatalf("failed to create indexes: %v", err)
	}
	fmt.Println("indexes ready")

	// Sample RDF triples representing a social/knowledge graph
	triples := []*model.Triple{
		{Subject: "user:alice", Predicate: "follows", Object: "user:bob", Props: map[string]interface{}{"since": time.Now().UnixMilli(), "source": "api"}},
		{Subject: "user:alice", Predicate: "likes", Object: "post:101", Props: map[string]interface{}{"weight": 0.9}},
		{Subject: "user:bob", Predicate: "follows", Object: "user:charlie"},
		{Subject: "user:bob", Predicate: "authored", Object: "post:101"},
		{Subject: "user:charlie", Predicate: "follows", Object: "user:diana"},
		{Subject: "user:charlie", Predicate: "likes", Object: "post:201"},
		{Subject: "user:diana", Predicate: "authored", Object: "post:201"},
		{Subject: "post:101", Predicate: "tagged", Object: "topic:golang"},
		{Subject: "post:201", Predicate: "tagged", Object: "topic:graphs"},
		{Subject: "topic:golang", Predicate: "related_to", Object: "topic:graphs"},
	}

	// Batch ingest
	loader := ingest.NewLoader(gs, 256)
	if err := loader.LoadTriples(triples); err != nil {
		log.Fatalf("ingestion failed: %v", err)
	}
	fmt.Printf("ingested %d triples\n\n", len(triples))

	// --- Query Pattern Demos ---

	// SPO: exact triple lookup
	fmt.Println("=== SPO: does alice follow bob? ===")
	t, err := gs.GetTriple("user:alice", "follows", "user:bob")
	if err != nil {
		log.Fatal(err)
	}
	if t != nil {
		fmt.Printf("  yes: %s -[%s]-> %s  props=%v\n", t.Subject, t.Predicate, t.Object, t.Props)
	}

	// S??: all triples for a subject
	fmt.Println("\n=== S??: everything about alice ===")
	results, err := gs.QueryBySubject("user:alice")
	if err != nil {
		log.Fatal(err)
	}
	printTriples(results)

	// SP?: subject + predicate
	fmt.Println("\n=== SP?: who does bob follow? ===")
	results, err = gs.QuerySP("user:bob", "follows")
	if err != nil {
		log.Fatal(err)
	}
	printTriples(results)

	// ?P?: all triples with a predicate
	fmt.Println("\n=== ?P?: all 'authored' relationships ===")
	results, err = gs.QueryByPredicate("authored")
	if err != nil {
		log.Fatal(err)
	}
	printTriples(results)

	// ?PO: predicate + object
	fmt.Println("\n=== ?PO: who authored post:101? ===")
	results, err = gs.QueryPO("authored", "post:101")
	if err != nil {
		log.Fatal(err)
	}
	printTriples(results)

	// ??O: all triples pointing to an object
	fmt.Println("\n=== ??O: everything pointing to topic:graphs ===")
	results, err = gs.QueryByObject("topic:graphs")
	if err != nil {
		log.Fatal(err)
	}
	printTriples(results)

	// Pattern query (high-level API)
	fmt.Println("\n=== PatternQuery: S?O alice -> post:101 ===")
	pq := &graph.PatternQuery{Subject: graph.S("user:alice"), Object: graph.O("post:101")}
	results, err = pq.Execute(gs)
	if err != nil {
		log.Fatal(err)
	}
	printTriples(results)

	// --- Graph Traversal ---
	fmt.Println("\n=== Outbound traversal from alice (depth=3) ===")
	paths, err := graph.TraverseOutbound(gs, "user:alice", 3, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range paths {
		fmt.Printf("  depth=%d: ", p.Depth)
		for i, step := range p.Path {
			if i > 0 {
				fmt.Print(" -> ")
			}
			fmt.Printf("%s -[%s]-> %s", step.Subject, step.Predicate, step.Object)
		}
		fmt.Println()
	}

	fmt.Println("\n=== Outbound traversal from alice (follows only, depth=3) ===")
	paths, err = graph.TraverseOutbound(gs, "user:alice", 3, "follows")
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range paths {
		fmt.Printf("  depth=%d: ", p.Depth)
		for i, step := range p.Path {
			if i > 0 {
				fmt.Print(" -> ")
			}
			fmt.Printf("%s -> %s", step.Subject, step.Object)
		}
		fmt.Println()
	}

	fmt.Println("\n=== Inbound traversal to topic:graphs (depth=2) ===")
	paths, err = graph.TraverseInbound(gs, "topic:graphs", 2, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range paths {
		fmt.Printf("  depth=%d: ", p.Depth)
		for i, step := range p.Path {
			if i > 0 {
				fmt.Print(" <- ")
			}
			fmt.Printf("%s <-[%s]- %s", step.Object, step.Predicate, step.Subject)
		}
		fmt.Println()
	}
}

func printTriples(triples []*model.Triple) {
	for _, t := range triples {
		if t.Props != nil && len(t.Props) > 0 {
			fmt.Printf("  %s -[%s]-> %s  props=%v\n", t.Subject, t.Predicate, t.Object, t.Props)
		} else {
			fmt.Printf("  %s -[%s]-> %s\n", t.Subject, t.Predicate, t.Object)
		}
	}
}
