package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"graph-sub-pred-obj/hashing"
	"graph-sub-pred-obj/ingest"
	"graph-sub-pred-obj/lpg"
	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"
)

// ---------------------------------------------------------------------------
// Descriptor types
// ---------------------------------------------------------------------------

type PropertyMeta struct {
	Created  int64   `json:"CREATED"`
	LastSeen int64   `json:"LAST_SEEN"`
	Decay    float64 `json:"DECAY"`
}

type ContentTypesFile struct {
	ContentTypes map[string]map[string][]PropertyMeta `json:"contentTypes"`
}

type OntologyRelation struct {
	Sub  string `json:"SUB"`
	Pred string `json:"PRED"`
	Obj  string `json:"OBJ"`
}

type OntologyFile struct {
	Ontology []OntologyRelation `json:"ontology"`
}

// Cardinality bounds the links between entities of LinkedTo type and
// entities of Type: each LinkedTo entity links to Min..Max of Type (e.g. an
// INDIVIDUAL has 0-4 ACCOUNTs), and one entity of Type is shared by at most
// ShareMax of LinkedTo (omitted: never shared). In the old format, with
// neither MIN nor SHARE_MAX, MAX is the sharing cap.
type Cardinality struct {
	Type     string `json:"TYPE"`
	LinkedTo string `json:"LINKED_TO"`
	Min      *int   `json:"MIN"`
	Max      int    `json:"MAX"`
	ShareMax *int   `json:"SHARE_MAX"`
}

type CardinalitiesFile struct {
	Cardinalities []Cardinality `json:"cardinalities"`
}

// ---------------------------------------------------------------------------
// Cardinality rules derived from ontology predicates
// ---------------------------------------------------------------------------

type cardinalityRange struct {
	min int
	max int
}

var predicateCardinality = map[string]cardinalityRange{
	"HAS_ZERO_OR_MORE": {0, 3},
	"HAS_ONE_OR_MORE":  {1, 3},
	"LIVES_AT":         {1, 1},
	"IS_PART_OF":       {1, 1},
	"HAS":              {1, 3},
	"CAN_HAVE_SEVERAL": {1, 5},
	"HAS_AN":           {1, 1},
}

// parseCardinalities returns, per entity type, the range of links each
// INDIVIDUAL makes to it and the most INDIVIDUALs one entity is shared by.
// Types without a range use their predicate's; types without a cap are
// never shared.
func parseCardinalities(cards []Cardinality) (map[string]cardinalityRange, map[string]int, error) {
	ranges := make(map[string]cardinalityRange)
	caps := make(map[string]int)
	for _, c := range cards {
		if c.LinkedTo != "INDIVIDUAL" {
			log.Printf("cardinality %s -> %s ignored: only INDIVIDUAL links are supported", c.Type, c.LinkedTo)
			continue
		}
		switch {
		case c.Min == nil && c.ShareMax == nil:
			log.Printf("cardinality %s has no MIN or SHARE_MAX; using MAX %d as its sharing cap (old format)", c.Type, c.Max)
			caps[c.Type] = c.Max
			continue
		case c.Min == nil && c.Max != 0:
			return nil, nil, fmt.Errorf("cardinality %s: MAX needs MIN (MAX is the per-INDIVIDUAL range; use SHARE_MAX for the sharing cap)", c.Type)
		case c.Min != nil && (*c.Min < 0 || c.Max < *c.Min):
			return nil, nil, fmt.Errorf("cardinality %s: need 0 <= MIN <= MAX, got MIN %d MAX %d", c.Type, *c.Min, c.Max)
		case c.ShareMax != nil && *c.ShareMax < 1:
			return nil, nil, fmt.Errorf("cardinality %s: SHARE_MAX must be at least 1, got %d", c.Type, *c.ShareMax)
		}
		if c.Min != nil {
			ranges[c.Type] = cardinalityRange{*c.Min, c.Max}
		}
		if c.ShareMax != nil {
			caps[c.Type] = *c.ShareMax
		}
	}
	return ranges, caps, nil
}

// Storage models for -model.
const (
	modelTriples = "triples"
	modelLPG     = "lpg"
)

func readJSON(path string, v interface{}) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("failed to read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		log.Fatalf("failed to parse %s: %v", path, err)
	}
}

// writeInBatches writes items in batches of size using write.
func writeInBatches[T any](items []T, size int, write func([]T) error) error {
	for i := 0; i < len(items); i += size {
		end := min(i+size, len(items))
		if err := write(items[i:end]); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	count := flag.Int("count", 1000, "Number of INDIVIDUALs to generate")
	batchSize := flag.Int("batch", 1024, "Batch write size")
	workers := flag.Int("workers", 8, "Number of concurrent generator and writer goroutines")
	contentTypesPath := flag.String("content-types", "descriptors/contentTypes.json", "Path to contentTypes.json")
	ontologyPath := flag.String("ontology", "descriptors/ontology.json", "Path to ontology.json")
	cardinalitiesPath := flag.String("cardinalities", "descriptors/cardinalities.json", "Path to cardinalities.json")
	shareRate := flag.Float64("share-rate", 0.2, "Probability an individual links to an existing entity instead of a new one")
	hashType := flag.String("hash-type", "no-hash", "Hash function for -hash-parts: no-hash, sha-256, or sha-512")
	hashParts := flag.String("hash-parts", hashing.PartIdentifiers, hashing.FlagHelp)
	clientIndex := flag.Int("client-index", 0, "This generator's index (0-based) when running on multiple hosts")
	clientCount := flag.Int("client-count", 1, "Total number of generators running at the same time")
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	rebuild := flag.Bool("rebuild-entities", false, "Rebuild the entities set and ID counter from existing triples, then exit (run once, with no loads running)")
	modelName := flag.String("model", modelTriples, "Storage model: triples (one record per triple) or lpg (one record per vertex, with its identifiers and edges)")
	idBuckets := flag.Uint64("id-buckets", 0, fmt.Sprintf("-model lpg: identifier lookup buckets, fixed on the first load into a namespace (default %d, or the namespace's)", lpg.DefaultIDBuckets))
	maxInlineEdges := flag.Int("max-inline-edges", lpg.DefaultMaxInlineEdges, "-model lpg: edge entries a vertex record holds before further edges go to overflow records")
	idIndex := flag.Bool("id-index", true, "-model lpg: write the identifier lookup set")
	labelIndex := flag.Bool("label-index", false, "-model lpg: create a secondary index on vertex types, so all vertices of a type are found without a scan")
	hubCount := flag.Int("hubs", 0, "Supernodes: number of fixed hub entities that INDIVIDUALs link to without a sharing cap")
	hubType := flag.String("hub-type", "HOUSEHOLD", "Supernodes: entity type of the hubs")
	hubRate := flag.Float64("hub-rate", 0.5, "Supernodes: probability each INDIVIDUAL links to one hub (chosen at random)")
	flag.Parse()

	if *showVersion {
		fmt.Println("generate", version.String())
		return
	}
	fmt.Println("generate", version.String())

	hasher, err := hashing.New(*hashType, *hashParts)
	if err != nil {
		log.Fatal(err)
	}
	hashContent, hashEntityID, hashPredicate, hashTypeName = hasher.Content, hasher.EntityID, hasher.Predicate, hasher.TypeName
	navigation, hashDesc := hasher.Navigation, hasher.Desc
	if *clientCount < 1 || *clientIndex < 0 || *clientIndex >= *clientCount {
		log.Fatalf("-client-index must be in [0, %d)", *clientCount)
	}
	useLPG := *modelName == modelLPG
	if !useLPG && *modelName != modelTriples {
		log.Fatalf("unknown -model %q (want %s or %s)", *modelName, modelTriples, modelLPG)
	}
	if useLPG && *rebuild {
		log.Fatal("-rebuild-entities applies to -model triples; the lpg model counts links on its vertices")
	}

	var ctFile ContentTypesFile
	var ontFile OntologyFile
	var cardFile CardinalitiesFile
	readJSON(*contentTypesPath, &ctFile)
	readJSON(*ontologyPath, &ontFile)
	readJSON(*cardinalitiesPath, &cardFile)

	fmt.Printf("Loaded %d content types, %d ontology relations, %d cardinalities (hash: %s)\n",
		len(ctFile.ContentTypes), len(ontFile.Ontology), len(cardFile.Cardinalities), hashDesc)
	for ct, props := range ctFile.ContentTypes {
		fmt.Printf("  %-20s %d identifiers\n", ct, len(props))
	}
	fmt.Println()

	// Connect to Aerospike
	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()

	var ls *lpg.Store
	mainSet := store.SetName
	if useLPG {
		// No secondary indexes: traversals and identifier lookups are key reads
		if ls, err = lpg.New(gs, *idBuckets); err != nil {
			log.Fatal(err)
		}
		ls.SetMaxInlineEdges(*maxInlineEdges)
		ls.IndexIdentifiers = *idIndex
		if *labelIndex {
			if err := ls.CreateLabelIndex(); err != nil {
				log.Fatalf("failed to create the vertex type index: %v", err)
			}
		}
		mainSet = lpg.VertexSet
		fmt.Printf("Model: lpg (%d identifier buckets, %d inline edges per record, identifier index %v)\n\n",
			ls.IDBuckets, ls.MaxInlineEdges, ls.IndexIdentifiers)
	} else if err := gs.CreateIndexes(); err != nil {
		log.Fatalf("failed to create indexes: %v", err)
	}
	if err := hashing.CheckStored(gs, navigation); err != nil {
		log.Fatal(err)
	}

	linkRanges, shareCaps, err := parseCardinalities(cardFile.Cardinalities)
	if err != nil {
		log.Fatal(err)
	}
	pool := newSharePool(*shareRate, shareCaps)

	// Entity types are stored in the entities set as written to the triples
	// (hashed when -hash-parts includes types); the pool works with readable names
	var typeList []string
	for ct := range ctFile.ContentTypes {
		typeList = append(typeList, ct)
	}
	for _, c := range cardFile.Cardinalities {
		typeList = append(typeList, c.Type)
	}
	readableType := typeNames(typeList, []string{"HOUSEHOLD"})
	typeName := func(stored string) string {
		if t, ok := readableType[stored]; ok {
			return t
		}
		return stored
	}
	storedTypes := func(entities []store.EntityLinks) []store.EntityLinks {
		for i := range entities {
			entities[i].Type = hashTypeName(entities[i].Type)
		}
		return entities
	}

	if *rebuild {
		fmt.Println("Rebuilding entities set from existing triples...")
		predicates := map[string]bool{}
		scan := func(fn func(*model.Triple) error) error {
			return gs.ScanEach(func(t *model.Triple) error {
				predicates[t.Predicate] = true
				return fn(t)
			})
		}
		entities, maxID, scanned, err := rebuildEntities(scan, pool, ontFile.Ontology, typeName)
		if err != nil {
			log.Fatalf("failed to scan triples: %v", err)
		}
		if len(predicates) > 0 {
			names := make([]string, 0, len(predicates))
			for p := range predicates {
				names = append(names, p)
			}
			if err := gs.RecordPredicates(names); err != nil {
				log.Fatalf("failed to record predicates: %v", err)
			}
		}
		if err := writeInBatches(storedTypes(entities), *batchSize, gs.BatchPutEntityLinks); err != nil {
			log.Fatalf("failed to write entities: %v", err)
		}
		if err := gs.RaiseCounter(idCounterName, maxID); err != nil {
			log.Fatalf("failed to raise ID counter: %v", err)
		}
		// Record the counter even when no generated IDs were found, as with
		// loaded data only, so that later generator runs proceed
		if _, err := gs.ReserveCounter(idCounterName, 0); err != nil {
			log.Fatalf("failed to record ID counter: %v", err)
		}
		counter, _ := gs.GetCounter(idCounterName)
		fmt.Printf("  %d triples scanned, %d entities written, %d predicates recorded, ID counter at %d\n", scanned, len(entities), len(predicates), counter)
		if maxID == 0 && strings.Contains(navigation, hashing.PartEntityIDs) {
			fmt.Println("  entity IDs are hashed, so the ID counter can't be recovered from them and was left as is")
		}
		return
	}

	// Refuse to write over data loaded before the ID counter existed
	counterSet, err := gs.CounterExists(idCounterName)
	if err != nil {
		log.Fatalf("failed to read ID counter: %v", err)
	}
	if !counterSet {
		existing, err := gs.SetRecordCount(mainSet)
		if err != nil {
			log.Fatalf("failed to check for existing records: %v", err)
		}
		if existing > 0 && useLPG {
			log.Fatalf("%d records exist in set %s but the ID counter is unset; load into an empty namespace", existing, mainSet)
		}
		if existing > 0 {
			log.Fatalf("%d triples exist but the ID counter is unset; run once with -rebuild-entities first", existing)
		}
	}
	counter, err := gs.GetCounter(idCounterName)
	if err != nil {
		log.Fatalf("failed to read ID counter: %v", err)
	}
	ids.reserve = func(n int64) (int64, error) { return gs.ReserveCounter(idCounterName, n) }

	// Seed sharing from this client's slice of the entities set
	begin, parts := partitionRange(*clientIndex, *clientCount)
	seedSet := store.EntitySetName
	if useLPG {
		seedSet = lpg.VertexSet
		pool.persist = false // vertex writes count links themselves
	}
	entityCount, err := gs.SetRecordCount(seedSet)
	if err != nil {
		log.Fatalf("failed to count entities: %v", err)
	}
	loaded := 0
	if entityCount > 0 && useLPG {
		fmt.Printf("Client %d of %d: loading shareable vertices from partitions %d-%d...\n",
			*clientIndex, *clientCount, begin, begin+parts-1)
		scanStart := time.Now()
		rng := rand.New(rand.NewSource(rand.Int63()))
		for entityType, shareMax := range pool.limits {
			err := ls.ScanShareable(begin, parts, hashTypeName(entityType), shareMax, maxPoolPerType, func(e store.EntityLinks) error {
				pool.add(entityType, e.ID, e.Links, rng)
				loaded++
				return nil
			})
			if err != nil {
				log.Fatalf("failed to load shareable %s vertices: %v", entityType, err)
			}
		}
		fmt.Printf("  scanned in %s\n", time.Since(scanStart).Round(time.Millisecond))
	} else if entityCount > 0 {
		fmt.Printf("Client %d of %d: loading shareable entities from partitions %d-%d...\n",
			*clientIndex, *clientCount, begin, begin+parts-1)
		scanStart := time.Now()
		scan := func(begin, count int, fn func(store.EntityLinks) error) error {
			return gs.ScanEntities(begin, count, func(e store.EntityLinks) error {
				e.Type = typeName(e.Type)
				return fn(e)
			})
		}
		loaded, err = loadShareable(scan, begin, parts, pool, rand.New(rand.NewSource(rand.Int63())))
		if err != nil {
			log.Fatalf("failed to load entities: %v", err)
		}
		fmt.Printf("  scanned in %s\n", time.Since(scanStart).Round(time.Millisecond))
	} else {
		fmt.Printf("Client %d of %d: no shareable entities yet\n", *clientIndex, *clientCount)
	}
	fmt.Printf("  %d entities loaded, ID counter at %d\n", loaded, counter)
	for _, c := range cardFile.Cardinalities {
		if c.LinkedTo != "INDIVIDUAL" {
			continue
		}
		links := "links per predicate"
		if r, ok := linkRanges[c.Type]; ok {
			links = fmt.Sprintf("%d-%d per INDIVIDUAL", r.min, r.max)
		}
		shared := "not shared"
		if cap, ok := shareCaps[c.Type]; ok {
			shared = fmt.Sprintf("shared by max %d, %d shareable", cap, len(pool.avail[c.Type]))
		}
		fmt.Printf("  %-20s %s, %s\n", c.Type, links, shared)
	}
	fmt.Printf("  %-20s shared without limit, %d shareable\n", "HOUSEHOLD", len(pool.avail["HOUSEHOLD"]))
	fmt.Println()

	var hubs *hubSet
	if *hubCount > 0 {
		if hubs, err = newHubSet(*hubCount, *hubType, *hubRate, ontFile.Ontology); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Supernodes: %d %s hubs, linked from %.0f%% of INDIVIDUALs (~%.0f new links per hub)\n\n",
			*hubCount, *hubType, *hubRate*100, float64(*count)**hubRate/float64(*hubCount))
	}

	fmt.Printf("Generating %d individuals with full entity graphs...\n", *count)

	// Pipeline: generate → batch → write. In the lpg model each INDIVIDUAL's
	// triples become vertex mutations before they are batched.
	var generated atomic.Int64 // triples generated
	var addGraph func([]*model.Triple)
	var closeWriter func()
	var written, writeErrors func() int64
	if useLPG {
		vocab := lpg.Vocabulary{
			IsType:         hashPredicate("IS_TYPE"),
			HasIdentifier:  hashPredicate("HAS_IDENTIFIER"),
			LinkPredicates: map[string]bool{},
		}
		for p := range individualPredicates(ontFile.Ontology) {
			vocab.LinkPredicates[hashPredicate(p)] = true
		}
		w := ingest.NewWriter(ls.Apply, *workers, *batchSize)
		addGraph = func(triples []*model.Triple) { w.Add(lpg.FromTriples(triples, vocab)) }
		closeWriter, written, writeErrors = w.Close, w.Written, w.Errors
	} else {
		w := ingest.NewWriter(gs.BatchPutTriples, *workers, *batchSize)
		addGraph = w.Add
		closeWriter, written, writeErrors = w.Close, w.Written, w.Errors
	}
	var entitiesWritten atomic.Int64
	var batchErrors atomic.Int64 // entity write errors; writer counts its own batch errors

	// Persist entity link counts as additive deltas
	writeEntities := func(batch []store.EntityLinks) error {
		if err := gs.BatchAddEntityLinks(batch); err != nil {
			return err
		}
		entitiesWritten.Add(int64(len(batch)))
		return nil
	}
	flushEntities := func() {
		if err := writeInBatches(storedTypes(pool.takeDirty()), *batchSize, writeEntities); err != nil {
			batchErrors.Add(1)
			log.Printf("entity write error: %v", err)
		}
	}

	startTime := time.Now()
	done := make(chan struct{})
	now := time.Now().UnixMilli()
	if hubs != nil {
		// Hubs' own triples are fixed, so every run and host writes the same ones
		triples := hubs.triples(*hubType, ctFile.ContentTypes[*hubType], now)
		generated.Add(int64(len(triples)))
		addGraph(triples)
		if useLPG {
			// Known supernodes: their edges go to edge buckets from the first one
			for _, id := range hubs.ids {
				if err := ls.PromoteHub(id); err != nil {
					log.Fatalf("failed to promote hub %s: %v", id, err)
				}
			}
		}
	}

	// Entity flusher, separate from progress so a slow flush never hides it
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				flushEntities()
			case <-done:
				return
			}
		}
	}()

	// Progress reporter
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				if useLPG {
					triples, muts := generated.Load(), written()
					fmt.Printf("\r  triples modeled: %d (%.0f/sec)  vertex mutations: %d (%.0f/sec)  errors: %d   ",
						triples, float64(triples)/elapsed, muts, float64(muts)/elapsed, writeErrors())
					continue
				}
				written, entities := written(), entitiesWritten.Load()
				fmt.Printf("\r  triples: %d (%.0f/sec)  entities: %d (%.0f/sec)  records: %.0f/sec  errors: %d   ",
					written, float64(written)/elapsed, entities, float64(entities)/elapsed,
					float64(written+entities)/elapsed, writeErrors()+batchErrors.Load())
			case <-done:
				return
			}
		}
	}()

	// Generator goroutines
	var genWg sync.WaitGroup
	genCh := make(chan int, *workers*2)

	for w := 0; w < *workers; w++ {
		genWg.Add(1)
		go func(seed int64) {
			defer genWg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range genCh {
				indID := generateEntityID("INDIVIDUAL")
				triples := generateIndividualGraph(indID, ctFile.ContentTypes, ontFile.Ontology, linkRanges, now, rng, pool)
				if hubs != nil {
					if t := hubs.link(indID, now, rng); t != nil {
						triples = append(triples, t)
					}
				}
				generated.Add(int64(len(triples)))
				addGraph(triples)
			}
		}(rand.Int63())
	}

	// Feed individual IDs
	for i := 0; i < *count; i++ {
		genCh <- i
	}
	close(genCh)
	genWg.Wait()
	closeWriter()
	close(done)
	<-flusherDone
	flushEntities()

	elapsed := time.Since(startTime)
	if useLPG {
		triples, muts := generated.Load(), written()
		fmt.Printf("\n\nDone. Generated %d individuals → %d triples modeled as %d vertex mutations in %s (%.0f triples/sec)\n",
			*count, triples, muts, elapsed.Round(time.Millisecond), float64(triples)/elapsed.Seconds())
		printLPGRecords(gs)
	} else {
		written, entities := written(), entitiesWritten.Load()
		fmt.Printf("\n\nDone. Generated %d individuals → %d triples + %d entity records in %s (%.0f triples/sec, %.0f records/sec)\n",
			*count, written, entities, elapsed.Round(time.Millisecond),
			float64(written)/elapsed.Seconds(), float64(written+entities)/elapsed.Seconds())
	}
	if errors := writeErrors() + batchErrors.Load(); errors > 0 {
		fmt.Printf("  batch errors: %d\n", errors)
		os.Exit(1)
	}
}

// printLPGRecords prints the record count of each lpg set and the primary
// index memory it takes: 64 bytes per record, replicas included, since the
// counts are summed over all nodes.
func printLPGRecords(gs *store.GraphStore) {
	var total int64
	for _, set := range []struct{ name, desc string }{
		{lpg.VertexSet, "vertices"},
		{lpg.EdgeBucketSet, "hub edge buckets"},
		{lpg.IDIndexSet, "identifier buckets"},
	} {
		n, err := gs.SetRecordCount(set.name)
		if err != nil {
			log.Printf("failed to count set %s: %v", set.name, err)
			return
		}
		total += n
		fmt.Printf("  set %-16s %-22s %d records\n", set.name, set.desc, n)
	}
	fmt.Printf("  primary index: %d records (all copies) × 64 B = %.2f GiB in the namespace\n",
		total, float64(total*64)/(1<<30))
}
