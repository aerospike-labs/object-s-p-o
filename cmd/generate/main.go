package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

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

// Cardinality caps how many entities of LinkedTo type may link to one entity
// of Type, e.g. one ACCOUNT is shared by at most 4 INDIVIDUALs.
type Cardinality struct {
	Type     string `json:"TYPE"`
	LinkedTo string `json:"LINKED_TO"`
	Max      int    `json:"MAX"`
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
	"HAS_ONE_OR_MORE":  {1, 3},
	"LIVES_AT":         {1, 1},
	"IS_PART_OF":       {1, 1},
	"HAS":              {1, 3},
	"CAN_HAVE_SEVERAL": {1, 5},
	"HAS_AN":           {1, 1},
}

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
	batchSize := flag.Int("batch", 256, "Batch write size")
	workers := flag.Int("workers", 8, "Number of concurrent generator and writer goroutines")
	contentTypesPath := flag.String("content-types", "descriptors/contentTypes.json", "Path to contentTypes.json")
	ontologyPath := flag.String("ontology", "descriptors/ontology.json", "Path to ontology.json")
	cardinalitiesPath := flag.String("cardinalities", "descriptors/cardinalities.json", "Path to cardinalities.json")
	shareRate := flag.Float64("share-rate", 0.2, "Probability an individual links to an existing entity instead of a new one")
	hashType := flag.String("hash-type", "no-hash", "Hash all content: no-hash, sha-256, or sha-512")
	clientIndex := flag.Int("client-index", 0, "This generator's index (0-based) when running on multiple hosts")
	clientCount := flag.Int("client-count", 1, "Total number of generators running at the same time")
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	rebuild := flag.Bool("rebuild-entities", false, "Rebuild the entities set and ID counter from existing triples, then exit (run once, with no loads running)")
	flag.Parse()

	if *showVersion {
		fmt.Println("generate", version.String())
		return
	}
	fmt.Println("generate", version.String())

	var err error
	if hashContent, err = newHashFunc(*hashType); err != nil {
		log.Fatal(err)
	}
	if *clientCount < 1 || *clientIndex < 0 || *clientIndex >= *clientCount {
		log.Fatalf("-client-index must be in [0, %d)", *clientCount)
	}

	var ctFile ContentTypesFile
	var ontFile OntologyFile
	var cardFile CardinalitiesFile
	readJSON(*contentTypesPath, &ctFile)
	readJSON(*ontologyPath, &ontFile)
	readJSON(*cardinalitiesPath, &cardFile)

	fmt.Printf("Loaded %d content types, %d ontology relations, %d cardinalities (hash: %s)\n",
		len(ctFile.ContentTypes), len(ontFile.Ontology), len(cardFile.Cardinalities), *hashType)
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

	if err := gs.CreateIndexes(); err != nil {
		log.Fatalf("failed to create indexes: %v", err)
	}

	pool := newSharePool(*shareRate, cardFile.Cardinalities)

	if *rebuild {
		fmt.Println("Rebuilding entities set from existing triples...")
		entities, maxID, scanned, err := rebuildEntities(gs.ScanEach, pool, ontFile.Ontology)
		if err != nil {
			log.Fatalf("failed to scan triples: %v", err)
		}
		if err := writeInBatches(entities, *batchSize, gs.BatchPutEntityLinks); err != nil {
			log.Fatalf("failed to write entities: %v", err)
		}
		if err := gs.RaiseCounter(idCounterName, maxID); err != nil {
			log.Fatalf("failed to raise ID counter: %v", err)
		}
		counter, _ := gs.GetCounter(idCounterName)
		fmt.Printf("  %d triples scanned, %d entities written, ID counter at %d\n", scanned, len(entities), counter)
		return
	}

	// Refuse to write over data loaded before the ID counter existed
	counter, err := gs.GetCounter(idCounterName)
	if err != nil {
		log.Fatalf("failed to read ID counter: %v", err)
	}
	if counter == 0 {
		existing, err := gs.SetRecordCount(store.SetName)
		if err != nil {
			log.Fatalf("failed to check for existing triples: %v", err)
		}
		if existing > 0 {
			log.Fatalf("%d triples exist but the ID counter is unset; run once with -rebuild-entities first", existing)
		}
	}
	ids.reserve = func(n int64) (int64, error) { return gs.ReserveCounter(idCounterName, n) }

	// Seed sharing from this client's slice of the entities set
	begin, parts := partitionRange(*clientIndex, *clientCount)
	entityCount, err := gs.SetRecordCount(store.EntitySetName)
	if err != nil {
		log.Fatalf("failed to count entities: %v", err)
	}
	loaded := 0
	if entityCount > 0 {
		fmt.Printf("Client %d of %d: loading shareable entities from partitions %d-%d...\n",
			*clientIndex, *clientCount, begin, begin+parts-1)
		scanStart := time.Now()
		loaded, err = loadShareable(gs.ScanEntities, begin, parts, pool, rand.New(rand.NewSource(rand.Int63())))
		if err != nil {
			log.Fatalf("failed to load entities: %v", err)
		}
		fmt.Printf("  scanned in %s\n", time.Since(scanStart).Round(time.Millisecond))
	} else {
		fmt.Printf("Client %d of %d: no shareable entities yet\n", *clientIndex, *clientCount)
	}
	fmt.Printf("  %d entities loaded, ID counter at %d\n", loaded, counter)
	for _, c := range cardFile.Cardinalities {
		fmt.Printf("  %-20s max %d %s, %d shareable\n", c.Type, c.Max, c.LinkedTo, len(pool.avail[c.Type]))
	}
	fmt.Printf("  %-20s no limit, %d shareable\n", "HOUSEHOLD", len(pool.avail["HOUSEHOLD"]))
	fmt.Println()

	fmt.Printf("Generating %d individuals with full entity graphs...\n", *count)

	// Pipeline: generate → batch → write
	tripleCh := make(chan []*model.Triple, *workers*4)
	var writeWg sync.WaitGroup
	var triplesWritten atomic.Int64
	var batchErrors atomic.Int64

	writeBatch := func(batch []*model.Triple) {
		if err := gs.BatchPutTriples(batch); err != nil {
			batchErrors.Add(1)
			log.Printf("batch write error: %v", err)
			return
		}
		triplesWritten.Add(int64(len(batch)))
	}

	// Writer goroutines
	for w := 0; w < *workers; w++ {
		writeWg.Add(1)
		go func() {
			defer writeWg.Done()
			buffer := make([]*model.Triple, 0, *batchSize)
			for batch := range tripleCh {
				buffer = append(buffer, batch...)
				for len(buffer) >= *batchSize {
					writeBatch(buffer[:*batchSize])
					buffer = append(buffer[:0], buffer[*batchSize:]...)
				}
			}
			if len(buffer) > 0 {
				writeBatch(buffer)
			}
		}()
	}

	// Persist entity link counts as additive deltas
	flushEntities := func() {
		if err := writeInBatches(pool.takeDirty(), *batchSize, gs.BatchAddEntityLinks); err != nil {
			batchErrors.Add(1)
			log.Printf("entity write error: %v", err)
		}
	}

	startTime := time.Now()
	done := make(chan struct{})

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
				written := triplesWritten.Load()
				elapsed := time.Since(startTime).Seconds()
				rate := float64(written) / elapsed
				fmt.Printf("\r  triples written: %d  (%.0f/sec)  errors: %d",
					written, rate, batchErrors.Load())
			case <-done:
				return
			}
		}
	}()

	// Generator goroutines
	var genWg sync.WaitGroup
	genCh := make(chan int, *workers*2)
	now := time.Now().UnixMilli()

	for w := 0; w < *workers; w++ {
		genWg.Add(1)
		go func(seed int64) {
			defer genWg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range genCh {
				indID := generateEntityID("INDIVIDUAL")
				tripleCh <- generateIndividualGraph(indID, ctFile.ContentTypes, ontFile.Ontology, now, rng, pool)
			}
		}(rand.Int63())
	}

	// Feed individual IDs
	for i := 0; i < *count; i++ {
		genCh <- i
	}
	close(genCh)
	genWg.Wait()
	close(tripleCh)
	writeWg.Wait()
	close(done)
	<-flusherDone
	flushEntities()

	elapsed := time.Since(startTime)
	written := triplesWritten.Load()
	rate := float64(written) / elapsed.Seconds()
	fmt.Printf("\n\nDone. Generated %d individuals → %d triples in %s (%.0f triples/sec)\n",
		*count, written, elapsed.Round(time.Millisecond), rate)
	if errors := batchErrors.Load(); errors > 0 {
		fmt.Printf("  batch errors: %d\n", errors)
		os.Exit(1)
	}
}
