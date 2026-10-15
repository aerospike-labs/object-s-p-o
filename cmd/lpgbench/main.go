// Command lpgbench grows one supernode in the vertex model step by step and,
// at each size, measures the latency of the operations that touch it: adding
// an edge, checking for an edge, reading a page of its edges, reading its
// vertex, and reading and traversing from its ordinary neighbors. It shows
// which operations stay flat as the hub grows, and which (a full degree
// count) grow with it.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"graph-sub-pred-obj/lpg"
	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"
)

const pred = "IS_PART_OF"

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	stepList := flag.String("steps", "1000,10000,100000,1000000", "Hub sizes (incoming edges) to measure at")
	samples := flag.Int("samples", 2000, "Operations measured per operation type and step")
	concurrency := flag.Int("concurrency", 16, "Concurrent operations while measuring")
	growWorkers := flag.Int("grow-workers", 16, "Concurrent writers growing the hub between steps")
	growBatch := flag.Int("grow-batch", 1000, "New neighbors per batch while growing")
	degreeSamples := flag.Int("degree-samples", 20, "Full degree counts measured per step (they read every edge bucket)")
	timeout := flag.Duration("timeout", 30*time.Second, "Timeout of each Aerospike command")
	run := flag.String("run", strconv.FormatInt(time.Now().Unix(), 36), "Run name, prefixed to every vertex ID this run writes")
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("lpgbench", version.String())
		return
	}
	var steps []int
	for _, s := range strings.Split(*stepList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			log.Fatalf("invalid step %q", s)
		}
		steps = append(steps, n)
	}
	sort.Ints(steps)

	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()
	// Growth writes large batches as fast as the cluster takes them
	gs.BatchPolicy.TotalTimeout = *timeout
	gs.BatchPolicy.SocketTimeout = *timeout
	gs.WritePolicy.TotalTimeout = *timeout
	gs.WritePolicy.SocketTimeout = *timeout
	ls, err := lpg.New(gs, 0)
	if err != nil {
		log.Fatal(err)
	}

	b := &bench{ls: ls, hub: fmt.Sprintf("BENCH-%s-HOUSEHOLD:HUB", *run), prefix: fmt.Sprintf("BENCH-%s-INDIVIDUAL:", *run)}
	if err := ls.Apply([]*lpg.Mutation{{ID: b.hub, Type: "HOUSEHOLD", Created: time.Now().UnixMilli()}}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("lpgbench %s: hub %s, %d samples per operation at concurrency %d\n\n", version.String(), b.hub, *samples, *concurrency)

	ops := []struct {
		name string
		n    int
		fn   func(rng *rand.Rand) error
	}{
		{"add edge", *samples, b.addEdge},
		{"has edge (hub side)", *samples, b.hasEdgeOnHub},
		{"has edge (leaf side)", *samples, b.hasEdgeOnLeaf},
		{"read hub vertex", *samples, b.readHub},
		{"page 100 hub edges", *samples, b.pageHub},
		{"read leaf vertex", *samples, b.readLeaf},
		{"2-hop from leaf", *samples, b.twoHops},
		{"hub degree (all)", *degreeSamples, b.degree},
	}

	header := fmt.Sprintf("%-22s %10s %10s %10s %10s", "operation", "p50 ms", "p95 ms", "p99 ms", "ops/sec")
	for _, target := range steps {
		start := time.Now()
		if err := b.grow(target, *growWorkers, *growBatch); err != nil {
			log.Fatal(err)
		}
		v, err := ls.GetVertex(b.hub)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("== hub at %d edges (grown in %s; incoming edges at bucket level %d, %d edge buckets)\n",
			b.leaves.Load(), time.Since(start).Round(time.Millisecond), v.HubLevels["in"], bucketsUpTo(v.HubLevels["in"]))
		fmt.Println(header)
		for _, op := range ops {
			lat, elapsed, err := measure(op.n, *concurrency, op.fn)
			if err != nil {
				log.Fatalf("%s: %v", op.name, err)
			}
			fmt.Printf("%-22s %10.2f %10.2f %10.2f %10.0f\n", op.name,
				pct(lat, 50), pct(lat, 95), pct(lat, 99), float64(len(lat))/elapsed.Seconds())
		}
		fmt.Println()
	}
}

func bucketsUpTo(level int) int {
	n := 0
	for l := 0; l <= level; l++ {
		n += lpg.HubBaseBuckets << l
	}
	return n
}

type bench struct {
	ls     *lpg.Store
	hub    string
	prefix string
	leaves atomic.Int64 // neighbors written so far, numbered from 0
}

func (b *bench) leafID(i int64) string { return b.prefix + strconv.FormatInt(i, 10) }

// leafMutations returns the writes linking new neighbors [from, to) to the
// hub: each neighbor's vertex with its outgoing edge, and the hub's incoming
// edges, as the generator writes them.
func (b *bench) leafMutations(from, to int64) []*lpg.Mutation {
	now := time.Now().UnixMilli()
	hub := &lpg.Mutation{ID: b.hub, In: map[string]map[string]lpg.Edge{pred: {}}, Links: int(to - from)}
	muts := make([]*lpg.Mutation, 0, to-from+1)
	for i := from; i < to; i++ {
		id := b.leafID(i)
		muts = append(muts, &lpg.Mutation{ID: id, Type: "INDIVIDUAL", Created: now,
			Out: map[string]map[string]lpg.Edge{pred: {b.hub: {Created: now, LastSeen: now}}}})
		hub.In[pred][id] = lpg.Edge{Created: now, LastSeen: now}
	}
	return append(muts, hub)
}

// grow adds neighbors until the hub has target edges.
func (b *bench) grow(target, workers, batch int) error {
	var next atomic.Int64
	next.Store(b.leaves.Load())
	var wg sync.WaitGroup
	var firstErr atomic.Value
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				from := next.Add(int64(batch)) - int64(batch)
				if from >= int64(target) {
					return
				}
				to := min(from+int64(batch), int64(target))
				if err := b.ls.Apply(b.leafMutations(from, to)); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok {
		return err
	}
	b.leaves.Store(max(b.leaves.Load(), int64(target)))
	return nil
}

func (b *bench) randomLeaf(rng *rand.Rand) string {
	return b.leafID(rng.Int63n(b.leaves.Load()))
}

// addEdge links one new neighbor to the hub. Its IDs are numbered past the
// grown range, so they don't change what the other operations sample.
func (b *bench) addEdge(rng *rand.Rand) error {
	i := 1<<40 + rng.Int63()>>24
	return b.ls.Apply(b.leafMutations(i, i+1))
}

func (b *bench) hasEdgeOnHub(rng *rand.Rand) error {
	leaf := b.randomLeaf(rng)
	ok, err := b.ls.HasInEdge(b.hub, pred, leaf)
	if err == nil && !ok {
		err = fmt.Errorf("hub has no edge from %s", leaf)
	}
	return err
}

func (b *bench) hasEdgeOnLeaf(rng *rand.Rand) error {
	leaf := b.randomLeaf(rng)
	ok, err := b.ls.HasEdge(leaf, pred, b.hub)
	if err == nil && !ok {
		err = fmt.Errorf("%s has no edge to the hub", leaf)
	}
	return err
}

func (b *bench) readHub(*rand.Rand) error {
	_, err := b.ls.GetVertex(b.hub)
	return err
}

func (b *bench) pageHub(*rand.Rand) error {
	v, err := b.ls.GetVertex(b.hub)
	if err != nil {
		return err
	}
	_, err = b.ls.HubEdges(v, lpg.In, nil, 100)
	return err
}

func (b *bench) readLeaf(rng *rand.Rand) error {
	v, err := b.ls.GetVertex(b.randomLeaf(rng))
	if err == nil && v == nil {
		err = fmt.Errorf("leaf missing")
	}
	return err
}

// twoHops traverses two hops from a neighbor of the hub; the hub is reported
// as a hub and not expanded.
func (b *bench) twoHops(rng *rand.Rand) error {
	g, err := b.ls.Traverse([]string{b.randomLeaf(rng)}, 2, lpg.Both, nil, 1000)
	if err == nil && len(g.Hubs) == 0 && b.leaves.Load() > 1000 {
		err = fmt.Errorf("traversal expanded the hub")
	}
	return err
}

func (b *bench) degree(*rand.Rand) error {
	n, err := b.ls.Degree(b.hub, lpg.In)
	if err == nil && int64(n) < b.leaves.Load() {
		err = fmt.Errorf("degree %d under %d edges written", n, b.leaves.Load())
	}
	return err
}

// measure runs fn n times from concurrency goroutines and returns each
// call's latency in milliseconds.
func measure(n, concurrency int, fn func(*rand.Rand) error) ([]float64, time.Duration, error) {
	lat := make([]float64, n)
	var next atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				i := next.Add(1) - 1
				if i >= int64(n) {
					return
				}
				t := time.Now()
				if err := fn(rng); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				lat[i] = float64(time.Since(t).Microseconds()) / 1000
			}
		}(rand.Int63())
	}
	wg.Wait()
	elapsed := time.Since(start)
	if err, ok := firstErr.Load().(error); ok {
		return nil, 0, err
	}
	sort.Float64s(lat)
	return lat, elapsed, nil
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p / 100)
	return sorted[i]
}
