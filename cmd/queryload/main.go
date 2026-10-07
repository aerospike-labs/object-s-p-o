// Command queryload measures query throughput and latency against the triple
// store, using keys sampled from the data already loaded. Run it on several
// hosts at once (alongside generator loads, if desired) to test the cluster
// under concurrent query and insert traffic.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"
)

// op is one query pattern in the workload mix.
type op struct {
	name string
	desc string
	run  func(q *querier, t *model.Triple) (rows int, err error)
}

type querier struct {
	gs        *store.GraphStore
	limit     int64
	hopFanout int
}

var ops = []op{
	{"spo", "SPO: primary key get of a known triple", func(q *querier, t *model.Triple) (int, error) {
		r, err := q.gs.GetTriple(t.Subject, t.Predicate, t.Object)
		if r == nil {
			return 0, err
		}
		return 1, err
	}},
	{"s", "S??: all triples of a subject", func(q *querier, t *model.Triple) (int, error) {
		r, err := q.gs.QueryByBinLimit("subject", t.Subject, nil, q.limit)
		return len(r), err
	}},
	{"o", "??O: all triples pointing to an object", func(q *querier, t *model.Triple) (int, error) {
		r, err := q.gs.QueryByBinLimit("object", t.Object, nil, q.limit)
		return len(r), err
	}},
	{"sp", "SP?: subject + predicate", func(q *querier, t *model.Triple) (int, error) {
		r, err := q.gs.QueryByBinLimit("subject", t.Subject, store.PredicateFilter(t.Predicate), q.limit)
		return len(r), err
	}},
	{"po", "?PO: predicate + object", func(q *querier, t *model.Triple) (int, error) {
		r, err := q.gs.QueryByBinLimit("object", t.Object, store.PredicateFilter(t.Predicate), q.limit)
		return len(r), err
	}},
	{"hop2", "2-hop outbound traversal, second hop queried concurrently", func(q *querier, t *model.Triple) (int, error) {
		first, err := q.gs.QueryByBinLimit("subject", t.Subject, nil, q.limit)
		if err != nil {
			return 0, err
		}
		seen := map[string]bool{}
		var next []string
		for _, f := range first {
			if !seen[f.Object] && len(next) < q.hopFanout {
				seen[f.Object] = true
				next = append(next, f.Object)
			}
		}
		var rows atomic.Int64
		var firstErr error
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, n := range next {
			wg.Add(1)
			go func(node string) {
				defer wg.Done()
				r, err := q.gs.QueryByBinLimit("subject", node, nil, q.limit)
				rows.Add(int64(len(r)))
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}(n)
		}
		wg.Wait()
		return len(first) + int(rows.Load()), firstErr
	}},
}

// parseMix parses "spo=30,s=20,..." into weights per op index.
func parseMix(mix string) ([]int, error) {
	weights := make([]int, len(ops))
	for _, part := range strings.Split(mix, ",") {
		name, w, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("mix entry %q is not name=weight", part)
		}
		n, err := strconv.Atoi(w)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("mix weight %q is not a non-negative integer", w)
		}
		found := false
		for i, o := range ops {
			if o.name == name {
				weights[i], found = n, true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown query %q", name)
		}
	}
	return weights, nil
}

// opStats accumulates one worker's results for one op.
type opStats struct {
	latencies []time.Duration
	errors    int64
	rows      int64
	firstErr  string
}

func (s *opStats) merge(o *opStats) {
	s.latencies = append(s.latencies, o.latencies...)
	s.errors += o.errors
	s.rows += o.rows
	if s.firstErr == "" {
		s.firstErr = o.firstErr
	}
}

// Summary is one op's result, also emitted with -json.
type Summary struct {
	Op        string  `json:"op"`
	Ops       int     `json:"ops"`
	Errors    int64   `json:"errors"`
	QPS       float64 `json:"qps"`
	RowsPerOp float64 `json:"rows_per_op"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
	MaxMs     float64 `json:"max_ms"`
	FirstErr  string  `json:"first_error,omitempty"`
}

func summarize(name string, s *opStats, elapsed time.Duration) Summary {
	sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })
	pct := func(p float64) float64 {
		if len(s.latencies) == 0 {
			return 0
		}
		i := int(p * float64(len(s.latencies)-1))
		return float64(s.latencies[i].Microseconds()) / 1000
	}
	n := len(s.latencies)
	sum := Summary{Op: name, Ops: n, Errors: s.errors, QPS: float64(n) / elapsed.Seconds(),
		P50Ms: pct(0.50), P95Ms: pct(0.95), P99Ms: pct(0.99), MaxMs: pct(1), FirstErr: s.firstErr}
	if n > 0 {
		sum.RowsPerOp = float64(s.rows) / float64(n)
	}
	return sum
}

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	duration := flag.Duration("duration", 30*time.Second, "How long to run")
	workers := flag.Int("workers", 64, "Concurrent query workers")
	qps := flag.Int("qps", 0, "Target total queries/sec for this process (0 = as fast as possible); latency is measured from each query's scheduled start")
	mix := flag.String("mix", "spo=30,s=20,o=10,sp=15,po=15,hop2=10", "Query mix as name=weight; queries: spo, s, o, sp, po, hop2")
	sampleSize := flag.Int64("sample", 10000, "Triples sampled as query keys")
	limit := flag.Int64("max-results", 100, "Max triples returned per secondary index query")
	hopFanout := flag.Int("hop-fanout", 10, "Max second-hop nodes queried per hop2 traversal")
	clientIndex := flag.Int("client-index", 0, "This process's index (0-based) when running on multiple hosts; picks which partitions to sample")
	clientCount := flag.Int("client-count", 1, "Total number of query load processes")
	short := flag.Bool("short-queries", true, "Run secondary index queries as short queries (for results under ~100 records); avoids FAIL_FORBIDDEN rejections at high concurrency")
	jsonOut := flag.Bool("json", false, "Print the summary as JSON")
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("queryload", version.String())
		return
	}

	weights, err := parseMix(*mix)
	if err != nil {
		log.Fatal(err)
	}
	var cumulative []int
	total := 0
	for _, w := range weights {
		total += w
		cumulative = append(cumulative, total)
	}
	if total == 0 {
		log.Fatal("-mix has no positive weights")
	}
	if *clientCount < 1 || *clientIndex < 0 || *clientIndex >= *clientCount {
		log.Fatalf("-client-index must be in [0, %d)", *clientCount)
	}

	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()
	if *short {
		gs.UseShortQueries()
	}

	begin := *clientIndex * store.PartitionCount / *clientCount
	end := (*clientIndex + 1) * store.PartitionCount / *clientCount
	sample, err := gs.SampleTriples(begin, end-begin, *sampleSize)
	if err != nil {
		log.Fatalf("failed to sample triples: %v", err)
	}
	if len(sample) == 0 {
		log.Fatal("no triples found to sample; load data first")
	}
	logf := func(format string, a ...interface{}) {
		if !*jsonOut {
			fmt.Printf(format, a...)
		}
	}
	logf("queryload %s\n", version.String())
	logf("Sampled %d triples from partitions %d-%d\n", len(sample), begin, end-1)
	logf("Running %d workers for %s (target qps: %s, short queries: %t), mix: %s\n\n", *workers, *duration,
		map[bool]string{true: "unlimited", false: strconv.Itoa(*qps)}[*qps == 0], *short, *mix)

	q := &querier{gs: gs, limit: *limit, hopFanout: *hopFanout}
	start := time.Now()
	deadline := start.Add(*duration)
	var interval time.Duration
	if *qps > 0 {
		interval = time.Second / time.Duration(*qps)
	}
	var slot atomic.Int64
	var completed, failed atomic.Int64

	stats := make([][]opStats, *workers)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		stats[w] = make([]opStats, len(ops))
		wg.Add(1)
		go func(mine []opStats, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				began := time.Now()
				if interval > 0 {
					// Open-loop pacing: each query has a scheduled start time,
					// and latency counts from it, so queueing behind a slow
					// server is measured rather than hidden
					at := start.Add(time.Duration(slot.Add(1)-1) * interval)
					if at.After(deadline) {
						return
					}
					time.Sleep(time.Until(at))
					began = at
				} else if began.After(deadline) {
					return
				}
				pick := rng.Intn(total)
				i := sort.SearchInts(cumulative, pick+1)
				t := sample[rng.Intn(len(sample))]

				rows, err := ops[i].run(q, t)
				s := &mine[i]
				s.latencies = append(s.latencies, time.Since(began))
				s.rows += int64(rows)
				completed.Add(1)
				if err != nil {
					s.errors++
					failed.Add(1)
					if s.firstErr == "" {
						s.firstErr = err.Error()
					}
				}
			}
		}(stats[w], rand.Int63())
	}

	// Progress reporter
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-ticker.C:
				n := completed.Load()
				logf("\r  %4.0fs  queries: %d  qps (last 1s): %d  errors: %d   ",
					time.Since(start).Seconds(), n, n-last, failed.Load())
				last = n
			case <-done:
				return
			}
		}
	}()
	wg.Wait()
	close(done)
	elapsed := time.Since(start)

	var summaries []Summary
	all := &opStats{}
	for i, o := range ops {
		merged := &opStats{}
		for w := range stats {
			merged.merge(&stats[w][i])
		}
		if len(merged.latencies) == 0 {
			continue
		}
		all.merge(merged)
		summaries = append(summaries, summarize(o.name, merged, elapsed))
	}
	summaries = append(summaries, summarize("total", all, elapsed))

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(summaries)
		return
	}
	fmt.Printf("\n\n%-6s %10s %10s %8s %8s %9s %9s %9s %9s\n", "query", "ops", "qps", "errors", "rows/op", "p50 ms", "p95 ms", "p99 ms", "max ms")
	for _, s := range summaries {
		fmt.Printf("%-6s %10d %10.0f %8d %8.1f %9.2f %9.2f %9.2f %9.2f\n",
			s.Op, s.Ops, s.QPS, s.Errors, s.RowsPerOp, s.P50Ms, s.P95Ms, s.P99Ms, s.MaxMs)
	}
	for _, s := range summaries {
		if s.FirstErr != "" && s.Op != "total" {
			fmt.Printf("  first %s error: %s\n", s.Op, s.FirstErr)
		}
	}
}
