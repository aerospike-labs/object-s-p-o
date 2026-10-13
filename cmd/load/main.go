// Command load reads RDF triples from CSV, TSV, JSON Lines or N-Triples files
// into the triples set, in parallel from one or many client hosts.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"graph-sub-pred-obj/hashing"
	"graph-sub-pred-obj/ingest"
	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"
)

// dryRunTriples is how many triples -dry-run prints.
const dryRunTriples = 10

// shownRejects is how many rejected records are printed; all of them go to
// -reject-file.
const shownRejects = 10

var errTooManyRejects = errors.New("too many rejected records")

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	loadType := flag.String("load_type", "", "Input format: csv, tsv, jsonl, or nt")
	input := flag.String("input", "", "Comma-separated files, globs, or directories to load")
	subjectCol := flag.String("subject_column", "", "Subject column: CSV/TSV header name, or JSONL path such as .person.id")
	predicateCol := flag.String("predicate_column", "", "Predicate column: CSV/TSV header name, or JSONL path")
	objectCol := flag.String("object_column", "", "Object column: CSV/TSV header name, or JSONL path")
	propsCols := flag.String("props_columns", "", "Comma-separated further columns (header names or JSONL paths) stored in each triple's props")
	delimiter := flag.String("delimiter", "", "Field delimiter for csv (default ,) or tsv (default tab)")
	csvMultiline := flag.Bool("csv-multiline", false, "CSV fields may contain quoted newlines: read each CSV file whole instead of in splits")
	splitMB := flag.Int64("split-mb", 64, "Read files in splits of about this many MB, in parallel; 0 reads each file whole")
	readers := flag.Int("readers", runtime.NumCPU(), "Concurrent file readers")
	workers := flag.Int("workers", 8, "Concurrent writer goroutines")
	batchSize := flag.Int("batch", 1024, "Batch write size")
	propsMode := flag.String("props-mode", "replace", "When a triple already exists: replace its props, or merge into them")
	rejectPath := flag.String("reject-file", "", "Write every rejected record, with its file, byte offset and reason, to this file")
	maxRejects := flag.Int64("max-errors", 1000, "Stop after this many rejected records; 0 = no limit")
	dryRun := flag.Bool("dry-run", false, "Check the columns of each file and print the first triples without connecting or writing")
	hashType := flag.String("hash-type", "no-hash", "Hash function for -hash-parts: no-hash, sha-256, or sha-512")
	hashParts := flag.String("hash-parts", hashing.PartIdentifiers, hashing.FlagHelp)
	clientIndex := flag.Int("client-index", 0, "This loader's index (0-based) when running on multiple hosts")
	clientCount := flag.Int("client-count", 1, "Total number of loaders sharing the same -input")
	rebuildPreds := flag.Bool("rebuild-predicates", false, "Record the predicates of all existing triples in the meta set, then exit (for data written before predicates were recorded)")
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("load", version.String())
		return
	}
	fmt.Println("load", version.String())

	if *rebuildPreds {
		gs, err := conn.Connect(*namespace)
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}
		defer gs.Close()
		fmt.Println("Recording predicates of existing triples...")
		scanned, err := gs.RebuildPredicates()
		if err != nil {
			log.Fatalf("failed to scan triples: %v", err)
		}
		preds, err := gs.Predicates()
		if err != nil {
			log.Fatalf("failed to read predicates: %v", err)
		}
		fmt.Printf("  %d triples scanned, %d predicates recorded\n", scanned, len(preds))
		return
	}

	if *input == "" {
		log.Fatal("-input is required")
	}
	if *clientCount < 1 || *clientIndex < 0 || *clientIndex >= *clientCount {
		log.Fatalf("-client-index must be in [0, %d)", *clientCount)
	}
	if *readers < 1 || *workers < 1 {
		log.Fatal("-readers and -workers must be at least 1")
	}
	var comma rune
	if *delimiter != "" {
		if *delimiter == `\t` {
			*delimiter = "\t"
		}
		if utf8.RuneCountInString(*delimiter) != 1 {
			log.Fatalf("-delimiter must be a single character, got %q", *delimiter)
		}
		comma, _ = utf8.DecodeRuneInString(*delimiter)
	}
	parser, err := ingest.NewParser(*loadType, ingest.Columns{
		Subject:   *subjectCol,
		Predicate: *predicateCol,
		Object:    *objectCol,
		Props:     ingest.SplitList(*propsCols),
	}, comma)
	if err != nil {
		log.Fatal(err)
	}

	hasher, err := hashing.New(*hashType, *hashParts)
	if err != nil {
		log.Fatal(err)
	}
	// hash applies -hash-type to each triple read, as the generator would
	hash := func(t *model.Triple) *model.Triple { return t }
	if hasher.Active() {
		hash = func(t *model.Triple) *model.Triple {
			literal := *loadType == "nt" && t.Props["datatype"] != nil
			return hasher.Loaded(t, literal)
		}
	}

	files, err := ingest.ExpandInputs(*input)
	if err != nil {
		log.Fatal(err)
	}
	splittable := func(string) bool { return *loadType != "csv" || !*csvMultiline }
	allSplits, err := ingest.PlanSplits(files, *splitMB<<20, splittable)
	if err != nil {
		log.Fatal(err)
	}
	splits := ingest.ClientSplits(allSplits, *clientIndex, *clientCount)

	// Check every file this client reads before writing anything
	var mine []string
	for _, s := range splits {
		if len(mine) == 0 || mine[len(mine)-1] != s.Path {
			mine = append(mine, s.Path)
		}
	}
	fmt.Printf("Client %d of %d: %d of %d splits from %d of %d files (%s, hash: %s)\n",
		*clientIndex, *clientCount, len(splits), len(allSplits), len(mine), len(files), *loadType, hasher.Desc)
	for _, path := range mine {
		desc, err := parser.Check(path)
		if err != nil {
			log.Fatal(err)
		}
		if *dryRun || len(mine) <= 5 {
			fmt.Printf("  %s: %s\n", path, desc)
		}
	}
	if len(splits) == 0 {
		fmt.Println("Nothing to load")
		return
	}

	if *dryRun {
		dryRunSplit(parser, splits[0], hash)
		return
	}

	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()
	if err := gs.CreateIndexes(); err != nil {
		log.Fatalf("failed to create indexes: %v", err)
	}
	if err := hashing.CheckStored(gs, hasher.Navigation); err != nil {
		log.Fatal(err)
	}

	write := gs.BatchPutTriples
	switch *propsMode {
	case "replace":
	case "merge":
		write = gs.BatchMergeTriples
	default:
		log.Fatalf("unknown -props-mode %q (want replace or merge)", *propsMode)
	}

	rejects, err := newRejectLog(*rejectPath, *maxRejects)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Loading with %d readers and %d writers...\n", *readers, *workers)
	writer := ingest.NewWriter(write, *workers, *batchSize)
	var rows atomic.Int64
	var failed atomic.Bool
	var firstErr error
	var errOnce sync.Once
	fail := func(err error) {
		errOnce.Do(func() { firstErr = err })
		failed.Store(true)
	}

	startTime := time.Now()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				written := writer.Written()
				fmt.Printf("\r  rows: %d  triples: %d (%.0f/sec)  rejects: %d  errors: %d   ",
					rows.Load(), written, float64(written)/time.Since(startTime).Seconds(),
					rejects.count.Load(), writer.Errors())
			case <-done:
				return
			}
		}
	}()

	splitCh := make(chan ingest.Split)
	var readWg sync.WaitGroup
	for r := 0; r < *readers; r++ {
		readWg.Add(1)
		go func() {
			defer readWg.Done()
			for s := range splitCh {
				if failed.Load() {
					continue
				}
				if err := loadSplit(parser, s, hash, writer, *batchSize, &rows, rejects, &failed); err != nil {
					fail(err)
				}
			}
		}()
	}
	for _, s := range splits {
		splitCh <- s
	}
	close(splitCh)
	readWg.Wait()
	writer.Close()
	close(done)
	if err := rejects.Close(); err != nil {
		fail(err)
	}

	elapsed := time.Since(startTime)
	written := writer.Written()
	fmt.Printf("\n\nDone. %d rows → %d triples in %s (%.0f triples/sec), %d rejected\n",
		rows.Load(), written, elapsed.Round(time.Millisecond), float64(written)/elapsed.Seconds(), rejects.count.Load())
	if *rejectPath != "" && rejects.count.Load() > 0 {
		fmt.Printf("  rejected records: %s\n", *rejectPath)
	}
	exit := 0
	if errors := writer.Errors(); errors > 0 {
		fmt.Printf("  batch errors: %d\n", errors)
		exit = 1
	}
	if firstErr != nil {
		fmt.Printf("  stopped early: %v\n", firstErr)
		exit = 1
	}
	os.Exit(exit)
}

// loadSplit parses one split, queueing its triples, hashed, for writing in
// batches. It stops early once another split has failed.
func loadSplit(parser ingest.Parser, s ingest.Split, hash func(*model.Triple) *model.Triple, writer *ingest.Writer, batchSize int,
	rows *atomic.Int64, rejects *rejectLog, failed *atomic.Bool) error {
	const rowFlush = 4096
	var n int64
	batch := make([]*model.Triple, 0, batchSize)
	err := parser.Parse(s, ingest.Handler{
		Row: func() {
			if n++; n == rowFlush {
				rows.Add(n)
				n = 0
			}
		},
		Triple: func(t *model.Triple) error {
			batch = append(batch, hash(t))
			if len(batch) == batchSize {
				if failed.Load() {
					return errStopped
				}
				writer.Add(batch)
				batch = make([]*model.Triple, 0, batchSize)
			}
			return nil
		},
		Reject: rejects.Add,
	})
	rows.Add(n)
	if len(batch) > 0 {
		writer.Add(batch)
	}
	if errors.Is(err, errStopped) {
		return nil
	}
	return err
}

// errStopped ends a split early because another one failed.
var errStopped = errors.New("stopped")

// dryRunSplit prints the first triples, hashed, and rejects of a split.
func dryRunSplit(parser ingest.Parser, s ingest.Split, hash func(*model.Triple) *model.Triple) {
	fmt.Printf("\nFirst triples of %s:\n", s)
	shown, rejected := 0, 0
	err := parser.Parse(s, ingest.Handler{
		Row: func() {},
		Triple: func(t *model.Triple) error {
			t = hash(t)
			if len(t.Props) > 0 {
				fmt.Printf("  %s -[%s]-> %s  props=%v\n", t.Subject, t.Predicate, t.Object, t.Props)
			} else {
				fmt.Printf("  %s -[%s]-> %s\n", t.Subject, t.Predicate, t.Object)
			}
			if shown++; shown == dryRunTriples {
				return errStopped
			}
			return nil
		},
		Reject: func(r ingest.Reject) error {
			fmt.Printf("  rejected %s\n", r)
			if rejected++; rejected == dryRunTriples {
				return errStopped
			}
			return nil
		},
	})
	if err != nil && !errors.Is(err, errStopped) {
		log.Fatal(err)
	}
	fmt.Println("\nDry run: nothing written")
}

// rejectLog counts rejected records, prints the first few (shortened), writes
// all of them in full to a file if one is given, and fails once there are more than max (0 = no
// limit).
type rejectLog struct {
	mu    sync.Mutex
	count atomic.Int64
	max   int64
	f     *os.File
	w     *bufio.Writer
}

func newRejectLog(path string, max int64) (*rejectLog, error) {
	l := &rejectLog{max: max}
	if path != "" {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		l.f, l.w = f, bufio.NewWriter(f)
	}
	return l, nil
}

func (l *rejectLog) Add(r ingest.Reject) error {
	n := l.count.Add(1)
	l.mu.Lock()
	if n <= shownRejects {
		fmt.Fprintf(os.Stderr, "\nrejected %s\n", r)
	}
	if l.w != nil {
		fmt.Fprintf(l.w, "%s@%d: %s: %s\n", r.Path, r.Offset, r.Reason, r.Record)
	}
	l.mu.Unlock()
	if l.max > 0 && n > l.max {
		return fmt.Errorf("%w: more than -max-errors %d", errTooManyRejects, l.max)
	}
	return nil
}

func (l *rejectLog) Close() error {
	if l.f == nil {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Close()
}
