// Command queryapi serves an HTTP/JSON query API over the triple store: key
// lookups, the roles (subject, predicate, object) a value plays, pattern
// matches, and traversals of one or more hops. It runs interactively, or as a
// systemd service on Linux (see deploy/).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"graph-sub-pred-obj/store"
	"graph-sub-pred-obj/version"
)

func main() {
	var conn store.ConnConfig
	conn.RegisterFlags(flag.CommandLine)
	namespace := flag.String("namespace", "test", "Aerospike namespace")
	listen := flag.String("listen", ":8090", "HTTP listen address")
	maxQueries := flag.Int("max-queries", 128, "Max concurrent Aerospike queries across all requests")
	threads := flag.Int("query-threads", 16, "Concurrent queries per traversal hop")
	shortQueries := flag.Bool("short-queries", false, "Run index queries as short queries (fast for small results; best when -max-degree is under ~100)")
	queryTimeout := flag.Duration("query-timeout", 10*time.Second, "Timeout of each Aerospike query")
	requestTimeout := flag.Duration("request-timeout", 30*time.Second, "Time a traversal may take; hops not started by then are skipped and the result marked timed_out")
	maxHops := flag.Int("max-hops", 6, "Largest hops a traversal may request")
	defaultHops := flag.Int("default-hops", 1, "Hops when a traversal gives none")
	maxDegree := flag.Int64("max-degree", 1000, "Default triples fetched per node and direction; nodes with more are hubs and aren't expanded")
	defaultLimit := flag.Int64("default-limit", 100, "Default limit of lookup and match results")
	maxLimit := flag.Int64("max-limit", 10000, "Largest limit or max_degree a request may set")
	defaultTriples := flag.Int("default-triples", 10000, "Default max_triples a traversal returns")
	maxTriples := flag.Int("max-triples", 100000, "Largest max_triples a traversal may set")
	showVersion := flag.Bool("version", false, "Print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("queryapi", version.String())
		return
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("queryapi", version.String())
	if *maxQueries < 1 || *threads < 1 {
		log.Fatal("-max-queries and -query-threads must be at least 1")
	}

	gs, err := conn.Connect(*namespace)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer gs.Close()
	if *shortQueries {
		gs.UseShortQueries()
	}
	gs.QueryPolicy.TotalTimeout = *queryTimeout

	fetch := &storeFetcher{gs: gs, slots: make(chan struct{}, *maxQueries)}
	api := &API{
		gs:        gs,
		fetch:     fetch,
		traverser: &Traverser{Fetch: fetch, Threads: *threads},
		namespace: *namespace,
		limits: Limits{
			MaxHops:        *maxHops,
			DefaultHops:    *defaultHops,
			MaxDegree:      *maxDegree,
			DefaultLimit:   *defaultLimit,
			MaxLimit:       *maxLimit,
			DefaultTotal:   *defaultTriples,
			MaxTriples:     *maxTriples,
			RequestTimeout: *requestTimeout,
		},
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	// SIGTERM from systemd or Ctrl-C: finish in-flight requests, then exit
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		log.Println("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), *requestTimeout+5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("connected to namespace %s; listening on %s", *namespace, *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	// ListenAndServe returns as soon as Shutdown starts; wait for the drain
	<-drained
}
