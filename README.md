# graph-sub-pred-obj

A high-performance RDF graph store in Go using Aerospike 8.2.0.0 as the key-value backend. Stores Subject-Predicate-Object triples with optional metadata properties and supports all standard RDF query patterns plus multi-hop graph traversal.

## Requirements

- Go 1.25+
- Aerospike Server 8.2.0.0+
- Python 3.12+ (for the graph viewer)

## Usage

```bash
go build -o graph-spo .
./graph-spo -host 127.0.0.1 -port 3000 -namespace test
```

| Flag | Default | Description |
|------|---------|-------------|
| `-namespace` | `test` | Aerospike namespace |

Plus the [connection flags](#connecting-to-aerospike).

## Connecting to Aerospike

`graph-spo` and the content generator share these flags for reaching a local or remote cluster:

| Flag | Default | Description |
|------|---------|-------------|
| `-host` | `127.0.0.1` | Aerospike host |
| `-port` | `3000` | Aerospike port |
| `-hosts` | | Comma-separated seed hosts `host[:port]`; overrides `-host`/`-port` |
| `-user` | | User, for clusters with security enabled |
| `-password` | `$AEROSPIKE_PASSWORD` | Password; prefer the environment variable to keep it out of shell history |
| `-tls-name` | | TLS name of the cluster nodes; enables TLS |
| `-tls-cafile` | | CA certificate file for TLS |
| `-alternate-access` | off | Connect via the nodes' `alternate-access-address`, for clusters behind NAT, in the cloud, or in Docker |
| `-conn-queue` | client default (100) | Max connections per node; set a little above the number of concurrent workers (see [sizing](#sizing-workers-and-connections)) |

```bash
AEROSPIKE_PASSWORD=... go run ./cmd/generate/ \
  -hosts 10.0.1.10:3000,10.0.1.11:3000 -user loader -alternate-access
```

## Data Model

Each triple is stored as an Aerospike record in the `triples` set:

| Bin | Type | Description |
|-----|------|-------------|
| `subject` | String | Subject URI/identifier |
| `predicate` | String | Predicate/relationship type |
| `object` | String | Object URI/literal |
| `props` | CDT Map | Optional metadata (timestamps, weights, labels, etc.) |

The primary key is a SHA-256 hash of `subject + \0 + predicate + \0 + object`, providing natural deduplication and even partition distribution.

## Query Patterns

All 7 RDF query patterns are supported, automatically selecting the optimal index strategy:

| Pattern | Example | Method |
|---------|---------|--------|
| `SPO` | Does Alice follow Bob? | Direct PK lookup (sub-ms) |
| `SP?` | Who does Bob follow? | SI on subject + filter on predicate |
| `S?O` | How is Alice connected to post:101? | SI on subject + filter on object |
| `S??` | Everything about Alice | SI on subject |
| `?PO` | Who authored post:101? | SI on object + filter on predicate |
| `?P?` | All "authored" relationships | SI on predicate |
| `??O` | Everything pointing to topic:graphs | SI on object |

Three secondary indexes are created automatically: `idx_subject`, `idx_predicate`, `idx_object`.

## Content Generator

A standalone tool that reads JSON descriptor files and generates synthetic identity graph data at scale. Driven by three configuration files:

- **`descriptors/contentTypes.json`** — defines 6 entity types (INDIVIDUAL, ACCOUNT, DISPLAY_DEVICE, ADDRESS, CREDIT_DEVICE, HOUSEHOLD), each with their own set of identifier properties and decay rates
- **`descriptors/ontology.json`** — defines relationship rules between entity types (e.g., INDIVIDUAL → HAS_ONE_OR_MORE → ACCOUNT) with cardinality semantics
- **`descriptors/cardinalities.json`** — caps how many INDIVIDUALs may share one entity (e.g., an ACCOUNT is shared by at most 4 INDIVIDUALs, an ADDRESS by at most 2)

### Running the Generator

```bash
go run ./cmd/generate/ \
  -host 127.0.0.1 \
  -port 3000 \
  -namespace test \
  -count 1000000 \
  -batch 256 \
  -workers 8
```

| Flag | Default | Description |
|------|---------|-------------|
| `-count` | `1000` | Number of INDIVIDUALs to generate |
| `-batch` | `256` | Batch write size |
| `-workers` | `8` | Concurrent generator and writer goroutines |
| `-content-types` | `descriptors/contentTypes.json` | Path to content types descriptor |
| `-ontology` | `descriptors/ontology.json` | Path to ontology descriptor |
| `-cardinalities` | `descriptors/cardinalities.json` | Path to cardinalities descriptor |
| `-share-rate` | `0.2` | Probability an INDIVIDUAL links to an existing entity instead of creating a new one |
| `-hash-type` | `no-hash` | Hash all content: `no-hash`, `sha-256`, or `sha-512` |
| `-client-index` | `0` | This generator's index (0-based) when running on multiple hosts |
| `-client-count` | `1` | Total number of generators running at the same time |
| `-rebuild-entities` | off | Rebuild the `entities` set and ID counter from existing triples, then exit |
| `-version` | off | Print the build version and exit; the version is also printed at startup |
| `-namespace` | `test` | Aerospike namespace |

Plus the [connection flags](#connecting-to-aerospike).

### Checking Which Build Is Running

When copying binaries to client hosts, confirm they were built from the expected commit:

```bash
GOOS=linux GOARCH=amd64 go build -o generate.linux ./cmd/generate
./generate.linux -version
# generate bec744a (2026-10-07T14:03:13Z) linux/amd64
```

The version is the git commit the binary was built from, read from the build information Go embeds automatically. A `+modified` suffix means the working tree had changes when it was built, including untracked files. Without running the binary, `strings generate.linux | grep vcs.revision` shows the same commit.

### What It Generates

Each INDIVIDUAL produces ~80 triples including:
- **IS_TYPE** triple linking entity to its content type
- **HAS_IDENTIFIER** triples for each property defined in contentTypes.json, with `created`, `last_seen`, and `decay` metadata
- **Relationship triples** following ontology rules (HAS_ONE_OR_MORE, LIVES_AT, IS_PART_OF, etc.)
- **Shared entities**: ~20% of INDIVIDUAL links to an ACCOUNT, ADDRESS, CREDIT_DEVICE, or DISPLAY_DEVICE reuse an existing entity (up to its cardinality cap), and ~20% of INDIVIDUALs join an existing HOUSEHOLD

### Sharing and Incremental Runs

Sharing is expressed purely as triples that reuse an existing entity's ID, so relations form naturally when the RDF content is loaded. A shared entity is linked to, but its own identifier, type, and outgoing relationship triples are not generated again.

Generators never scan the `triples` set during a load. Two small sets hold the state they need:

| Set | Record | Contents |
|-----|--------|----------|
| `meta` | `id_counter` | Highest ID reserved. Each generator atomically reserves blocks of 100,000 IDs, so any number of generators on any hosts never collide |
| `entities` | one per shareable entity | Entity ID, its (hashed) type, and how many INDIVIDUALs link to it |

At startup each generator loads its slice of the `entities` set, then links ~20% of INDIVIDUAL relations to those entities, never past their cardinality cap. New link counts are written back as atomic increments every 2 seconds and at the end. Entities that reach their cap leave the pool; each pool holds up to 100,000 entities per type. HOUSEHOLDs are shared the same way with no cap.

### Initial and Incremental Loads on Multiple Hosts

Run one generator per client host, all with the same `-client-count` and a distinct `-client-index`:

```bash
# host A                                   # host B
go run ./cmd/generate/ -hosts ... \        go run ./cmd/generate/ -hosts ... \
  -count 1000000 -workers 32 \               -count 1000000 -workers 32 \
  -client-index 0 -client-count 2             -client-index 1 -client-count 2
```

The same commands serve the initial load (into an empty set) and each periodic incremental load. The 4,096 partitions of the `entities` set are split evenly between clients, so each client shares a disjoint set of existing entities and the caps hold exactly across hosts without a database round trip per link. Run more than one generator per host the same way, giving each its own index.

Caps are only guaranteed between generators started with the same `-client-count`; don't overlap two loads that split the partitions differently. ID uniqueness holds regardless.

#### Data Loaded Before the `entities` Set

A generator refuses to write into a `triples` set that has data but no ID counter. Run a rebuild once, from one host, with no loads running:

```bash
go run ./cmd/generate/ -rebuild-entities              # plus -hash-type for hashed data
```

It scans all triples, writes the `entities` set with exact link counts, and raises the ID counter above the highest existing ID. It only recognizes content of the given `-hash-type`; run it once per hash type present.

#### cardinalities.json

```json
{
	"cardinalities":
		[
			{"TYPE": "ACCOUNT",        "LINKED_TO": "INDIVIDUAL", "MAX": 4},
			{"TYPE": "ADDRESS",        "LINKED_TO": "INDIVIDUAL", "MAX": 2},
			{"TYPE": "CREDIT_DEVICE",  "LINKED_TO": "INDIVIDUAL", "MAX": 4},
			{"TYPE": "DISPLAY_DEVICE", "LINKED_TO": "INDIVIDUAL", "MAX": 6}
		]
}
```

Types without an entry are never shared. Only `LINKED_TO: "INDIVIDUAL"` is supported; other entries are ignored with a warning.

### Content Hashing

`-hash-type sha-256` or `-hash-type sha-512` hashes the identifier data (hex-encoded) before it is written: each identifier node (the object of `HAS_IDENTIFIER`) and its `value` property. Entity IDs, predicates, type names, and the `identifier_type` property stay readable, and numeric properties such as timestamps and decay are left as-is.

```
INDIVIDUAL:121591 -[HAS_IDENTIFIER]-> cbe0eaed9c0905ca119c…   identifier_type=MCID_E  value=e2a7fe2f1bcadf85c68e…
INDIVIDUAL:121591 -[LIVES_AT]-> ADDRESS:58349
```

Hashing is deterministic, so identical identifier data still produces the same node and relations form naturally. Since entity IDs and types are readable, sharing, ID resumption, and `-rebuild-entities` work the same with or without hashing, and hashed and unhashed runs share entities in the same graph. Data generated by earlier versions, which also hashed entity IDs or predicates, won't link with newer runs.

In the viewer, hashed values are shown as their first 20 characters plus `…` in node labels, search results, the info panel, and edge properties; the info panel also shows the selected node's full ID. Hashed identifier nodes have no type prefix, so they appear in the default color.

### Performance

Benchmarked at ~300K triples/sec with 8 workers on a single Aerospike node. Three generators running in parallel on one laptop against a single local node sustained ~440K triples/sec combined. Throughput scales with client hosts and cluster nodes; raise `-workers` (and `-conn-queue` a little above it) for remote clusters, where each batch spends longer on the network.

## Query Load Generator

`cmd/queryload` measures query throughput and latency using keys sampled from the data already loaded. It is read-only, so it can run against production-sized data and alongside generator loads to test combined query and insert traffic.

```bash
go run ./cmd/queryload/ -hosts ... -duration 60s -workers 16
```

It samples `-sample` triples, then runs a weighted mix of query patterns, printing queries/sec each second and a per-pattern summary:

```
query         ops        qps   errors  rows/op    p50 ms    p95 ms    p99 ms    max ms
spo          4286        427        0      1.0      1.64     26.68     51.06    133.86
s            2871        286        0     10.8     44.59    104.45    139.13    228.85
...
total       14398       1434        0      9.1     33.01    140.60    197.73    364.86
```

| Query | Pattern |
|-------|---------|
| `spo` | Primary key get of a known triple |
| `s` | `S??`: all triples of a subject |
| `o` | `??O`: all triples pointing to an object |
| `sp` | `SP?`: subject + predicate |
| `po` | `?PO`: predicate + object |
| `hop2` | 2-hop outbound traversal; second hop queried concurrently (counted as one op) |

| Flag | Default | Description |
|------|---------|-------------|
| `-duration` | `30s` | How long to run |
| `-workers` | `64` | Concurrent query workers |
| `-qps` | `0` | Target queries/sec for this process; `0` runs as fast as possible. Latency is measured from each query's scheduled start, so queueing behind an overloaded server shows up in the percentiles |
| `-mix` | `spo=30,s=20,o=10,sp=15,po=15,hop2=10` | Query mix as `name=weight` |
| `-sample` | `10000` | Triples sampled as query keys |
| `-max-results` | `100` | Max triples returned per secondary index query |
| `-short-queries` | on | Run secondary index queries as short queries, which the server optimizes for small results at high rates. Long queries count against the server's `query-threads-limit` (default 128) and are rejected with `FAIL_FORBIDDEN: Operation not allowed at this time` beyond it. Turn off (`-short-queries=false`) when `-max-results` is well above 100 |
| `-hop-fanout` | `10` | Max second-hop nodes queried per `hop2` |
| `-client-index`, `-client-count` | `0`, `1` | Split sampling between processes on multiple hosts, as with the generator |
| `-json` | off | Print the summary as JSON, for collecting results from many hosts |
| `-version` | off | Print the build version and exit |
| `-namespace` | `test` | Aerospike namespace |

Plus the [connection flags](#connecting-to-aerospike). To load-test a cluster, run one `queryload` per client host (each with its own `-client-index`) and add up the totals.

### Sizing Workers and Connections

Each worker has one query in flight, so throughput is at most `workers / round-trip time`. Against a remote cluster, raise `-workers` to cover the round trip, and run from a host in the same region as the cluster to measure the cluster rather than the network.

Set `-conn-queue` a little above `-workers` (e.g. 300 for 256 workers), leaving some room for `hop2` fan-out:

- **Too small:** with the client default of 100, more than 100 workers fail with `NO_AVAILABLE_CONNECTIONS_TO_NODE` and time out.
- **Too large:** a pool far above the worker count lets the client open thousands of connections across the cluster's nodes, which can overwhelm the network path. Against a 2-node cluster, 256 workers with `-conn-queue 2000` dropped from 1,607 to 419 queries/sec with 110 timeouts; `-conn-queue 300` ran clean.

### Examples

The first four commands are the runs used to measure a 2-node cluster in AWS from a Mac about 51 ms away (ping), loaded with 10,000 individuals (684,968 triples, sha-512); results are in the table below. The last two show other options. Replace `<cluster-host>` with a node address; `-alternate-access` is needed when nodes advertise private addresses.

```bash
# Default mix, default 64 workers
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 64 -conn-queue 256 -duration 30s

# Default mix, 256 workers
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 256 -conn-queue 300 -duration 30s

# Primary key gets only
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 256 -conn-queue 300 -duration 15s -mix spo=1

# Secondary index queries only
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 256 -conn-queue 300 -duration 15s -mix s=1

# Fixed rate (open loop), latency measured from each query's scheduled start
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 64 -conn-queue 256 -duration 30s -qps 500

# JSON summary, for collecting results from several hosts
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 256 -conn-queue 300 -json
```

| Run | Queries/sec | p50 ms | p99 ms | Errors |
|-----|-------------|--------|--------|--------|
| Default mix, 64 workers | 841 | 57 | 215 | 0 |
| Default mix, 256 workers | 1,607 | 140 | 463 | 0 |
| Default mix, 256 workers, `-conn-queue 2000` | 419 | 130 | 3,968 | 110 |
| Primary key gets, 256 workers | 3,984 | 54 | 149 | 0 |
| Secondary index queries, 256 workers | 2,102 | 110 | 243 | 0 |

Primary key gets stayed at the network round trip (~53 ms), so they were limited by distance, not the cluster. Secondary index queries topped out around 2,100/sec on this 2-node cluster, about 1,700/sec on a 1-node cluster; extra workers beyond that only queue.

To test queries during inserts, run a generator load and `queryload` at the same time, from the same or different hosts:

```bash
go run ./cmd/generate/ -host <cluster-host> -alternate-access -count 10000 -hash-type sha-512 -workers 32 -conn-queue 128 &
go run ./cmd/queryload/ -host <cluster-host> -alternate-access -workers 64 -conn-queue 256 -duration 60s
```

### What to Expect

Measured on one laptop against a single Aerospike node in Docker, with the server and both clients sharing the same CPUs:

| Workload | Result |
|----------|--------|
| Primary key gets (`spo`) | ~52K/sec from one client, sub-millisecond |
| Secondary index queries (`s`, `o`, `sp`, `po`) | ~1.2–2.2K/sec, server-CPU bound (~2.4 ms of server CPU each) regardless of client concurrency |
| Inserts alone | ~270–335K triples/sec |
| Inserts + secondary index queries paced at 500/sec | 225K triples/sec inserted |
| Inserts + secondary index queries at 4 workers | 179K triples/sec inserted, 1.2K queries/sec |
| Inserts + secondary index queries at 64 workers | 38K triples/sec inserted, 1.3K queries/sec |

Secondary index queries cost far more server CPU than primary key operations, since each consults the index in every partition. Pushing more concurrency at them past the server's capacity adds no query throughput but sharply slows inserts. For high combined rates: pace or limit secondary index query concurrency (`-qps`, `-workers`), add cluster nodes to scale secondary index throughput, and prefer primary key lookups on hot paths.

## Interactive Graph Viewer

A web-based property graph viewer built with Python, Dash, and Cytoscape. Connects directly to Aerospike and provides interactive visualization of RDF relationships.

### Setup

```bash
pip install -r viewer/requirements.txt
python viewer/app.py --host 127.0.0.1 --port 3000 --namespace test
```

Then open http://127.0.0.1:8050 in your browser.

For a remote cluster:

```bash
AEROSPIKE_PASSWORD=... python viewer/app.py \
  --hosts 10.0.1.10:3000,10.0.1.11:3000 --user viewer --alternate-access
```

### Features

- **Start Node** search: type 2+ characters to find a node in a sample of the graph (up to 50 matches are shown), or type a full node ID to find any node. Hashed IDs are shown shortened to 20 characters plus `…`
- **Max Hops** slider (1-6) to control traversal depth
- **Ontology relationships only** (on by default): navigate entity to entity through the relationships in `descriptors/ontology.json`, regardless of hash type (see below)
- **Predicate Filter** to show only specific relationship types
- **Direction** control for outbound, inbound, or bidirectional traversal
- Changing Max Hops, Predicate Filter, or Direction rebuilds the current graph immediately
- **Multiple layouts**: cola (force-directed), dagre (hierarchical), breadthfirst, circle, concentric, grid
- **Click** a node or edge to inspect all properties in the info panel (up to 50 edges listed per direction; counts above 1,000 show as `1000+`)
- **Click** an unexpanded node to expand its relationships inline
- **Graph stats** panel showing node/edge counts, predicates, type breakdown, and hubs
- **Color-coded nodes** by type prefix (blue=user, green=post, orange=topic)

### Ontology Navigation

With **Ontology relationships only** checked (the default), the viewer navigates through the relationships defined in `descriptors/ontology.json` (`HAS_ONE_OR_MORE`, `LIVES_AT`, `IS_PART_OF`, `HAS`, `HAS_AN`, `CAN_HAVE_SEVERAL`) between entities such as `INDIVIDUAL:306897` and `ACCOUNT:306991`. Entity IDs and ontology predicates are never hashed, so this works the same with `no-hash`, `sha-256`, or `sha-512` data.

- Traversal and tap-to-expand follow only ontology relationships; identifier nodes and `IS_TYPE` type hubs are left out of the graph
- The Predicate Filter offers only ontology relationships; selecting none follows all of them
- Start Node search lists only entities of the ontology's types
- Clicking an entity lists its identifiers in the info panel as `identifier_type = value` (hashed values shortened), followed by its relationships

Uncheck it to browse every triple, including identifier nodes and `IS_TYPE`. Use `--ontology` to point the viewer at a different ontology descriptor.

### Hub Limit

Nodes with more than 10 edges are treated as **hubs** and are not expanded, so a shared node doesn't pull thousands of neighbors into the graph. For example, every entity links to its bare type node (`INDIVIDUAL`, `ACCOUNT`, ...) through `IS_TYPE`.

- Hubs still appear in the graph, labeled with their edge count (e.g. `ACCOUNT:15 (12 edges)`, or `INDIVIDUAL (1000+ edges)` above 1,000) and drawn with a dashed orange border
- Graph Stats lists the hubs that were not expanded, largest first
- Clicking a hub does not expand it
- The start node is always expanded, whatever its degree
- Degree is counted after the predicate filter and direction are applied, so a node can be a hub under one filter and expandable under another

Change the threshold with `MAX_EXPAND_DEGREE` at the top of `viewer/app.py`.

### Large and Remote Graphs

The viewer is built to stay responsive against millions of triples and remote clusters, where every round trip and every row crosses the network:

- **No full scans.** On page load (and Refresh Data) the viewer samples up to `--sample-size` triples (default 100,000) for the node search list and predicate filter, instead of scanning the whole set. Nodes outside the sample are found by typing their full ID.
- **Capped queries.** Each node fetches at most 1,000 edges per direction (`HUB_COUNT_CAP`), so a type node with millions of edges costs the same as one with 1,000. The predicate filter is applied on the server, so filtered-out edges never cross the network.
- **Parallel traversal.** Each hop's nodes are queried concurrently (`QUERY_THREADS`, default 16), so a hop costs about one round trip instead of one per node.

Through a link with 80 ms round trips, a 3-hop explore over 2 million triples takes about 1 second, and page load about 0.3 seconds.

### Viewer CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Aerospike host |
| `--port` | `3000` | Aerospike port |
| `--hosts` | | Comma-separated seed hosts `host[:port]`; overrides `--host`/`--port` |
| `--namespace` | `test` | Aerospike namespace |
| `--user` | | User, for clusters with security enabled |
| `--password` | `$AEROSPIKE_PASSWORD` | Password |
| `--tls-name` | | TLS name of the cluster nodes; enables TLS |
| `--tls-cafile` | | CA certificate file for TLS |
| `--alternate-access` | off | Connect via the nodes' `alternate-access-address` |
| `--listen-port` | `8050` | Port the viewer's web server listens on |
| `--sample-size` | `100000` | Triples sampled for node search and the predicate filter |
| `--ontology` | `descriptors/ontology.json` | Ontology descriptor whose relationships drive navigation |
| `--debug` | off | Enable Dash debug mode with hot reload |

## Package Structure

```
model/triple.go      Triple struct, key hashing, type helpers
store/client.go      Connection flags (seeds, auth, TLS, alternate access) and policies
store/index.go       Secondary index creation
store/writer.go      PutTriple, BatchPutTriples, DeleteTriple, UpdateTripleProperty
store/reader.go      GetTriple, QueryBySubject/Predicate/Object, QuerySP/PO/SO, ScanAll, ScanEach
store/meta.go        Atomic ID counter reservation (meta set)
store/entities.go    Shareable entity link counts (entities set), partitioned scans
graph/query.go       PatternQuery dispatcher for all 7 RDF patterns
graph/traverse.go    BFS outbound/inbound traversal with depth limits
ingest/loader.go     Batch and channel-based streaming ingestion
cmd/generate/        Content generator: main.go (flags, pipeline), graph.go (triples),
                     pool.go (sharing, rebuild), ids.go (hashing, ID blocks)
cmd/queryload/       Query load generator: throughput and latency by query pattern
version/             Build version (git commit) for -version
viewer/app.py        Interactive web-based graph viewer (Dash + Cytoscape)
descriptors/         Content type, ontology, and cardinality JSON descriptors
```

## Example

```go
gs, _ := store.NewGraphStore("127.0.0.1", 3000, "test")
defer gs.Close()
gs.CreateIndexes()

// Write a triple with properties
gs.PutTriple(&model.Triple{
    Subject:   "user:alice",
    Predicate: "follows",
    Object:    "user:bob",
    Props:     map[string]interface{}{"since": time.Now().UnixMilli()},
})

// Query: who does alice follow?
results, _ := gs.QuerySP("user:alice", "follows")

// Traverse: BFS from alice, 3 hops deep, follows-only
paths, _ := graph.TraverseOutbound(gs, "user:alice", 3, "follows")

// Pattern query: find how alice connects to post:101
pq := &graph.PatternQuery{Subject: graph.S("user:alice"), Object: graph.O("post:101")}
results, _ = pq.Execute(gs)
```

## Ingestion

Batch loader for bulk writes (default 256 records/batch):

```go
loader := ingest.NewLoader(gs, 256)
loader.LoadTriples(triples)
```

Channel-based streaming for continuous ingestion:

```go
ch := make(chan *model.Triple)
go loader.LoadFromChannel(ch)
ch <- &model.Triple{Subject: "a", Predicate: "b", Object: "c"}
close(ch)
```

## Performance

- **SPO lookups**: Sub-millisecond via direct primary key get
- **Batch writes**: Single network round-trip for up to 256 records
- **Property updates**: Atomic CDT map operations (no read-modify-write)
- **Server-side filtering**: Compound queries filter on the server, not the client
- **Concurrent traversal**: Parallel SI queries per BFS level with configurable concurrency
