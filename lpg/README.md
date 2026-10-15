# The Vertex Model (LPG)

This package stores the identity graph as a **labeled property graph**: one Aerospike record per vertex, holding the vertex's type, its identifiers and its edges. It sits beside the original **triple model**, one record per subject–predicate–object triple, and is written from the same generator and loader with `-model lpg`. This document explains why we added it, how it works, and what it costs.

Command-line flags are documented in the [main README](../README.md#vertex-model-lpg); this page covers the design.

- [Why a Second Model](#why-a-second-model)
- [How Triples Become Vertices](#how-triples-become-vertices)
- [Record Layout](#record-layout)
- [Hubs: Vertices With Many Edges](#hubs-vertices-with-many-edges)
- [Finding a Vertex by Identifier](#finding-a-vertex-by-identifier)
- [Reads](#reads)
- [Writes](#writes)
- [Gremlin](#gremlin)
- [Measured Results](#measured-results)
- [Trade-offs and Limits](#trade-offs-and-limits)
- [Compared With Aerospike Graph (AGS)](#compared-with-aerospike-graph-ags)
- [Using It](#using-it)
- [Code Map](#code-map)

## Why a Second Model

The triple model is simple and loads fast: every triple is one small record, keyed by a hash of subject, predicate and object, with secondary indexes on all three. The problem is what that costs at the target size of **250 billion triples**.

**Every record costs index memory, however small it is.** Aerospike keeps 64 bytes of primary index per record per replica, in RAM by default, and the triple model adds three secondary-index entries per record. Measured per triple, that is 70 B of primary index and 57.6 B of secondary index (19.2 B per entry, B-tree overhead included). At 250B triples and replication factor 2:

| | Triple model | Vertex model |
|---|---:|---:|
| Index memory | ~64 TB | ~4.5 TB |
| Data on device (before defrag headroom) | ~136 TB | ~62 TB |

**Most triples are not relationships.** With the default descriptors an INDIVIDUAL produces about 84 triples, and 69.5 of them (83%) are `HAS_IDENTIFIER` or `IS_TYPE`: properties and labels of a vertex. The triple model gives each one its own record, index entry and three secondary-index entries. In a property graph they are entries in the vertex's own record.

**Traversals were index queries that reach every node.** Each hop in the triple model is a secondary-index query (`S??`, `??O`), and an Aerospike secondary-index query runs on every node of the cluster. The repo's `queryload` measured a key lookup at 1.6 ms p50 and a subject query at 44 ms. In the vertex model a hop is a batch read of keys, each sent straight to the node that owns it.

**The predicate index barely selects.** There are about 10 distinct predicates, so each value of `idx_predicate` points at roughly 25B records. It costs about 7 TB of memory at 250B triples and RF 2 for queries that are effectively scans.

The vertex model keeps the triple model's strengths where it can: the same descriptors and generator, the same hashing, idempotent writes, partitioned multi-host loads. It then fixes the record count.

## How Triples Become Vertices

The generator still produces triples for each INDIVIDUAL; `FromTriples` (in `model.go`) turns them into one mutation per vertex touched:

| Triple | Becomes |
|---|---|
| `S IS_TYPE T` | The `type` of vertex S, with its `created` time |
| `S HAS_IDENTIFIER x` | An entry in S's `identifiers` map, `{identifier type: {value: [created, last_seen, decay]}}`, plus an entry in the identifier lookup set. The identifier node `x` is not stored |
| `S P "literal"` (N-Triples) | A vertex property in the same map, keyed by the predicate |
| `S P O` (anything else) | An edge: outgoing on S, incoming on O, with its properties |

The same INDIVIDUAL that was 84 records in the triple model is about 14 vertex writes here (the INDIVIDUAL, the entities it creates, and the shared entities it links to). `HOUSEHOLD -[CAN_HAVE_SEVERAL]-> INDIVIDUAL` needs nothing of its own: it is the HOUSEHOLD's incoming `IS_PART_OF` edges.

Because every edge is stored on both of its vertices, a traversal can follow it in either direction with one read, with no reverse index.

## Record Layout

```
vertices                  key: entity ID, e.g. "INDIVIDUAL:100514" (stored, so scans can return it)
  type            "INDIVIDUAL"
  created         1792079156844
  identifiers     { "EMAIL_E":  { "EMAIL_E_100524_4c9dc9": [created, last_seen, 0.1] },
                    "PHONE_E":  { "PHONE_E_100531_8b1e0a": [created, last_seen, 0.1] }, ... }
  out_edges       { "LIVES_AT": { "ADDRESS:100612": [created, last_seen] },
                    "IS_PART_OF": { "HOUSEHOLD:HUB-0": [created, last_seen] }, ... }
  in_edges        { ... }                 same shape, for edges pointing at this vertex
  indiv_links     2                       INDIVIDUALs linked to this vertex (sharing caps)
  edge_count      12                      edge entries in this record
  hub_out_level   absent unless a hub     newest outgoing edge bucket level
  hub_in_level    absent unless a hub     newest incoming edge bucket level

edge_buckets              key: "<vertex ID>|<out or in>|<level>|<bucket>"   (hubs only)
  vertex_id       "HOUSEHOLD:HUB-0"
  edges           { "IS_PART_OF": { "INDIVIDUAL:1": [created, last_seen], ... } }
  edge_count      256

identifier_index          key: bucket number (integer)
  vertices_by_tag { <64-bit tag>: { "INDIVIDUAL:100514": created } }

meta / lpg_config         id_buckets: the identifier bucket count, fixed by the first load
```

All maps are key-ordered. An edge is stored as `[created, last_seen]`, or `[created, last_seen, {name: value}]` when it carries other properties (from `load -props_columns`). `created` comes first on purpose: a map value range over these lists selects edges by creation time on the server.

Set and bin names are descriptive. Aerospike stores them in every record, so this costs about 47 B per vertex over one- and two-letter names (measured: 678 B against 632 B per vertex), which is roughly 3.2 TB more on device for 34B vertices at RF 2. It costs nothing in index memory.

There are no secondary indexes unless you ask for the optional type index (`idx_vertices_type`, from `generate -label-index`).

## Hubs: Vertices With Many Edges

Most vertices have a handful of edges (about 2.5 on average with the default descriptors), and those stay inline in the vertex record. Some vertices collect very many: a shared address, a household, or in real data a placeholder identifier. A record can't grow without limit: Aerospike rewrites the whole record on every update, and records are capped at 8 MB. Those vertices become **hubs**.

**Promotion.** A vertex takes edges inline until it holds 256 entries (`-max-inline-edges`). Inline writes carry a filter expression on `edge_count`; the write that would pass the limit is filtered out, and the writer promotes the vertex by recording `hub_in_level` or `hub_out_level` = 0. From then on, new edges in that direction go to edge buckets; the edges already inline stay where they are. A vertex known to be a hub can be promoted before it has any edges (`Store.PromoteHub`, which `generate -hubs` does for its hubs), so its first edge goes straight to a bucket.

**Levels of hashed buckets.** A hub's edges in one direction live in levels of buckets. Level *k* has 16·2^*k* buckets, and an edge goes to bucket `FNV-1a(neighbor ID) mod 16·2^k`.

```
level 0   16 buckets  ■■■■■■■■■■■■■■■■                                   full
level 1   32 buckets  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■                   full
level 2   64 buckets  ■■■■■■■■■□□■■■□■■■□□■■■■□■■■■□■■■□□■■■■■■□■■■■...  newest: new edges go here
```

Bucket writes carry a filter on the bucket's `edge_count`. When a bucket of the newest level reaches 256 entries, the writer moves the hub up a level with a compare-and-set on its level bin, so racing writers agree, and retries there. **Nothing is ever rehashed or moved**, so a hub keeps taking writes while it grows. 1M edges take 9 levels (8,176 buckets); 10M take 12 (65,520).

This gives every operation on one edge or one page a cost that depends on the number of levels, not edges:

| Operation | What it reads |
|---|---|
| Read the hub vertex | 1 record; its bucketed edges aren't in it |
| Does edge X exist? | The vertex, plus the one bucket per level that X hashes to (about 10 keys at 1M edges) |
| The first *n* edges | About *n*/256 buckets: reads start with the buckets a limit needs and double |
| Add an edge | 1 bucket at the newest level; concurrent writers spread over all its buckets |
| A traversal through the hub | At most `max-degree`+1 of its edges, enough to find it is a hub, then it isn't expanded |
| An exact degree, or all edges | Every bucket: the one read that grows with the hub |

Because writes spread over many buckets, concurrent writers to a hub don't queue on one record. The exception is the hub's own `indiv_links` counter, which is a single bin.

## Finding a Vertex by Identifier

The triple model found the vertex that holds an identifier through the object index. Here, `identifier_index` does it with records instead of a secondary index:

1. SHA-256 of the identifier's type and value gives a bucket (the first 8 bytes mod `id_buckets`) and a 64-bit tag (the next 8 bytes).
2. One read of that bucket returns the vertices under that tag.
3. A batch read of those vertices confirms each really holds the identifier, so tag collisions can't produce wrong answers.

Buckets are sized at about 100 identifiers each at the target size. The default 2^24 buckets suits about 30M INDIVIDUALs; 2^31 suits the ~173B identifiers of 250B triples. The count is fixed by the first load into a namespace, since lookups would otherwise read the wrong buckets.

This costs about 0.6 B of index memory per identifier, against 14–19 B for a secondary-index entry. The price is paid on writes; see [Trade-offs](#trade-offs-and-limits).

## Reads

All reads are key lookups or batch reads (`reader.go`):

- **`GetVertices`** batch-reads vertex records: type, identifiers, inline edges and hub levels.
- **`HubEdges`** reads a hub's bucketed edges in growing chunks, stopping at a limit. It builds keys per chunk, since each key costs a digest.
- **`HasEdge` / `HasInEdge`** look one edge up from either side.
- **`Degree`** counts all edges, reading only each bucket's `edge_count`.
- **`Traverse`** walks hops as one batch read of the frontier per hop. Hubs over `max-degree` are reported and not expanded.
- **`LookupIdentifier`** and **`VerticesByType`** find vertices by identifier and by type.

The seven RDF query patterns of the triple model all map onto these, through Gremlin or the Go API:

| Pattern | In the vertex model |
|---|---|
| `SPO` | `g.E('S\|P\|O')`: one read of S, or one bucket per level if S is a hub |
| `SP?` | `g.V(S).outE(P)`: one read of S |
| `S?O` | `g.V(S).outE().where(inV().hasId(O))`: one read of S |
| `S??` | `g.V(S).union(valueMap(true), outE())`: one read of S; type and identifiers are in it |
| `?PO` | `g.V(O).inE(P)`: one read of O. For `IS_TYPE`, `g.V().hasLabel(T)` (type index or filtered scan); for `HAS_IDENTIFIER`, `g.V().has(type, value)` (identifier index) |
| `?P?` | `g.E().hasLabel(P)`: a scan, as in practice it was before |
| `??O` | `g.V(O).inE()`: one read of O |

## Writes

`Store.Apply` writes a batch of mutations (`store.go`):

1. Mutations of the same vertex are merged, so a batch never writes one key twice.
2. Each vertex becomes one batch write:
   - **Ordinary vertex:** its map entries are added with server-side CDT operations, plus a filter that refuses the write if the vertex is full or a hub.
   - **Known hub:** its edges go straight to edge-bucket writes in the same batch.
3. Each identifier bucket touched gets one write in the same batch.
4. The few writes that come back filtered out (a vertex just filled up, or a bucket did) are retried individually through promotion or the next level.

Writes are idempotent: map entries are keyed, so re-running a load updates rather than duplicates. Counters (`indiv_links`, `edge_count`) are additive and atomic, so concurrent generators never lose an update.

Each edge is written twice, once on each vertex. The two writes are not atomic with each other; a failure between them leaves the edge on one side. Making them atomic would take multi-record transactions, which need a strong-consistency namespace.

**Sharing** (the generator's `SHARE_MAX` caps) works without the triple model's `entities` set. Each vertex counts its INDIVIDUAL links in `indiv_links`, in the same write that adds the edge. At startup each generator scans its slice of partitions for shareable vertices under their cap, filtered on the server and stopped once the pool is full.

## Gremlin

`gremlin/` is a read-only [TinkerPop](https://tinkerpop.apache.org/) provider over these same records, run in Gremlin Server, so Gremlin clients and G.V() (gdotv) query this data directly with no other graph service in between. It uses the same keys and hashes as the Go code; golden tests on both sides keep them in step.

Its traversal strategies read only what a query needs:

- **Lookups instead of scans.** `g.V().has(type, value)` becomes an identifier lookup, and `hasLabel` uses the type index when it exists.
- **Edge filters on the server.** `inE(P).has('created', gt(t))` returns only matching hub edges, selected inside each bucket; with a lower bound, the newest levels are read first.
- **Counting without reading edges.** `outE().count()` and similar are counted on the server.
- **Reading less of each record.** Vertices are read without their edge bins, or as keys alone for counts, when that is all the query needs.
- **`~supernode`** marks hubs, as in AGS.

See the [main README](../README.md#gremlin-and-gdotv) for running it and for example queries.

## Measured Results

All measured on one laptop against a single-node Aerospike in Docker (in-memory namespace, RF 1), with the default descriptors and no hashing. Absolute latencies on a real cluster will differ; what matters is the shape.

**Storage, per triple of data** (200,000 INDIVIDUALs as `lpg`, 177,000 as `triples`):

| | Triple model | Vertex model |
|---|---:|---:|
| Records | 1.10 | 0.14 |
| Primary index | 70 B | 8.9 B |
| Secondary indexes | 57.6 B | none |
| Data | 272 B | 123 B |

Vertices average 678 B. With `-hash-type sha-512 -hash-parts all` they grow to about 2.5 KB, since every ID, type and identifier becomes 128 hex characters.

**One hub growing from 1K to 10M edges** (`lpgbench`; p50 ms, 2,000 operations each at concurrency 16):

| Operation | 1K | 10K | 100K | 1M | 10M |
|---|---:|---:|---:|---:|---:|
| Add an edge | 0.71 | 0.85 | 0.69 | 0.70 | 0.76 |
| Has edge, on the hub | 1.22 | 1.40 | 1.15 | 1.25 | 1.65 |
| Has edge, on the neighbor | 0.51 | 0.60 | 0.54 | 0.50 | 0.56 |
| Read the hub vertex | 0.62 | 0.53 | 0.53 | 0.55 | 0.60 |
| First 100 hub edges | 1.67 | 1.57 | 1.58 | 1.58 | 1.38 |
| Read a neighbor | 0.63 | 0.55 | 0.54 | 0.49 | 0.60 |
| 2-hop traversal through the hub | 5.96 | 3.77 | 3.94 | 4.58 | 3.07 |
| Exact degree (reads every bucket) | 2.06 | 1.79 | 3.10 | 41.6 | 358 |

Every operation on one edge or one page stays flat across four orders of magnitude; only the exact degree grows, as designed. Growing the hub to 10M edges took 38 s.

**Through Gremlin Server** (12.3M vertices loaded, warm, round trip included):

| Query | Time |
|---|---:|
| A vertex by ID, an identifier lookup, 10 neighbors of the 10M-edge hub | 1.4–4 ms |
| The 10 most recent of the 10M hub's edges (`has('created', gt(t)).limit(10)`) | 8 ms |
| Count the 10M hub's edges (`inE(P).count()`) | 0.5 s |
| Count all 159,958 HOUSEHOLDs (`hasLabel(...).count()`, with the type index) | 87 ms |
| Count all 12.3M vertices (`g.V().count()`, keys only) | 3.4 s |

## Trade-offs and Limits

**Write amplification on identifier buckets.** Every new identifier rewrites its whole bucket: about 8 KB at 177 entries, against about 260 B for a triple record. With about 58 identifiers per INDIVIDUAL, this is where the model pays for its small index. On a 4 GiB file-backed test namespace, defrag couldn't keep up with these rewrites. Options:

- More buckets: smaller rewrites, more records.
- `-id-index=false` during bulk loads that don't need lookup by identifier.
- A different home for identifier lookup.

**Exact degree grows with the hub.** It reads every bucket's count: 358 ms at 10M edges. The hub's `indiv_links` gives its INDIVIDUAL links in one read. A split counter, sharded across a few records, would make the exact degree constant too.

**Counts without a predicate can over-count.** They add each bucket's `edge_count`, so an edge written to the same bucket twice counts twice. An edge re-added after its hub moved up a level is stored again in the new level, so all-edge reads can see it twice too (the newest copy last). Counts that name a predicate are exact.

**No multigraph.** An edge is identified by (out vertex, predicate, in vertex), so there is at most one edge per predicate between two vertices. That is exact for RDF-style data, which is a set of triples anyway, but not for event-style edges.

**Edge properties are stored twice**, once on each vertex, and the two writes are not atomic.

**Edge filter pushdown covers `created` only**, the one property in a fixed position in the stored list. Other edge properties are filtered in the client.

**Labels need the optional index** (`-label-index`, about 14 B of index memory per vertex: roughly 1 TB at 34B vertices and RF 2) or a filtered scan.

**The Gremlin provider is read-only and partial.** There are no writes and no OLAP. Its optimizations cover the patterns above; other traversals work, but read whole records.

**What still reads only the triple model:** the viewer, `queryapi` and `queryload`.

## Compared With Aerospike Graph (AGS)

The design follows the same vertex-centric pattern AGS uses, which is standard practice for graphs on Aerospike: one record per vertex, properties on the vertex, regular vertices' adjacency inline, supernodes spilled into multiple records read lazily, no edge secondary indexes, and neighbors read in batches.

The deliberate differences:

| | AGS | Vertex model |
|---|---|---|
| Inline edge limit | Set by record size (~6,500 edges at 1 MiB) | 256, to keep busy records small, since every update rewrites the record |
| Supernode edges | Multi-record edge lists (2.x: secondary-index queries over an EDGES set) | Hash-addressed bucket levels: one-edge lookups and pages cost the same at any size |
| Edge records | 2.x: separate EDGES records packed ~10 per record; 3.x: unpublished | None for ordinary vertices; each edge lives on both vertices |
| Property lookup | Optional secondary indexes on vertex properties and labels | Identifier lookup through bucket records (cheaper in RAM, costlier to write); optional type index |
| Query engine | Full Gremlin, read and write, with OLAP | Read-only Gremlin with the strategies above |

From AGS we adopted the supernode designation (`PromoteHub`, `~supernode`), supernode warnings, edge-property filter pushdown, local counting, edge-bin omission and counting without bin data, an optional label index, and a general edge-property map.

## Using It

```bash
# Generate (add -hubs 3 for supernodes, -label-index for the type index)
go run ./cmd/generate/ -model lpg -count 1000000 -workers 16

# Load files: types, identifiers and literals become vertex properties, other triples edges
go run ./cmd/load/ -model lpg -load_type csv -input edges.csv \
  -subject_column cust_id -predicate_column rel_type -object_column acct_id -props_columns since,score

# Query
go run ./cmd/lpgquery/ -vertex INDIVIDUAL:100514 -hops 2
go run ./cmd/lpgquery/ -id-type EMAIL_E -id-value EMAIL_E_100524_4c9dc9
go run ./cmd/lpgquery/ -vertex HOUSEHOLD:HUB-0 -edges 5 -degree

# Grow a hub and measure latency at each size
go run ./cmd/lpgbench/ -steps 1000,10000,100000,1000000,10000000 -run demo

# Serve Gremlin on ws://localhost:8182/gremlin (Java 17)
JAVA_HOME=/path/to/jdk-17 gremlin/run.sh
```

The same namespace can hold both models (they use different sets), but they don't share data: load each model separately.

The integration tests run against a live cluster:

```bash
LPG_TEST_HOST=127.0.0.1:3000 go test ./lpg/
(cd gremlin && LPG_TEST_HOST=127.0.0.1:3000 mvn test)
```

## Code Map

| File | What it does |
|---|---|
| `model.go` | Sets, bins, the `Mutation` and `Edge` types, and `FromTriples` |
| `store.go` | `Store`, writes (`Apply`), inline limit and hub promotion, bucket levels, identifier buckets, `PromoteHub`, the type index |
| `reader.go` | `GetVertices`, `HubEdges`, `HasEdge`, `Degree`, `LookupIdentifier`, `Traverse`, `VerticesByType`, `ScanShareable` |
| `model_test.go` | Conversion, merging, bucket arithmetic, and golden hashes shared with the Java provider |
| `store_test.go` | Integration tests against a live cluster: round trips, concurrent hub growth, edge lookups, promotion, sharing scans |
| `../cmd/lpgquery` | Command-line reads |
| `../cmd/lpgbench` | Hub latency benchmark |
| `../gremlin` | Read-only TinkerPop provider and Gremlin Server config |
