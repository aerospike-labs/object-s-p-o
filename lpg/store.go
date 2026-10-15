package lpg

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"

	"graph-sub-pred-obj/store"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ConfigName is the meta record holding the identifier bucket count, which
// every writer and reader of a namespace must share.
const ConfigName = "lpg_config"

// DefaultIDBuckets suits about 1.7B identifiers (~30M INDIVIDUALs) at ~100
// per bucket. Size it for the target before the first load: 2^31 buckets
// suits the ~173B identifiers of 250B triples.
const DefaultIDBuckets = 1 << 24

// DefaultMaxInlineEdges is how many edge entries a vertex record takes before
// the vertex is promoted to a hub.
const DefaultMaxInlineEdges = 256

// Hub edge buckets. Level k of a hub has HubBaseBuckets << k buckets, and an
// edge goes to bucket hash(neighbor) mod that count. When a bucket of the
// newest level fills, the hub moves to the next level, which has twice the
// buckets; edges are never moved. Levels stay few: 1M edges take about 9.
const (
	HubBaseBuckets = 16
	HubBucketEdges = 256 // edge entries per bucket before the hub moves up a level
)

// Store reads and writes the vertex model through a GraphStore's client and
// policies.
type Store struct {
	gs *store.GraphStore

	// IDBuckets is the number of identifier lookup buckets.
	IDBuckets uint64
	// MaxInlineEdges is the soft limit of edge entries in a vertex record;
	// a vertex that reaches it becomes a hub.
	MaxInlineEdges int
	// IndexIdentifiers writes the identifier lookup set. Off, identifiers
	// are only stored on their vertices.
	IndexIdentifiers bool

	mapPolicy   *aero.MapPolicy
	bucketWrite *aero.BatchWritePolicy
	plainWrite  *aero.BatchWritePolicy // vertex writes; keys are stored so scans can share vertices
	inlineWrite *aero.BatchWritePolicy // as plainWrite, but FILTERED_OUT when the vertex is full or a hub
	hubWrite    *aero.BatchWritePolicy // edge bucket writes, FILTERED_OUT when the bucket is full
	wp          *aero.WritePolicy

	hubs sync.Map // "<vertex ID>|<out or in>" -> edge bucket level last seen by this process
}

// New returns a Store for writing, with the default limits and the bucket
// count recorded for the namespace, recording idBuckets (or DefaultIDBuckets
// when 0) if none is recorded yet. A non-zero idBuckets that differs from the
// recorded count is an error, since lookups would read the wrong buckets.
func New(gs *store.GraphStore, idBuckets uint64) (*Store, error) {
	want := idBuckets
	if want == 0 {
		want = DefaultIDBuckets
	}
	if _, err := gs.CreateMetaString(ConfigName, "id_buckets", strconv.FormatUint(want, 10)); err != nil {
		return nil, fmt.Errorf("failed to record lpg config: %w", err)
	}
	s, err := Open(gs)
	if err != nil {
		return nil, err
	}
	if idBuckets != 0 && idBuckets != s.IDBuckets {
		return nil, fmt.Errorf("namespace uses %d identifier buckets, not %d; load into an empty namespace to change it", s.IDBuckets, idBuckets)
	}
	return s, nil
}

// Open returns a Store for a namespace already written by New, without
// writing anything.
func Open(gs *store.GraphStore) (*Store, error) {
	stored, err := gs.GetMetaString(ConfigName, "id_buckets")
	if err != nil {
		return nil, fmt.Errorf("failed to read lpg config: %w", err)
	}
	if stored == "" {
		return nil, fmt.Errorf("namespace %s has no lpg data (meta record %s is missing)", gs.Namespace, ConfigName)
	}
	buckets, err := strconv.ParseUint(stored, 10, 64)
	if err != nil || buckets == 0 {
		return nil, fmt.Errorf("invalid id_buckets %q in meta record %s", stored, ConfigName)
	}

	s := &Store{
		gs:               gs,
		IDBuckets:        buckets,
		IndexIdentifiers: true,
		mapPolicy:        aero.NewMapPolicy(aero.MapOrder.KEY_ORDERED, aero.MapWriteMode.UPDATE),
		wp:               gs.WritePolicy,
	}
	s.bucketWrite = aero.NewBatchWritePolicy()
	s.bucketWrite.RecordExistsAction = aero.UPDATE
	s.plainWrite = aero.NewBatchWritePolicy()
	s.plainWrite.RecordExistsAction = aero.UPDATE
	s.plainWrite.SendKey = true
	s.hubWrite = aero.NewBatchWritePolicy()
	s.hubWrite.RecordExistsAction = aero.UPDATE
	s.hubWrite.SendKey = true
	s.hubWrite.FilterExpression = below(binEdges, HubBucketEdges)
	s.SetMaxInlineEdges(DefaultMaxInlineEdges)
	return s, nil
}

// SetMaxInlineEdges sets the soft limit of edge entries in a vertex record.
func (s *Store) SetMaxInlineEdges(n int) {
	s.MaxInlineEdges = n
	s.inlineWrite = aero.NewBatchWritePolicy()
	s.inlineWrite.RecordExistsAction = aero.UPDATE
	s.inlineWrite.SendKey = true
	s.inlineWrite.FilterExpression = aero.ExpAnd(
		aero.ExpNot(aero.ExpBinExists(binHubOut)),
		aero.ExpNot(aero.ExpBinExists(binHubIn)),
		below(binEdges, n))
}

// below is true while a record's count bin is under n, or absent.
func below(bin string, n int) *aero.Expression {
	return aero.ExpOr(
		aero.ExpNot(aero.ExpBinExists(bin)),
		aero.ExpLess(aero.ExpIntBin(bin), aero.ExpIntVal(int64(n))),
	)
}

func (s *Store) vertexKey(id string) (*aero.Key, error) {
	return aero.NewKey(s.gs.Namespace, VertexSet, id)
}

// dirName names a direction in edge bucket keys.
func dirName(outgoing bool) string {
	if outgoing {
		return "out"
	}
	return "in"
}

// levelBin is the bin holding a hub's edge bucket level in a direction.
func levelBin(outgoing bool) string {
	if outgoing {
		return binHubOut
	}
	return binHubIn
}

// hubBuckets returns the number of edge buckets at a level.
func hubBuckets(level int) uint64 {
	return uint64(HubBaseBuckets) << level
}

// hubBucket returns the bucket of a neighbor at a level.
func hubBucket(nbr string, level int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(nbr))
	return h.Sum64() % hubBuckets(level)
}

func (s *Store) edgeBucketKey(id string, outgoing bool, level int, bucket uint64) (*aero.Key, error) {
	return aero.NewKey(s.gs.Namespace, EdgeBucketSet,
		id+"|"+dirName(outgoing)+"|"+strconv.Itoa(level)+"|"+strconv.FormatUint(bucket, 10))
}

// identifierSlot returns the lookup bucket of an identifier and its tag
// within the bucket, both from one hash of its type and value. 64-bit tags
// make collisions within a bucket negligible; readers still confirm a match
// against the vertex.
func (s *Store) identifierSlot(idType, value string) (bucket uint64, tag int64) {
	h := sha256.Sum256([]byte(idType + "\x00" + value))
	return binary.BigEndian.Uint64(h[0:8]) % s.IDBuckets, int64(binary.BigEndian.Uint64(h[8:16]))
}

func (s *Store) bucketKey(bucket uint64) (*aero.Key, error) {
	return aero.NewKey(s.gs.Namespace, IDIndexSet, int64(bucket))
}

// hubLevel returns the edge bucket level this process last saw for a hub
// in a direction.
func (s *Store) hubLevel(id string, outgoing bool) (int, bool) {
	if l, ok := s.hubs.Load(id + "|" + dirName(outgoing)); ok {
		return l.(int), true
	}
	return 0, false
}

// batchItem is what a batch record writes, for handling its result.
type batchItem struct {
	vertex *Mutation // vertex record write (edges inline when not a hub)
	bucket *hubWrite // edge bucket write of a known hub
	ident  bool      // identifier bucket write
	rec    *aero.BatchWrite
}

// hubWrite is a set of a hub's edges in one direction and bucket.
type hubWrite struct {
	id       string
	outgoing bool
	level    int
	edges    map[string]map[string]Edge
}

// Apply writes mutations in one batch: one write per vertex, with mutations
// of the same vertex merged first, plus one write per edge bucket and
// identifier bucket touched. Edges of a vertex this process knows is a hub go
// straight to its edge buckets. A vertex that fills its record is promoted to
// a hub, and a hub whose bucket fills moves up a level; those few writes are
// then retried individually. Mutations may be modified.
func (s *Store) Apply(muts []*Mutation) error {
	merged := make(map[string]*Mutation, len(muts))
	vertices := make([]*Mutation, 0, len(muts))
	for _, m := range muts {
		if prev, ok := merged[m.ID]; ok {
			prev.merge(m)
			continue
		}
		merged[m.ID] = m
		vertices = append(vertices, m)
	}

	var items []batchItem
	var slow []*Mutation   // too many edges to risk inline in one write
	var newDir []*hubWrite // a known hub's edges in a direction without buckets yet
	for _, m := range vertices {
		key, err := s.vertexKey(m.ID)
		if err != nil {
			return err
		}
		outLevel, outHub := s.hubLevel(m.ID, true)
		inLevel, inHub := s.hubLevel(m.ID, false)
		switch {
		case outHub || inHub:
			if ops := s.vertexOps(m, false); len(ops) > 0 {
				items = append(items, batchItem{vertex: m, rec: aero.NewBatchWrite(s.plainWrite, key, ops...)})
			}
			for _, dir := range []struct {
				outgoing, known bool
				level           int
				edges           map[string]map[string]Edge
			}{{true, outHub, outLevel, m.Out}, {false, inHub, inLevel, m.In}} {
				if len(dir.edges) == 0 {
					continue
				}
				if !dir.known {
					newDir = append(newDir, &hubWrite{id: m.ID, outgoing: dir.outgoing, edges: dir.edges})
					continue
				}
				for _, hw := range splitHubEdges(m.ID, dir.outgoing, dir.edges, dir.level) {
					bk, err := s.edgeBucketKey(hw.id, hw.outgoing, hw.level, hubBucket(firstNeighbor(hw.edges), hw.level))
					if err != nil {
						return err
					}
					items = append(items, batchItem{bucket: hw, rec: aero.NewBatchWrite(s.hubWrite, bk, s.bucketOps(hw)...)})
				}
			}
		case m.edgeCount() > s.MaxInlineEdges/2:
			slow = append(slow, m)
		default:
			policy := s.plainWrite
			if m.edgeCount() > 0 {
				policy = s.inlineWrite
			}
			items = append(items, batchItem{vertex: m, rec: aero.NewBatchWrite(policy, key, s.vertexOps(m, true)...)})
		}
	}

	if s.IndexIdentifiers {
		bucketOps := make(map[uint64][]*aero.Operation)
		var buckets []uint64
		for _, m := range vertices {
			for idType, vals := range m.Identifiers {
				for value, id := range vals {
					b, tag := s.identifierSlot(idType, value)
					if bucketOps[b] == nil {
						buckets = append(buckets, b)
					}
					bucketOps[b] = append(bucketOps[b], aero.MapPutOp(s.mapPolicy, binBucket, m.ID, id.Created,
						aero.CtxMapKeyCreate(aero.NewIntegerValue(int(tag)), aero.MapOrder.KEY_ORDERED)))
				}
			}
		}
		for _, b := range buckets {
			key, err := s.bucketKey(b)
			if err != nil {
				return err
			}
			items = append(items, batchItem{ident: true, rec: aero.NewBatchWrite(s.bucketWrite, key, bucketOps[b]...)})
		}
	}

	if len(items) > 0 {
		records := make([]aero.BatchRecordIfc, len(items))
		for i, it := range items {
			records[i] = it.rec
		}
		if err := s.gs.Client.BatchOperate(s.gs.BatchPolicy, records); err != nil {
			return err
		}
	}

	failed := 0
	var first error
	fail := func(err error) {
		if first == nil {
			first = err
		}
		failed++
	}
	for _, it := range items {
		br := it.rec.BatchRec()
		switch {
		case br.ResultCode == types.OK:
		case br.ResultCode != types.FILTERED_OUT:
			fail(br.Err)
		case it.vertex != nil:
			// Full, or promoted by another writer: nothing was written
			if err := s.writeSlow(it.vertex); err != nil {
				fail(err)
			}
		case it.bucket != nil:
			if err := s.writeHubEdges(it.bucket.id, it.bucket.outgoing, it.bucket.edges, it.bucket.level+1); err != nil {
				fail(err)
			}
		}
	}
	for _, m := range slow {
		if err := s.writeSlow(m); err != nil {
			fail(err)
		}
	}
	for _, hw := range newDir {
		if err := s.writeHubEdges(hw.id, hw.outgoing, hw.edges, 0); err != nil {
			fail(err)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d batch records failed, first: %w", failed, len(items)+len(slow)+len(newDir), first)
	}
	return nil
}

// splitHubEdges groups a hub's edges in one direction by bucket at a level.
func splitHubEdges(id string, outgoing bool, edges map[string]map[string]Edge, level int) []*hubWrite {
	var out []*hubWrite
	byBucket := make(map[uint64]*hubWrite)
	for pred, nbrs := range edges {
		for n, e := range nbrs {
			b := hubBucket(n, level)
			hw := byBucket[b]
			if hw == nil {
				hw = &hubWrite{id: id, outgoing: outgoing, level: level}
				byBucket[b] = hw
				out = append(out, hw)
			}
			hw.edges = addEdge(hw.edges, pred, n, e)
		}
	}
	return out
}

func firstNeighbor(edges map[string]map[string]Edge) string {
	for _, nbrs := range edges {
		for n := range nbrs {
			return n
		}
	}
	return ""
}

func countEdges(edges map[string]map[string]Edge) int {
	n := 0
	for _, nbrs := range edges {
		n += len(nbrs)
	}
	return n
}

// vertexOps returns the operations adding m to its vertex record, with its
// edges inline when withEdges.
func (s *Store) vertexOps(m *Mutation, withEdges bool) []*aero.Operation {
	var ops []*aero.Operation
	if m.Type != "" {
		ops = append(ops,
			aero.PutOp(aero.NewBin(binType, m.Type)),
			aero.PutOp(aero.NewBin(binCreated, m.Created)))
	}
	for idType, vals := range m.Identifiers {
		items := make(map[interface{}]interface{}, len(vals))
		for v, id := range vals {
			items[v] = []interface{}{id.Created, id.LastSeen, id.Decay}
		}
		ops = append(ops, aero.MapPutItemsOp(s.mapPolicy, binIDs, items,
			aero.CtxMapKeyCreate(aero.NewStringValue(idType), aero.MapOrder.KEY_ORDERED)))
	}
	if m.Links > 0 {
		ops = append(ops, aero.AddOp(aero.NewBin(binLinks, m.Links)))
	}
	if withEdges {
		ops = append(ops, s.edgeMapOps(binOut, m.Out)...)
		ops = append(ops, s.edgeMapOps(binIn, m.In)...)
		if n := m.edgeCount(); n > 0 {
			ops = append(ops, aero.AddOp(aero.NewBin(binEdges, n)))
		}
	}
	return ops
}

// edgeMapOps returns the operations adding edges to a {predicate: {neighbor:
// props}} map bin.
func (s *Store) edgeMapOps(bin string, edges map[string]map[string]Edge) []*aero.Operation {
	var ops []*aero.Operation
	for pred, nbrs := range edges {
		items := make(map[interface{}]interface{}, len(nbrs))
		for n, e := range nbrs {
			items[n] = e.tuple()
		}
		ops = append(ops, aero.MapPutItemsOp(s.mapPolicy, bin, items,
			aero.CtxMapKeyCreate(aero.NewStringValue(pred), aero.MapOrder.KEY_ORDERED)))
	}
	return ops
}

// bucketOps returns the operations adding a hub's edges to one edge bucket.
func (s *Store) bucketOps(hw *hubWrite) []*aero.Operation {
	ops := s.edgeMapOps(binBucketE, hw.edges)
	return append(ops,
		aero.AddOp(aero.NewBin(binEdges, countEdges(hw.edges))),
		aero.PutOp(aero.NewBin(binVertex, hw.id)))
}

// writeSlow writes a mutation whose vertex may be full or a hub: inline if
// it still fits, otherwise its other changes to the vertex and its edges to
// the hub's buckets. A vertex becomes a hub when its first edge bucket level
// is recorded.
func (s *Store) writeSlow(m *Mutation) error {
	key, err := s.vertexKey(m.ID)
	if err != nil {
		return err
	}
	rec, aerr := s.gs.Client.Get(nil, key, binEdges, binHubOut, binHubIn)
	if aerr != nil && !aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
		return aerr
	}
	count, isHub := 0, false
	if rec != nil {
		count, _ = rec.Bins[binEdges].(int)
		_, outHub := rec.Bins[binHubOut].(int)
		_, inHub := rec.Bins[binHubIn].(int)
		isHub = outHub || inHub
	}

	wp := *s.wp
	wp.SendKey = true
	if !isHub && count+m.edgeCount() <= s.MaxInlineEdges {
		inline := wp
		inline.FilterExpression = s.inlineWrite.FilterExpression
		_, aerr := s.gs.Client.Operate(&inline, key, s.vertexOps(m, true)...)
		if aerr == nil {
			return nil
		}
		if !aerr.Matches(types.FILTERED_OUT) {
			return aerr
		}
		// Filled or promoted meanwhile: continue as a hub
	}

	if ops := s.vertexOps(m, false); len(ops) > 0 {
		if _, aerr := s.gs.Client.Operate(&wp, key, ops...); aerr != nil {
			return aerr
		}
	}
	if len(m.Out) > 0 {
		if err := s.writeHubEdges(m.ID, true, m.Out, 0); err != nil {
			return err
		}
	}
	if len(m.In) > 0 {
		if err := s.writeHubEdges(m.ID, false, m.In, 0); err != nil {
			return err
		}
	}
	return nil
}

// writeHubEdges writes a hub's edges in one direction to their buckets at
// its newest level in that direction, at least minLevel, recording level
// minLevel first if the direction has none yet. A bucket that is full moves
// the direction up a level, by compare-and-set so racing writers agree, and
// its edges are written again there.
func (s *Store) writeHubEdges(id string, outgoing bool, edges map[string]map[string]Edge, minLevel int) error {
	key, err := s.vertexKey(id)
	if err != nil {
		return err
	}
	bin := levelBin(outgoing)
	pending := edges
	for attempt := 0; attempt < 64 && len(pending) > 0; attempt++ {
		rec, aerr := s.gs.Client.Get(nil, key, bin)
		if aerr != nil {
			return aerr
		}
		level, ok := rec.Bins[bin].(int)
		if !ok || level < minLevel {
			if err := s.casHubLevel(key, bin, level, ok, minLevel); err != nil {
				return err
			}
			continue
		}
		s.hubs.Store(id+"|"+dirName(outgoing), level)

		groups := splitHubEdges(id, outgoing, pending, level)
		records := make([]aero.BatchRecordIfc, len(groups))
		for i, hw := range groups {
			bk, err := s.edgeBucketKey(id, outgoing, level, hubBucket(firstNeighbor(hw.edges), level))
			if err != nil {
				return err
			}
			records[i] = aero.NewBatchWrite(s.hubWrite, bk, s.bucketOps(hw)...)
		}
		if err := s.gs.Client.BatchOperate(s.gs.BatchPolicy, records); err != nil {
			return err
		}
		pending = nil
		for i, r := range records {
			br := r.BatchRec()
			switch br.ResultCode {
			case types.OK:
			case types.FILTERED_OUT:
				pending = mergeEdges(pending, groups[i].edges)
			default:
				return br.Err
			}
		}
		if len(pending) > 0 {
			minLevel = level + 1
		}
	}
	if len(pending) > 0 {
		return fmt.Errorf("hub %s: edge buckets kept filling", id)
	}
	return nil
}

// casHubLevel sets a hub's level bin to new if it still holds old (or is
// still absent, when !existed), doing nothing if another writer moved it.
func (s *Store) casHubLevel(key *aero.Key, bin string, old int, existed bool, new int) error {
	wp := *s.wp
	if existed {
		wp.FilterExpression = aero.ExpEq(aero.ExpIntBin(bin), aero.ExpIntVal(int64(old)))
	} else {
		wp.FilterExpression = aero.ExpNot(aero.ExpBinExists(bin))
	}
	_, err := s.gs.Client.Operate(&wp, key, aero.PutOp(aero.NewBin(bin, new)))
	if err != nil && !err.Matches(types.FILTERED_OUT) {
		return err
	}
	return nil
}

// PromoteHub makes a vertex a hub before it has many edges, in both
// directions, so that its edges go straight to edge buckets rather than
// filling its record first: the equivalent of designating a supernode up
// front. Edges already in its record stay there. Safe to repeat.
func (s *Store) PromoteHub(id string) error {
	key, err := s.vertexKey(id)
	if err != nil {
		return err
	}
	for _, outgoing := range []bool{true, false} {
		if err := s.casHubLevel(key, levelBin(outgoing), 0, false, 0); err != nil {
			return err
		}
	}
	return nil
}

// LabelIndexName is the secondary index on vertex types, created only on
// request (generate -label-index): it serves "all vertices of a type"
// without a scan, at about 14 bytes of index memory per vertex.
const LabelIndexName = "idx_vertices_type"

// CreateLabelIndex creates the secondary index on vertex types, if it
// doesn't exist yet, and waits until it is built.
func (s *Store) CreateLabelIndex() error {
	task, err := s.gs.Client.CreateIndex(nil, s.gs.Namespace, VertexSet, LabelIndexName, binType, aero.STRING)
	if err != nil {
		if err.Matches(types.INDEX_FOUND) {
			return nil
		}
		return err
	}
	return <-task.OnComplete()
}
