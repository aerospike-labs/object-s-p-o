package lpg

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"

	"graph-sub-pred-obj/store"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ConfigName is the meta record holding the identifier bucket count, which
// every writer and reader of a namespace must share.
const ConfigName = "lpg_config"

// FormatName is the meta record holding the version of the record layout a
// namespace was written with, and Format the version this package writes.
// Version 2 keeps counters exact and merges edges and identifiers rather than
// replacing them; data written before it has no format record.
const (
	FormatName = "lpg_format"
	Format     = "2"
)

// IDIndexOffName is the meta record that exists once any writer has skipped
// the identifier lookup set (-id-index=false). Readers then can't trust the
// set to hold every identifier and look identifiers up by scanning.
const IDIndexOffName = "lpg_id_index_off"

// DefaultIDBuckets suits about 1.7B identifiers (~30M INDIVIDUALs) at ~100
// per bucket. Size it for the target before the first load: 2^31 buckets
// suits the ~173B identifiers of 250B triples.
const DefaultIDBuckets = 1 << 24

// MaxIdentifierShare is the most vertices the identifier lookup set holds
// for one identifier. A placeholder identifier shared by millions of
// vertices would otherwise outgrow its bucket record and fail every write
// to it. Readers treat an identifier with this many entries as over-shared
// and find its vertices by scanning. gremlin/ uses the same value.
const MaxIdentifierShare = 1000

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
	// Format is the record layout version the namespace was written with,
	// "" for data written before formats were recorded.
	Format string

	indexIDs bool

	bucketWrite  *aero.BatchWritePolicy // identifier bucket writes
	plainWrite   *aero.BatchWritePolicy // vertex writes; keys are stored so scans can return vertices
	inlineWrite  *aero.BatchWritePolicy // as plainWrite, but FILTERED_OUT when the vertex is full or a hub
	hubWrite     *aero.BatchWritePolicy // new edges to a bucket, FILTERED_OUT when the bucket is full
	hubUpdate    *aero.BatchWritePolicy // edges already in a bucket, which don't grow it
	wp           *aero.WritePolicy      // single-record writes, with keys stored
	locatePolicy *aero.BatchReadPolicy

	hubs sync.Map // "<vertex ID>|<out or in>" -> edge bucket level last seen by this process
}

// New returns a Store for writing, with the default limits and the bucket
// count recorded for the namespace, recording idBuckets (or DefaultIDBuckets
// when 0) and the format if the namespace has no lpg data yet. A non-zero
// idBuckets that differs from the recorded count is an error, since lookups
// would read the wrong buckets, and so is data in an older format, which
// this version would write inconsistently.
func New(gs *store.GraphStore, idBuckets uint64) (*Store, error) {
	want := idBuckets
	if want == 0 {
		want = DefaultIDBuckets
	}
	existing, err := gs.GetMetaString(ConfigName, "id_buckets")
	if err != nil {
		return nil, fmt.Errorf("failed to read lpg config: %w", err)
	}
	if existing == "" {
		// The format goes first, so a writer that sees the config sees it too
		if _, err := gs.CreateMetaString(FormatName, "format", Format); err != nil {
			return nil, fmt.Errorf("failed to record lpg format: %w", err)
		}
		if _, err := gs.CreateMetaString(ConfigName, "id_buckets", strconv.FormatUint(want, 10)); err != nil {
			return nil, fmt.Errorf("failed to record lpg config: %w", err)
		}
	}
	s, err := Open(gs)
	if err != nil {
		return nil, err
	}
	if s.Format != Format {
		return nil, fmt.Errorf("namespace %s holds lpg data in an older format (%q, this version writes %q); load into an empty namespace",
			gs.Namespace, s.Format, Format)
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
	format, err := gs.GetMetaString(FormatName, "format")
	if err != nil {
		return nil, fmt.Errorf("failed to read lpg format: %w", err)
	}

	s := &Store{
		gs:        gs,
		IDBuckets: buckets,
		Format:    format,
		indexIDs:  true,
	}
	wp := *gs.WritePolicy
	wp.SendKey = true
	s.wp = &wp
	s.bucketWrite = aero.NewBatchWritePolicy()
	s.bucketWrite.RecordExistsAction = aero.UPDATE
	s.plainWrite = aero.NewBatchWritePolicy()
	s.plainWrite.RecordExistsAction = aero.UPDATE
	s.plainWrite.SendKey = true
	s.hubUpdate = aero.NewBatchWritePolicy()
	s.hubUpdate.RecordExistsAction = aero.UPDATE
	s.hubUpdate.SendKey = true
	hw := *s.hubUpdate
	hw.FilterExpression = below(binEdges, HubBucketEdges)
	s.hubWrite = &hw
	s.locatePolicy = aero.NewBatchReadPolicy()
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

// SetIndexIdentifiers turns writing the identifier lookup set on or off.
// Turning it off records that in the namespace, for good: readers then find
// identifiers by scanning, since the set no longer holds them all.
func (s *Store) SetIndexIdentifiers(on bool) error {
	s.indexIDs = on
	if on {
		return nil
	}
	if _, err := s.gs.CreateMetaString(IDIndexOffName, "off", "true"); err != nil {
		return fmt.Errorf("failed to record that the identifier index is off: %w", err)
	}
	return nil
}

// IndexesIdentifiers reports whether this Store writes the identifier lookup
// set.
func (s *Store) IndexesIdentifiers() bool { return s.indexIDs }

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

// edgeBin is the vertex bin holding its inline edges in a direction.
func edgeBin(outgoing bool) string {
	if outgoing {
		return binOut
	}
	return binIn
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
	vertex *Mutation // inline vertex write, retried through writeSlow if filtered out
	rec    *aero.BatchWrite
}

// hubWrite is a set of a hub's edges in one direction.
type hubWrite struct {
	id        string
	outgoing  bool
	level     int
	edges     map[string]map[string]Edge
	linkPreds map[string]bool // predicates whose edges count as INDIVIDUAL links
}

// Apply writes mutations in one batch: one write per vertex, with mutations
// of the same vertex merged first, plus one write per identifier bucket
// touched. Edges of a vertex this process knows is a hub, and the few
// vertices that fill their record, are then written to the hubs' edge
// buckets. Applying the same mutations again changes nothing, so a failed
// Apply can be retried. Mutations may be modified.
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
	var slow []*Mutation     // too many edges to risk inline in one write
	var hubEdges []*hubWrite // edges of known hubs
	for _, m := range vertices {
		key, err := s.vertexKey(m.ID)
		if err != nil {
			return err
		}
		_, outHub := s.hubLevel(m.ID, true)
		_, inHub := s.hubLevel(m.ID, false)
		switch {
		case outHub || inHub:
			// Once a vertex is a hub in either direction, its inline writes
			// are filtered out, so all its new edges go to buckets
			if ops := s.vertexOps(m, false); len(ops) > 0 {
				items = append(items, batchItem{rec: aero.NewBatchWrite(s.plainWrite, key, ops...)})
			}
			if len(m.Out) > 0 {
				hubEdges = append(hubEdges, &hubWrite{id: m.ID, outgoing: true, edges: m.Out})
			}
			if len(m.In) > 0 {
				hubEdges = append(hubEdges, &hubWrite{id: m.ID, outgoing: false, edges: m.In, linkPreds: m.LinkPreds})
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

	if s.indexIDs {
		bucketOps := make(map[uint64][]*aero.Operation)
		var buckets []uint64
		for _, m := range vertices {
			for idType, vals := range m.Identifiers {
				for value, id := range vals {
					b, tag := s.identifierSlot(idType, value)
					if bucketOps[b] == nil {
						buckets = append(buckets, b)
					}
					tagCtx := aero.CtxMapKey(aero.NewIntegerValue(int(tag)))
					bucketOps[b] = append(bucketOps[b],
						aero.MapPutOp(insertPolicy, binBucket, m.ID, id.Created,
							aero.CtxMapKeyCreate(aero.NewIntegerValue(int(tag)), aero.MapOrder.KEY_ORDERED)),
						// Keep an over-shared identifier's entry bounded
						aero.MapRemoveByIndexRangeOp(binBucket, MaxIdentifierShare, aero.MapReturnType.NONE, tagCtx))
				}
			}
		}
		for _, b := range buckets {
			key, err := s.bucketKey(b)
			if err != nil {
				return err
			}
			items = append(items, batchItem{rec: aero.NewBatchWrite(s.bucketWrite, key, bucketOps[b]...)})
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
		case br.ResultCode == types.FILTERED_OUT && it.vertex != nil:
			// Full, or promoted by another writer: nothing was written
			if err := s.writeSlow(it.vertex); err != nil {
				fail(err)
			}
		default:
			fail(br.Err)
		}
	}
	for _, m := range slow {
		if err := s.writeSlow(m); err != nil {
			fail(err)
		}
	}
	for _, hw := range hubEdges {
		if err := s.writeHubEdges(hw.id, hw.outgoing, hw.edges, hw.linkPreds, 0); err != nil {
			fail(err)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d writes failed, first: %w", failed, len(items)+len(slow)+len(hubEdges), first)
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
// edges inline when withEdges. The record's edge_count and indiv_links stay
// equal to the edge entries it holds.
func (s *Store) vertexOps(m *Mutation, withEdges bool) []*aero.Operation {
	ops := typeOps(m)
	ops = append(ops, identifierOps(m.Identifiers)...)
	if !withEdges || m.edgeCount() == 0 {
		return ops
	}
	var edges []*aero.Operation
	edges = append(edges, edgeOps(binOut, m.Out)...)
	edges = append(edges, edgeOps(binIn, m.In)...)
	cs := []counter{{binEdges, aero.ExpNumAdd(entries(binOut, sortedKeys(m.Out)), entries(binIn, sortedKeys(m.In)))}}
	if lp := m.linkPreds(); len(lp) > 0 {
		cs = append(cs, counter{binLinks, entries(binIn, lp)})
	}
	return append(ops, withCounters(edges, cs...)...)
}

// Result names of the reads in an edge bucket write that count its
// INDIVIDUAL links before and after.
const (
	linksBefore = "links_before"
	linksAfter  = "links_after"
)

// bucketOps returns the operations adding a hub's edges to one edge bucket,
// keeping its edge_count equal to its entries. With link predicates, it also
// reads the link edges before and after, for the hub's indiv_links.
func bucketOps(hw *hubWrite) []*aero.Operation {
	ops := withCounters(edgeOps(binBucketE, hw.edges), counter{binEdges, entries(binBucketE, sortedKeys(hw.edges))})
	ops = append(ops, aero.PutOp(aero.NewBin(binVertex, hw.id)))
	var lp []string
	for p := range hw.edges {
		if hw.linkPreds[p] {
			lp = append(lp, p)
		}
	}
	if len(lp) == 0 {
		return ops
	}
	sort.Strings(lp)
	n := entries(binBucketE, lp)
	ops = append([]*aero.Operation{aero.ExpReadOp(linksBefore, n, aero.ExpReadFlagDefault)}, ops...)
	return append(ops, aero.ExpReadOp(linksAfter, n, aero.ExpReadFlagDefault))
}

// addedLinks returns the INDIVIDUAL links an edge bucket write added.
func addedLinks(r *aero.Record) int {
	if r == nil {
		return 0
	}
	after, _ := r.Bins[linksAfter].(int)
	before, _ := r.Bins[linksBefore].(int)
	return after - before
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

	if !isHub && count+m.edgeCount() <= s.MaxInlineEdges {
		inline := *s.wp
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
		if _, aerr := s.gs.Client.Operate(s.wp, key, ops...); aerr != nil {
			return aerr
		}
	}
	if len(m.Out) > 0 {
		if err := s.writeHubEdges(m.ID, true, m.Out, nil, 0); err != nil {
			return err
		}
	}
	if len(m.In) > 0 {
		if err := s.writeHubEdges(m.ID, false, m.In, m.LinkPreds, 0); err != nil {
			return err
		}
	}
	return nil
}

// writeHubEdges writes a hub's edges in one direction, recording level
// minLevel first if the direction has none yet (creating the vertex record
// if needed). An edge already stored, inline or in a bucket at any level, is
// merged where it is, so writing an edge again never stores a second copy.
// New edges go to the newest level, at least minLevel; a bucket that is full
// moves the direction up a level, by compare-and-set so racing writers
// agree, and its new edges are written again there. INDIVIDUAL links added
// over linkPreds are then added to the hub's indiv_links.
func (s *Store) writeHubEdges(id string, outgoing bool, edges map[string]map[string]Edge, linkPreds map[string]bool, minLevel int) error {
	key, err := s.vertexKey(id)
	if err != nil {
		return err
	}
	bin := levelBin(outgoing)
	pending := edges
	links := 0
	for attempt := 0; attempt < 64 && len(pending) > 0; attempt++ {
		level, ok := 0, false
		rec, aerr := s.gs.Client.Get(nil, key, bin)
		switch {
		case aerr == nil:
			level, ok = rec.Bins[bin].(int)
		case !aerr.Matches(types.KEY_NOT_FOUND_ERROR):
			return aerr
		}
		if !ok || level < minLevel {
			if err := s.casHubLevel(key, bin, level, ok, minLevel); err != nil {
				return err
			}
			continue
		}
		s.hubs.Store(id+"|"+dirName(outgoing), level)

		inline, stored, fresh, err := s.locateHubEdges(id, outgoing, level, pending)
		if err != nil {
			return err
		}
		var records []aero.BatchRecordIfc
		var groups []*hubWrite // the bucket write of each record, nil for the vertex
		if len(inline) > 0 {
			ops := withCounters(edgeOps(edgeBin(outgoing), inline), counter{binEdges, entries(edgeBin(outgoing), sortedKeys(inline))})
			records = append(records, aero.NewBatchWrite(s.plainWrite, key, ops...))
			groups = append(groups, nil)
		}
		for l := 0; l <= level; l++ {
			for _, hw := range splitHubEdges(id, outgoing, stored[l], l) {
				hw.linkPreds = linkPreds
				bk, err := s.edgeBucketKey(id, outgoing, l, hubBucket(firstNeighbor(hw.edges), l))
				if err != nil {
					return err
				}
				records = append(records, aero.NewBatchWrite(s.hubUpdate, bk, bucketOps(hw)...))
				groups = append(groups, hw)
			}
		}
		newGroups := splitHubEdges(id, outgoing, fresh, level)
		for _, hw := range newGroups {
			hw.linkPreds = linkPreds
			bk, err := s.edgeBucketKey(id, outgoing, level, hubBucket(firstNeighbor(hw.edges), level))
			if err != nil {
				return err
			}
			records = append(records, aero.NewBatchWrite(s.hubWrite, bk, bucketOps(hw)...))
			groups = append(groups, hw)
		}
		if err := s.gs.Client.BatchOperate(s.gs.BatchPolicy, records); err != nil {
			return err
		}
		pending = nil
		for i, r := range records {
			br := r.BatchRec()
			switch br.ResultCode {
			case types.OK:
				links += addedLinks(br.Record)
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
	if links > 0 {
		if _, aerr := s.gs.Client.Operate(s.wp, key, aero.AddOp(aero.NewBin(binLinks, links))); aerr != nil {
			return aerr
		}
	}
	return nil
}

// locateHubEdges sorts a hub's edges in one direction by where they are
// already stored: inline in its record, in the bucket they hash to at a
// level up to level (the newest such level when several), or nowhere yet.
// It reads the record and one bucket per edge and level, asking each for
// just the edges that could be there.
func (s *Store) locateHubEdges(id string, outgoing bool, level int, edges map[string]map[string]Edge) (
	inline map[string]map[string]Edge, stored map[int]map[string]map[string]Edge, fresh map[string]map[string]Edge, err error) {

	type probe struct {
		level int // -1 for the vertex record
		preds []string
	}
	var records []aero.BatchRecordIfc
	var probes []probe
	add := func(key *aero.Key, bin string, l int, group map[string]map[string]Edge) {
		preds := sortedKeys(group)
		ops := make([]*aero.Operation, len(preds))
		for i, p := range preds {
			ops[i] = aero.ExpReadOp("p"+strconv.Itoa(i), keysIn(bin, p, sortedKeys(group[p])), aero.ExpReadFlagEvalNoFail)
		}
		records = append(records, aero.NewBatchReadOps(s.locatePolicy, key, ops...))
		probes = append(probes, probe{l, preds})
	}
	vk, err := s.vertexKey(id)
	if err != nil {
		return nil, nil, nil, err
	}
	add(vk, edgeBin(outgoing), -1, edges)
	for l := 0; l <= level; l++ {
		for _, hw := range splitHubEdges(id, outgoing, edges, l) {
			bk, err := s.edgeBucketKey(id, outgoing, l, hubBucket(firstNeighbor(hw.edges), l))
			if err != nil {
				return nil, nil, nil, err
			}
			add(bk, binBucketE, l, hw.edges)
		}
	}
	if err := s.gs.Client.BatchOperate(s.gs.BatchPolicy, records); err != nil {
		return nil, nil, nil, err
	}

	where := make(map[[2]string]int) // (predicate, neighbor) -> level, -1 inline
	for i, r := range records {
		br := r.BatchRec()
		if br.ResultCode == types.KEY_NOT_FOUND_ERROR {
			continue
		}
		if br.ResultCode != types.OK {
			return nil, nil, nil, br.Err
		}
		for j, p := range probes[i].preds {
			found, _ := br.Record.Bins["p"+strconv.Itoa(j)].([]interface{})
			for _, n := range found {
				if ns, ok := n.(string); ok {
					where[[2]string{p, ns}] = probes[i].level // later records are newer levels
				}
			}
		}
	}
	stored = make(map[int]map[string]map[string]Edge)
	for p, nbrs := range edges {
		for n, e := range nbrs {
			l, ok := where[[2]string{p, n}]
			switch {
			case !ok:
				fresh = addEdge(fresh, p, n, e)
			case l < 0:
				inline = addEdge(inline, p, n, e)
			default:
				stored[l] = addEdge(stored[l], p, n, e)
			}
		}
	}
	return inline, stored, fresh, nil
}

// casHubLevel sets a hub's level bin to new if it still holds old (or is
// still absent, when !existed), doing nothing if another writer moved it.
// It creates the vertex record, with its key, if there is none.
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
