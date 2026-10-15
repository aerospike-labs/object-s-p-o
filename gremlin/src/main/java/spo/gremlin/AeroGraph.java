package spo.gremlin;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A read-only TinkerPop graph over the vertex model that {@code generate -model lpg} writes to
 * Aerospike. Vertices are records of the {@code vertices} set; their inline edges are in the
 * {@code out_edges} and {@code in_edges} maps, and a hub's further edges are in hash-bucketed
 * {@code edge_buckets} records. Every read is a key lookup or batch read; only {@code g.V()} and
 * {@code g.E()} without ids, labels or identifiers scan, and labels use the type index when the
 * namespace has one.
 *
 * <p>Vertex ids are the entity ids ({@code INDIVIDUAL:123}), labels are the entity types, and
 * identifiers are multi-properties keyed by identifier type, with {@code created},
 * {@code last_seen} and {@code decay} meta-properties. Edge ids are
 * {@code <out id>|<label>|<in id>}.
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
public final class AeroGraph implements Graph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(AeroGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(AeroGraphStepStrategy.instance(), AeroVertexStepStrategy.instance()));
    }

    private static final Logger log = LoggerFactory.getLogger(AeroGraph.class);

    // Must match lpg/model.go and lpg/store.go
    static final String VERTEX_SET = "vertices";
    static final String EDGE_BUCKET_SET = "edge_buckets";
    static final String ID_INDEX_SET = "identifier_index";
    static final String META_SET = "meta";
    static final String BIN_TYPE = "type";
    static final String BIN_CREATED = "created";
    static final String BIN_IDS = "identifiers";
    static final String BIN_OUT = "out_edges";
    static final String BIN_IN = "in_edges";
    static final String BIN_LINKS = "indiv_links";
    static final String BIN_EDGES = "edge_count";
    static final String BIN_HUB_OUT = "hub_out_level";
    static final String BIN_HUB_IN = "hub_in_level";
    static final String BIN_BUCKET_EDGES = "edges";
    static final String BIN_ID_BUCKET = "vertices_by_tag";
    static final String LABEL_INDEX = "idx_vertices_type";
    static final int HUB_BASE_BUCKETS = 16;
    static final int HUB_BUCKET_EDGES = 256;
    static final int HUB_READ_CHUNK = 64;

    /** The bins of a vertex without its inline edges. */
    static final String[] VERTEX_BINS_NO_EDGES = {BIN_TYPE, BIN_CREATED, BIN_IDS, BIN_LINKS, BIN_HUB_OUT, BIN_HUB_IN};

    /** Edges and neighbors are loaded this many at a time. */
    static final int NEIGHBOR_CHUNK = 64;

    /** What a scan returns of each vertex. */
    enum ScanBins {ALL, NO_EDGES, NONE}

    private final Configuration configuration;
    final AerospikeClient client;
    final String namespace;
    final long idBuckets;
    final boolean labelIndexed;
    final BatchPolicy batchPolicy;
    final QueryPolicy queryPolicy;
    final Policy readPolicy;
    private final boolean supernodeWarnings;
    private final AtomicLong supernodeReads = new AtomicLong();
    private final Features features = new AeroFeatures();
    private final VertexCache cache = new VertexCache(100_000);

    private AeroGraph(Configuration configuration) {
        this.configuration = configuration;
        String[] seeds = configuration.getString("aerospike.hosts", "127.0.0.1:3000").split(",");
        Host[] hosts = new Host[seeds.length];
        for (int i = 0; i < seeds.length; i++) {
            String[] hp = seeds[i].trim().split(":");
            hosts[i] = new Host(hp[0], hp.length > 1 ? Integer.parseInt(hp[1]) : 3000);
        }
        ClientPolicy cp = new ClientPolicy();
        cp.user = configuration.getString("aerospike.user", null);
        cp.password = configuration.getString("aerospike.password", System.getenv("AEROSPIKE_PASSWORD"));
        cp.useServicesAlternate = configuration.getBoolean("aerospike.alternate-access", false);
        this.client = new AerospikeClient(cp, hosts);
        this.namespace = configuration.getString("aerospike.namespace", "test");
        this.supernodeWarnings = configuration.getBoolean("aerospike.supernode.warning", true);

        this.readPolicy = new Policy();
        this.batchPolicy = new BatchPolicy();
        this.queryPolicy = new QueryPolicy();
        this.queryPolicy.includeBinData = true;

        Record config = client.get(readPolicy, new Key(namespace, META_SET, "lpg_config"), "id_buckets");
        if (config == null || config.getString("id_buckets") == null) {
            client.close();
            throw new IllegalStateException("namespace " + namespace + " has no lpg data (meta record lpg_config is missing)");
        }
        this.idBuckets = Long.parseUnsignedLong(config.getString("id_buckets"));
        this.labelIndexed = hasLabelIndex();
        log.info("Aerospike namespace {}: {} identifier buckets, vertex type index {}", namespace, idBuckets,
                labelIndexed ? "present" : "absent (hasLabel scans)");
    }

    /** Called by GraphFactory, e.g. from Gremlin Server's graphs configuration. */
    public static AeroGraph open(Configuration configuration) {
        return new AeroGraph(configuration);
    }

    public static AeroGraph open(String hosts, String namespace) {
        BaseConfiguration c = new BaseConfiguration();
        c.setProperty("aerospike.hosts", hosts);
        c.setProperty("aerospike.namespace", namespace);
        return open(c);
    }

    private boolean hasLabelIndex() {
        String cmd = "sindex-list:ns=" + namespace;
        for (Node node : client.getNodes()) {
            String info = Info.request(null, node, cmd);
            if (info != null && info.contains("indexname=" + LABEL_INDEX + ":")) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------- vertices

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return IteratorUtils.cast(vertices(true, vertexIds));
    }

    /** Reads vertices by id, with or without their inline edges, skipping those that don't exist. */
    Iterator<AeroVertex> vertices(boolean withEdges, Object... vertexIds) {
        if (vertexIds.length == 0) {
            return scanVertices(null, withEdges ? ScanBins.ALL : ScanBins.NO_EDGES);
        }
        List<String> ids = new ArrayList<>(vertexIds.length);
        for (Object id : vertexIds) {
            ids.add(id instanceof Vertex ? ((Vertex) id).id().toString() : id.toString());
        }
        List<AeroVertex> found = new ArrayList<>(ids.size());
        for (AeroVertex v : loadVertices(ids, withEdges)) {
            if (v != null) {
                found.add(v);
            }
        }
        return found.iterator();
    }

    /**
     * Batch-reads vertices, in the order of ids, null where one doesn't exist. Without edges, the
     * edge bins are left out of the read, as AGS omits edge bin data when a traversal doesn't need
     * it; such a vertex reads them if they turn out to be needed.
     */
    List<AeroVertex> loadVertices(List<String> ids, boolean withEdges) {
        List<AeroVertex> out = new ArrayList<>(Collections.nCopies(ids.size(), null));
        List<Integer> missing = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            AeroVertex v = cache.get(ids.get(i), withEdges);
            if (v != null) {
                out.set(i, v);
            } else {
                missing.add(i);
            }
        }
        if (missing.isEmpty()) {
            return out;
        }
        Key[] keys = new Key[missing.size()];
        for (int j = 0; j < keys.length; j++) {
            keys[j] = vertexKey(ids.get(missing.get(j)));
        }
        Record[] recs = withEdges ? client.get(batchPolicy, keys) : client.get(batchPolicy, keys, VERTEX_BINS_NO_EDGES);
        for (int j = 0; j < recs.length; j++) {
            if (recs[j] != null) {
                AeroVertex v = new AeroVertex(this, ids.get(missing.get(j)), recs[j], withEdges);
                cache.put(v);
                out.set(missing.get(j), v);
            }
        }
        return out;
    }

    /** Reads one vertex with its inline edges, or returns null if it doesn't exist. */
    AeroVertex loadVertex(String id, boolean withEdges) {
        return loadVertices(Collections.singletonList(id), withEdges).get(0);
    }

    /**
     * Streams the vertices of a scan, all of them or those of one type: from the type index when
     * the namespace has one, otherwise filtered on the server. With {@link ScanBins#NONE} only
     * keys come back, for counting. Closing the iterator stops the scan.
     */
    CloseableIterator<AeroVertex> scanVertices(String type, ScanBins bins) {
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(VERTEX_SET);
        QueryPolicy qp = new QueryPolicy(queryPolicy);
        if (type != null && labelIndexed) {
            stmt.setFilter(Filter.equal(BIN_TYPE, type));
        } else if (type != null) {
            qp.filterExp = Exp.build(Exp.eq(Exp.stringBin(BIN_TYPE), Exp.val(type)));
        }
        switch (bins) {
            case NONE:
                qp.includeBinData = false;
                break;
            case NO_EDGES:
                stmt.setBinNames(VERTEX_BINS_NO_EDGES);
                break;
            default:
        }
        RecordSet rs = client.query(qp, stmt);
        return new CloseableIterator<AeroVertex>() {
            AeroVertex next;

            @Override
            public boolean hasNext() {
                while (next == null && rs.next()) {
                    Key key = rs.getKey();
                    if (key.userKey != null) {
                        next = bins == ScanBins.NONE
                                ? AeroVertex.ref(AeroGraph.this, key.userKey.toString())
                                : new AeroVertex(AeroGraph.this, key.userKey.toString(), rs.getRecord(), bins == ScanBins.ALL);
                    }
                }
                return next != null;
            }

            @Override
            public AeroVertex next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                AeroVertex v = next;
                next = null;
                return v;
            }

            @Override
            public void close() {
                rs.close();
            }
        };
    }

    /**
     * Finds the vertices holding an identifier through the identifier index: one read of the
     * identifier's bucket, then a batch read confirming each candidate holds it.
     */
    List<AeroVertex> lookupIdentifier(String idType, String value, boolean withEdges) {
        byte[] h = sha256(idType + "\0" + value);
        ByteBuffer bb = ByteBuffer.wrap(h);
        long bucket = Long.remainderUnsigned(bb.getLong(0), idBuckets);
        long tag = bb.getLong(8);
        Record rec;
        try {
            rec = client.operate(null, new Key(namespace, ID_INDEX_SET, bucket),
                    MapOperation.getByKey(BIN_ID_BUCKET, Value.get(tag), MapReturnType.VALUE));
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return Collections.emptyList();
            }
            throw e;
        }
        if (rec == null) {
            return Collections.emptyList();
        }
        // {vertex id: created} of the identifier's tag
        List<String> ids = new ArrayList<>();
        for (Map.Entry<?, ?> e : entries(rec.getValue(BIN_ID_BUCKET))) {
            ids.add(e.getKey().toString());
        }
        List<AeroVertex> found = new ArrayList<>();
        for (AeroVertex v : loadVertices(ids, withEdges)) {
            if (v != null && v.hasIdentifier(idType, value)) {
                found.add(v);
            }
        }
        return found;
    }

    // ------------------------------------------------------------------- edges

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds.length == 0) {
            return IteratorUtils.cast(scanEdges());
        }
        List<Edge> found = new ArrayList<>();
        for (Object o : edgeIds) {
            String id = o instanceof Edge ? ((Edge) o).id().toString() : o.toString();
            int first = id.indexOf('|');
            int last = id.lastIndexOf('|');
            if (first < 0 || first == last) {
                continue;
            }
            String out = id.substring(0, first);
            String label = id.substring(first + 1, last);
            String in = id.substring(last + 1);
            Object tuple = edgeTuple(out, true, label, in);
            if (tuple != null) {
                found.add(AeroEdge.of(this, out, label, in, tuple));
            }
        }
        return found.iterator();
    }

    /** Every edge, from a scan of vertices' outgoing edges, inline and bucketed. */
    private Iterator<AeroEdge> scanEdges() {
        CloseableIterator<AeroVertex> vertices = scanVertices(null, ScanBins.ALL);
        return new CloseableIterator<AeroEdge>() {
            Iterator<AeroEdge> current = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                while (!current.hasNext() && vertices.hasNext()) {
                    current = vertices.next().edgeIterator(Direction.OUT, null);
                }
                return current.hasNext();
            }

            @Override
            public AeroEdge next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }

            @Override
            public void close() {
                vertices.close();
            }
        };
    }

    /**
     * Returns the stored tuple of an edge on vertex id in a direction, or null if it isn't there:
     * one read of the vertex, plus for a hub one bucket read per level. Only the edge itself
     * comes back from the server.
     */
    Object edgeTuple(String id, boolean outgoing, String label, String nbr) {
        String bin = outgoing ? BIN_OUT : BIN_IN;
        String levelBin = outgoing ? BIN_HUB_OUT : BIN_HUB_IN;
        Record rec;
        try {
            rec = client.operate(null, vertexKey(id),
                    ExpOperation.read("e", Exp.build(edgeOf(bin, label, nbr)), ExpReadFlags.EVAL_NO_FAIL),
                    Operation.get(levelBin));
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return null;
            }
            throw e;
        }
        if (rec == null) {
            return null;
        }
        if (tuple(rec.getValue("e")) != null) {
            return rec.getValue("e");
        }
        if (rec.getValue(levelBin) == null) {
            return null;
        }
        int level = rec.getInt(levelBin);
        Key[] keys = new Key[level + 1];
        for (int l = 0; l <= level; l++) {
            keys[l] = edgeBucketKey(id, outgoing, l, hubBucket(nbr, l));
        }
        Record[] recs = client.get(batchPolicy, keys,
                ExpOperation.read("e", Exp.build(edgeOf(BIN_BUCKET_EDGES, label, nbr)), ExpReadFlags.EVAL_NO_FAIL));
        for (int i = recs.length - 1; i >= 0; i--) { // newest level wins
            if (recs[i] != null && tuple(recs[i].getValue("e")) != null) {
                return recs[i].getValue("e");
            }
        }
        return null;
    }

    /**
     * The stored tuple of edge label -> nbr in a {label: {neighbor: tuple}} map bin, or an empty
     * list (both branches of a cond must have one type).
     */
    private static Exp edgeOf(String bin, String label, String nbr) {
        Exp edges = Exp.mapBin(bin);
        Exp byLabel = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP, Exp.val(label), edges);
        return Exp.cond(
                Exp.and(Exp.binExists(bin),
                        MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(label), edges),
                        MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(nbr), byLabel)),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST, Exp.val(nbr), byLabel),
                Exp.val(Collections.emptyList()));
    }

    /**
     * Counts a vertex's edges in a direction with the given labels (all when null) without
     * reading them: its inline edge maps, plus for a hub the counts of its edge buckets computed
     * on the server, as AGS counts adjacency locally. Without labels a bucket's count is its
     * edge_count, which counts an edge written to the same bucket twice twice.
     */
    long degree(AeroVertex v, Direction direction, Set<String> labels) {
        long n = 0;
        for (boolean outgoing : new boolean[]{true, false}) {
            if ((outgoing && direction == Direction.IN) || (!outgoing && direction == Direction.OUT)) {
                continue;
            }
            n += v.inlineCount(outgoing, labels);
            Integer level = v.hubLevel(outgoing);
            if (level == null) {
                continue;
            }
            noteSupernodeRead(v.id, outgoing);
            Exp count;
            if (labels == null) {
                count = Exp.cond(Exp.binExists(BIN_EDGES), Exp.intBin(BIN_EDGES), Exp.val(0));
            } else {
                count = Exp.val(0);
                Exp edges = Exp.mapBin(BIN_BUCKET_EDGES);
                for (String label : labels) {
                    count = Exp.add(count, Exp.cond(
                            Exp.and(Exp.binExists(BIN_BUCKET_EDGES),
                                    MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(label), edges)),
                            MapExp.size(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP, Exp.val(label), edges)),
                            Exp.val(0)));
                }
            }
            Operation op = ExpOperation.read("n", Exp.build(count), ExpReadFlags.DEFAULT);
            long total = bucketsUpTo(level);
            for (long i = 0; i < total; i += 4096) {
                int size = (int) Math.min(4096, total - i);
                Key[] keys = new Key[size];
                for (int j = 0; j < size; j++) {
                    keys[j] = bucketKeyAt(v.id, outgoing, i + j);
                }
                for (Record r : client.get(batchPolicy, keys, op)) {
                    if (r != null) {
                        n += asLong(r.getValue("n"));
                    }
                }
            }
        }
        return n;
    }

    /**
     * Logs a traversal reading a supernode's edge buckets, as AGS does: the first time, then one
     * in every 10 after that. aerospike.supernode.warning=false turns it off.
     */
    void noteSupernodeRead(String id, boolean outgoing) {
        if (supernodeWarnings && supernodeReads.getAndIncrement() % 10 == 0) {
            log.warn("Traversal reads the {} edges of supernode {} (logged for 1 in 10 supernode reads)",
                    outgoing ? "outgoing" : "incoming", id);
        }
    }

    // ------------------------------------------------------------------- keys and hashing

    Key vertexKey(String id) {
        return new Key(namespace, VERTEX_SET, id);
    }

    Key edgeBucketKey(String id, boolean outgoing, int level, long bucket) {
        return new Key(namespace, EDGE_BUCKET_SET, id + "|" + (outgoing ? "out" : "in") + "|" + level + "|" + bucket);
    }

    /** The key of the i-th edge bucket of a hub in a direction, counting level by level. */
    Key bucketKeyAt(String id, boolean outgoing, long i) {
        int level = 0;
        while (i >= hubBuckets(level)) {
            i -= hubBuckets(level);
            level++;
        }
        return edgeBucketKey(id, outgoing, level, i);
    }

    static long hubBuckets(int level) {
        return ((long) HUB_BASE_BUCKETS) << level;
    }

    /** The number of edge buckets in levels 0 to level. */
    static long bucketsUpTo(int level) {
        return hubBuckets(level + 1) - HUB_BASE_BUCKETS;
    }

    /** FNV-1a 64 of the neighbor id, mod the level's bucket count, as in lpg/store.go. */
    static long hubBucket(String nbr, int level) {
        long h = 0xcbf29ce484222325L;
        for (byte b : nbr.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xff);
            h *= 0x100000001b3L;
        }
        return Long.remainderUnsigned(h, hubBuckets(level));
    }

    private static byte[] sha256(String s) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns the entries of a map read from the server, or none. Key-value results of map range
     * operations come back as a list of entries rather than a map.
     */
    static Iterable<Map.Entry<?, ?>> entries(Object map) {
        if (map instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> m = (Map<Object, Object>) map;
            return new ArrayList<>(m.entrySet());
        }
        if (map instanceof List) {
            List<Map.Entry<?, ?>> out = new ArrayList<>();
            for (Object o : (List<?>) map) {
                if (o instanceof Map.Entry) {
                    out.add((Map.Entry<?, ?>) o);
                }
            }
            return out;
        }
        return Collections.emptyList();
    }

    /** Decodes a stored [created, last_seen, ...] tuple's first two values, or returns null. */
    static long[] tuple(Object v) {
        if (!(v instanceof List) || ((List<?>) v).size() < 2) {
            return null;
        }
        List<?> l = (List<?>) v;
        return new long[]{asLong(l.get(0)), asLong(l.get(1))};
    }

    /** Decodes a stored tuple's third value, its other properties, or returns none. */
    static Map<String, Object> tupleProps(Object v) {
        if (!(v instanceof List) || ((List<?>) v).size() < 3) {
            return Collections.emptyMap();
        }
        Map<String, Object> props = new HashMap<>();
        for (Map.Entry<?, ?> e : entries(((List<?>) v).get(2))) {
            props.put(e.getKey().toString(), e.getValue());
        }
        return props;
    }

    static long asLong(Object o) {
        return o instanceof Number ? ((Number) o).longValue() : 0;
    }

    // ------------------------------------------------------------------- read-only graph

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw Graph.Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "aerospike " + namespace);
    }

    /** An iterator that releases its resources, such as a running scan, when closed. */
    interface CloseableIterator<T> extends org.apache.tinkerpop.gremlin.structure.util.CloseableIterator<T> {
        @Override
        void close();
    }

    /**
     * A small LRU of vertices read in the last 10 seconds, so edges' endpoints aren't read one at
     * a time again. A vertex read without its edges serves only reads that don't need them.
     */
    private static final class VertexCache {
        private final Map<String, AeroVertex> map;

        VertexCache(int size) {
            this.map = Collections.synchronizedMap(new LinkedHashMap<String, AeroVertex>(1024, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, AeroVertex> eldest) {
                    return size() > size;
                }
            });
        }

        AeroVertex get(String id, boolean withEdges) {
            AeroVertex v = map.get(id);
            if (v == null || (withEdges && !v.hasEdges())) {
                return null;
            }
            if (System.currentTimeMillis() - v.loadedAt > 10_000) {
                map.remove(id);
                return null;
            }
            return v;
        }

        void put(AeroVertex v) {
            AeroVertex prev = map.get(v.id);
            if (prev == null || v.hasEdges() || !prev.hasEdges()) {
                map.put(v.id, v);
            }
        }
    }

    // ------------------------------------------------------------------- features

    private static final class AeroFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return false;
                }

                @Override
                public boolean supportsTransactions() {
                    return false;
                }

                @Override
                public boolean supportsThreadedTransactions() {
                    return false;
                }

                @Override
                public VariableFeatures variables() {
                    return new VariableFeatures() {
                        @Override
                        public boolean supportsVariables() {
                            return false;
                        }
                    };
                }
            };
        }

        @Override
        public VertexFeatures vertex() {
            return new VertexFeatures() {
                @Override
                public boolean supportsAddVertices() {
                    return false;
                }

                @Override
                public boolean supportsRemoveVertices() {
                    return false;
                }

                @Override
                public boolean supportsUserSuppliedIds() {
                    return false;
                }

                @Override
                public boolean supportsAddProperty() {
                    return false;
                }

                @Override
                public boolean supportsRemoveProperty() {
                    return false;
                }
            };
        }

        @Override
        public EdgeFeatures edge() {
            return new EdgeFeatures() {
                @Override
                public boolean supportsAddEdges() {
                    return false;
                }

                @Override
                public boolean supportsRemoveEdges() {
                    return false;
                }

                @Override
                public boolean supportsUserSuppliedIds() {
                    return false;
                }

                @Override
                public boolean supportsAddProperty() {
                    return false;
                }

                @Override
                public boolean supportsRemoveProperty() {
                    return false;
                }
            };
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }
}
