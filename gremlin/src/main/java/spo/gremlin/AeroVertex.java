package spo.gremlin;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.MapExp;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A vertex record. Created from a read record, with or without its inline edge bins, or as a
 * reference by id (an edge's endpoint, a counted scan result) that reads its record the first
 * time its label or properties are needed. A vertex read without its edges reads them when its
 * edges are first needed.
 */
final class AeroVertex implements Vertex {

    /**
     * The hidden property that marks a supernode, as AGS's ~supernode: present only on hubs, and
     * only returned when asked for by name, so has('~supernode') and hasNot('~supernode') work.
     */
    static final String SUPERNODE = "~supernode";

    final AeroGraph graph;
    final String id;
    /** Replaced whole, never changed, so threads sharing a cached vertex see a consistent read. */
    private volatile Loaded loaded;

    /** A read of the vertex's record, with or without its edge bins, and when it was made. */
    private static final class Loaded {
        final Record record;
        final boolean withEdges;
        final long at;

        Loaded(Record record, boolean withEdges) {
            this.record = record;
            this.withEdges = withEdges;
            this.at = System.currentTimeMillis();
        }
    }

    AeroVertex(AeroGraph graph, String id, Record record, boolean withEdges) {
        this.graph = graph;
        this.id = id;
        this.loaded = record == null ? null : new Loaded(record, withEdges);
    }

    /** A reference to a vertex by id, read when first needed. */
    static AeroVertex ref(AeroGraph graph, String id) {
        return new AeroVertex(graph, id, null, false);
    }

    boolean hasEdges() {
        Loaded l = loaded;
        return l != null && l.withEdges;
    }

    long loadedAt() {
        Loaded l = loaded;
        return l == null ? 0 : l.at;
    }

    private Record record() {
        Loaded l = loaded;
        return l != null ? l.record : load(false).record;
    }

    /** The record with its inline edge bins, read again if it was read without them. */
    private Record recordWithEdges() {
        Loaded l = loaded;
        return l != null && l.withEdges ? l.record : load(true).record;
    }

    private Loaded load(boolean edges) {
        AeroVertex v = graph.loadVertex(id, edges);
        Loaded l = v != null && v.loaded != null
                ? new Loaded(v.loaded.record, v.loaded.withEdges)
                : new Loaded(new Record(Collections.emptyMap(), 0, 0), true);
        loaded = l;
        return l;
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public String label() {
        Object type = record().getValue(AeroGraph.BIN_TYPE);
        return type != null ? type.toString() : Vertex.DEFAULT_LABEL;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    boolean hasIdentifier(String idType, String value) {
        for (Map.Entry<?, ?> t : AeroGraph.entries(record().getValue(AeroGraph.BIN_IDS))) {
            if (idType.equals(t.getKey())) {
                for (Map.Entry<?, ?> v : AeroGraph.entries(t.getValue())) {
                    if (value.equals(v.getKey())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** The newest edge bucket level of a hub in a direction, or null if it has none there. */
    Integer hubLevel(boolean outgoing) {
        Object l = record().getValue(outgoing ? AeroGraph.BIN_HUB_OUT : AeroGraph.BIN_HUB_IN);
        return l == null ? null : (int) AeroGraph.asLong(l);
    }

    boolean isHub() {
        return hubLevel(true) != null || hubLevel(false) != null;
    }

    /** The number of inline edges in a direction with the given labels (all when null). */
    long inlineCount(boolean outgoing, Set<String> labels) {
        long n = 0;
        for (Map.Entry<?, ?> p : AeroGraph.entries(recordWithEdges().getValue(outgoing ? AeroGraph.BIN_OUT : AeroGraph.BIN_IN))) {
            if (labels == null || labels.contains(p.getKey().toString())) {
                n += p.getValue() instanceof Map ? ((Map<?, ?>) p.getValue()).size() : 0;
            }
        }
        return n;
    }

    // -------------------------------------------------------------- properties

    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Set<String> want = propertyKeys.length == 0 ? null : new HashSet<>(Arrays.asList(propertyKeys));
        List<VertexProperty<V>> out = new ArrayList<>();
        Record r = record();
        if (want == null || want.contains("created")) {
            Object c = r.getValue(AeroGraph.BIN_CREATED);
            if (c != null) {
                out.add(new AeroVertexProperty<>(this, "created", (V) c, null));
            }
        }
        if (want == null || want.contains("individual_links")) {
            Object l = r.getValue(AeroGraph.BIN_LINKS);
            if (l != null) {
                out.add(new AeroVertexProperty<>(this, "individual_links", (V) l, null));
            }
        }
        if (isHub() && (want == null || want.contains("hub"))) {
            out.add(new AeroVertexProperty<>(this, "hub", (V) Boolean.TRUE, null));
        }
        if (want != null && want.contains(SUPERNODE) && isHub()) {
            out.add(new AeroVertexProperty<>(this, SUPERNODE, (V) Boolean.TRUE, null));
        }
        for (Map.Entry<?, ?> t : AeroGraph.entries(r.getValue(AeroGraph.BIN_IDS))) {
            String idType = t.getKey().toString();
            if (want != null && !want.contains(idType)) {
                continue;
            }
            for (Map.Entry<?, ?> v : AeroGraph.entries(t.getValue())) {
                out.add(new AeroVertexProperty<>(this, idType, (V) v.getKey(), v.getValue()));
            }
        }
        return out.iterator();
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public Set<String> keys() {
        Set<String> keys = new HashSet<>();
        properties().forEachRemaining(p -> keys.add(p.key()));
        return keys;
    }

    // ------------------------------------------------------------------- edges

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return IteratorUtils.cast(edgeIterator(direction, labelSet(edgeLabels)));
    }

    static Set<String> labelSet(String... edgeLabels) {
        return edgeLabels.length == 0 ? null : new HashSet<>(Arrays.asList(edgeLabels));
    }

    Iterator<AeroEdge> edgeIterator(Direction direction, Set<String> labels) {
        return edgeIterator(direction, labels, null);
    }

    /**
     * Streams the vertex's edges in a direction: its inline edges, then for a hub its bucketed
     * edges, read a bucket chunk at a time as they are consumed, so a traversal that takes only
     * the first few edges of a hub reads only the first bucket or two. With labels and a created
     * range, each bucket returns only the matching edges, selected on the server. Inline edges
     * are not filtered by the range; the traversal's has() steps still filter every edge.
     */
    Iterator<AeroEdge> edgeIterator(Direction direction, Set<String> labels, CreatedRange range) {
        List<Iterator<AeroEdge>> parts = new ArrayList<>();
        Record r = recordWithEdges();
        if (direction != Direction.IN) {
            parts.add(inlineEdges(r.getValue(AeroGraph.BIN_OUT), true, labels));
            Integer level = hubLevel(true);
            if (level != null) {
                parts.add(new HubEdgeIterator(true, level, labels, range));
            }
        }
        if (direction != Direction.OUT) {
            parts.add(inlineEdges(r.getValue(AeroGraph.BIN_IN), false, labels));
            Integer level = hubLevel(false);
            if (level != null) {
                parts.add(new HubEdgeIterator(false, level, labels, range));
            }
        }
        return IteratorUtils.concat(parts.toArray(new Iterator[0]));
    }

    private Iterator<AeroEdge> inlineEdges(Object map, boolean outgoing, Set<String> labels) {
        List<AeroEdge> out = new ArrayList<>();
        addEdges(out, map, outgoing, labels);
        return out.iterator();
    }

    /** Adds the edges of a {predicate: {neighbor: tuple}} map. */
    private void addEdges(List<AeroEdge> out, Object map, boolean outgoing, Set<String> labels) {
        for (Map.Entry<?, ?> p : AeroGraph.entries(map)) {
            String label = p.getKey().toString();
            if (labels != null && !labels.contains(label)) {
                continue;
            }
            addLabelEdges(out, label, p.getValue(), outgoing);
        }
    }

    /** Adds the edges of one predicate's {neighbor: tuple} map. */
    private void addLabelEdges(List<AeroEdge> out, String label, Object nbrs, boolean outgoing) {
        for (Map.Entry<?, ?> n : AeroGraph.entries(nbrs)) {
            String nbr = n.getKey().toString();
            out.add(outgoing
                    ? AeroEdge.of(graph, id, label, nbr, n.getValue())
                    : AeroEdge.of(graph, nbr, label, id, n.getValue()));
        }
    }

    /**
     * Reads a hub's edge buckets in one direction, level by level, starting small and doubling.
     * Levels fill one after another, so recent edges are mostly in the newest levels: with a lower
     * bound on created, levels are read newest first, so a limit is met after a few buckets.
     */
    private final class HubEdgeIterator implements Iterator<AeroEdge> {
        private final boolean outgoing;
        private final Set<String> labels;
        private final int level;
        private final boolean newestFirst;
        private final long total;
        private final List<String> rangeLabels; // labels read with a server-side range, or null
        private final Operation[] rangeOps;
        private long nextBucket;
        private int chunk = 1;
        private final ArrayDeque<AeroEdge> buffer = new ArrayDeque<>();

        HubEdgeIterator(boolean outgoing, int level, Set<String> labels, CreatedRange range) {
            this.outgoing = outgoing;
            this.labels = labels;
            this.level = level;
            this.newestFirst = range != null && range.lo != Long.MIN_VALUE;
            this.total = AeroGraph.bucketsUpTo(level);
            if (range != null && labels != null) {
                this.rangeLabels = new ArrayList<>(labels);
                this.rangeOps = new Operation[rangeLabels.size()];
                Exp edges = Exp.mapBin(AeroGraph.BIN_BUCKET_EDGES);
                for (int i = 0; i < rangeOps.length; i++) {
                    String label = rangeLabels.get(i);
                    Exp selected = MapExp.getByValueRange(MapReturnType.KEY_VALUE,
                            Exp.val(Collections.singletonList(range.lo)), Exp.val(Collections.singletonList(range.hi)),
                            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP, Exp.val(label), edges));
                    rangeOps[i] = ExpOperation.read("e" + i, Exp.build(Exp.cond(
                            Exp.and(Exp.binExists(AeroGraph.BIN_BUCKET_EDGES),
                                    MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(label), edges)),
                            selected,
                            Exp.val(Collections.emptyMap()))), ExpReadFlags.EVAL_NO_FAIL);
                }
            } else {
                this.rangeLabels = null;
                this.rangeOps = null;
            }
        }

        @Override
        public boolean hasNext() {
            while (buffer.isEmpty() && nextBucket < total) {
                if (nextBucket == 0) {
                    graph.noteSupernodeRead(id, outgoing);
                }
                long end = Math.min(nextBucket + chunk, total);
                Key[] keys = new Key[(int) (end - nextBucket)];
                for (long i = nextBucket; i < end; i++) {
                    keys[(int) (i - nextBucket)] = newestFirst ? newestFirstKey(i) : graph.bucketKeyAt(id, outgoing, i);
                }
                nextBucket = end;
                chunk = Math.min(AeroGraph.HUB_READ_CHUNK, chunk * 2);
                List<AeroEdge> edges = new ArrayList<>();
                if (rangeOps == null) {
                    for (Record rec : graph.client.get(graph.batchPolicy, keys, AeroGraph.BIN_BUCKET_EDGES)) {
                        if (rec != null) {
                            addEdges(edges, rec.getValue(AeroGraph.BIN_BUCKET_EDGES), outgoing, labels);
                        }
                    }
                } else {
                    for (Record rec : graph.client.get(graph.batchPolicy, keys, rangeOps)) {
                        if (rec != null) {
                            for (int i = 0; i < rangeOps.length; i++) {
                                addLabelEdges(edges, rangeLabels.get(i), rec.getValue("e" + i), outgoing);
                            }
                        }
                    }
                }
                buffer.addAll(edges);
            }
            return !buffer.isEmpty();
        }

        /** The key of the i-th bucket counting from the newest level down. */
        private Key newestFirstKey(long i) {
            int l = level;
            while (i >= AeroGraph.hubBuckets(l)) {
                i -= AeroGraph.hubBuckets(l);
                l--;
            }
            return graph.edgeBucketKey(id, outgoing, l, i);
        }

        @Override
        public AeroEdge next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buffer.poll();
        }
    }

    // ---------------------------------------------------------------- vertices

    /** Streams adjacent vertices, batch-reading them a chunk of edges at a time. */
    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Iterator<AeroEdge> edges = edgeIterator(direction, labelSet(edgeLabels));
        return new Iterator<Vertex>() {
            final ArrayDeque<Vertex> buffer = new ArrayDeque<>();

            @Override
            public boolean hasNext() {
                while (buffer.isEmpty() && edges.hasNext()) {
                    List<String> ids = new ArrayList<>(AeroGraph.NEIGHBOR_CHUNK);
                    while (ids.size() < AeroGraph.NEIGHBOR_CHUNK && edges.hasNext()) {
                        AeroEdge e = edges.next();
                        ids.add(e.outId.equals(id) && !e.inId.equals(id) ? e.inId : e.outId);
                    }
                    for (AeroVertex v : graph.loadVertices(ids, true)) {
                        if (v != null) {
                            buffer.add(v);
                        }
                    }
                }
                return !buffer.isEmpty();
            }

            @Override
            public Vertex next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return buffer.poll();
            }
        };
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    public boolean equals(Object o) {
        return ElementHelper.areEqual(this, o);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    /** An identifier or other vertex property, with created/last_seen/decay meta-properties. */
    static final class AeroVertexProperty<V> implements VertexProperty<V> {
        private final AeroVertex vertex;
        private final String key;
        private final V value;
        private final Object meta; // [created, last_seen, decay] for identifiers

        AeroVertexProperty(AeroVertex vertex, String key, V value, Object meta) {
            this.vertex = vertex;
            this.key = key;
            this.value = value;
            this.meta = meta;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public V value() {
            return value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public Vertex element() {
            return vertex;
        }

        @Override
        public Object id() {
            return vertex.id + "|" + key + "|" + value;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <U> Iterator<Property<U>> properties(String... propertyKeys) {
            if (!(meta instanceof List)) {
                return Collections.emptyIterator();
            }
            List<?> l = (List<?>) meta;
            String[] names = {"created", "last_seen", "decay"};
            Set<String> want = propertyKeys.length == 0 ? null : new HashSet<>(Arrays.asList(propertyKeys));
            List<Property<U>> out = new ArrayList<>();
            for (int i = 0; i < names.length && i < l.size(); i++) {
                if (want == null || want.contains(names[i])) {
                    out.add(new AeroProperty<>(this, names[i], (U) l.get(i)));
                }
            }
            return out.iterator();
        }

        @Override
        public <U> Property<U> property(String key, U value) {
            throw Element.Exceptions.propertyAdditionNotSupported();
        }

        @Override
        public void remove() {
            throw Property.Exceptions.propertyRemovalNotSupported();
        }

        @Override
        public boolean equals(Object o) {
            return ElementHelper.areEqual(this, o);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode((Element) this);
        }

        @Override
        public String toString() {
            return StringFactory.propertyString(this);
        }
    }
}
