package spo.gremlin;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An edge, read from either of its vertices, with created, last_seen and any other properties it
 * was loaded with. Its id is {@code <out id>|<label>|<in id>}, see {@link AeroGraph#edgeId}.
 */
final class AeroEdge implements Edge {

    private final AeroGraph graph;
    final String outId;
    private final String label;
    final String inId;
    private final long created;
    private final long lastSeen;
    private final Map<String, Object> props;

    private AeroEdge(AeroGraph graph, String outId, String label, String inId, long created, long lastSeen, Map<String, Object> props) {
        this.graph = graph;
        this.outId = outId;
        this.label = label;
        this.inId = inId;
        this.created = created;
        this.lastSeen = lastSeen;
        this.props = props;
    }

    /** An edge from its stored tuple, [created, last_seen] or [created, last_seen, {props}]. */
    static AeroEdge of(AeroGraph graph, String outId, String label, String inId, Object tuple) {
        long[] t = AeroGraph.tuple(tuple);
        return new AeroEdge(graph, outId, label, inId, t != null ? t[0] : 0, t != null ? t[1] : 0, AeroGraph.tupleProps(tuple));
    }

    @Override
    public Object id() {
        return AeroGraph.edgeId(outId, label, inId);
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(AeroVertex.ref(graph, outId));
            case IN:
                return IteratorUtils.of(AeroVertex.ref(graph, inId));
            default:
                return IteratorUtils.of(AeroVertex.ref(graph, outId), AeroVertex.ref(graph, inId));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Set<String> want = propertyKeys.length == 0 ? null : new HashSet<>(Arrays.asList(propertyKeys));
        List<Property<V>> out = new ArrayList<>(2);
        if (want == null || want.contains("created")) {
            out.add(new AeroProperty<>(this, "created", (V) Long.valueOf(created)));
        }
        if (want == null || want.contains("last_seen")) {
            out.add(new AeroProperty<>(this, "last_seen", (V) Long.valueOf(lastSeen)));
        }
        for (Map.Entry<String, Object> p : props.entrySet()) {
            if (want == null || want.contains(p.getKey())) {
                out.add(new AeroProperty<>(this, p.getKey(), (V) p.getValue()));
            }
        }
        return out.iterator();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
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
        return StringFactory.edgeString(this);
    }
}
