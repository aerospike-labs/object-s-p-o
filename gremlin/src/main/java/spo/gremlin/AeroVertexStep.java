package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Iterator;

/**
 * outE/inE/bothE with labels, followed by has('created', ...) filters: a hub's buckets return only
 * the edges in the created range, selected on the server, as AGS pushes edge property filters
 * down onto supernode reads. The has() steps stay in the traversal and still filter every edge.
 */
final class AeroVertexStep<E extends Element> extends VertexStep<E> {

    private final CreatedRange range;

    AeroVertexStep(Traversal.Admin traversal, Class<E> returnClass, Direction direction, CreatedRange range, String... edgeLabels) {
        super(traversal, returnClass, direction, edgeLabels);
        this.range = range;
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        Vertex v = traverser.get();
        if (!(v instanceof AeroVertex)) {
            return super.flatMap(traverser);
        }
        if (range.isEmpty()) {
            return java.util.Collections.emptyIterator();
        }
        return IteratorUtils.cast(((AeroVertex) v).edgeIterator(getDirection(), AeroVertex.labelSet(getEdgeLabels()), range));
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, getDirection(), Arrays.asList(getEdgeLabels()), range, getReturnClass().getSimpleName().toLowerCase());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ range.toString().hashCode();
    }
}
