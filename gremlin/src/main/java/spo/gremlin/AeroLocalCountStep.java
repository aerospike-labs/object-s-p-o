package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Set;

/**
 * Replaces out/in/both/outE/inE/bothE directly before count(): rather than reading each edge,
 * it counts a vertex's edges on the server ({@link AeroGraph#degree}) and passes the vertex on
 * once with that bulk, which the count() step sums as if each edge had arrived. AGS's local
 * counting, for hubs as well as inline adjacency.
 */
final class AeroLocalCountStep extends AbstractStep<Vertex, Vertex> {

    private final Direction direction;
    private final String[] edgeLabels;
    private final Set<String> labels;

    AeroLocalCountStep(Traversal.Admin traversal, Direction direction, String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.labels = AeroVertex.labelSet(edgeLabels);
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        while (true) {
            Traverser.Admin<Vertex> t = this.starts.next();
            Vertex v = t.get();
            long degree = v instanceof AeroVertex
                    ? ((AeroVertex) v).graph.degree((AeroVertex) v, direction, labels)
                    : IteratorUtils.count(v.edges(direction, edgeLabels));
            if (degree == 0) {
                continue;
            }
            Traverser.Admin<Vertex> counted = t.split();
            counted.setBulk(t.bulk() * degree);
            return counted;
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, direction, Arrays.asList(edgeLabels));
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ direction.hashCode() ^ Arrays.hashCode(edgeLabels);
    }
}
