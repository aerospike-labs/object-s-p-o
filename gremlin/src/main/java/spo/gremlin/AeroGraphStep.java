package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * {@code g.V()} and {@code g.E()} with the {@code has()} filters that follow them folded in, so
 * they can be answered by key reads: {@code g.V().has('EMAIL_E', value)} reads the identifier
 * index, and {@code g.V().hasLabel('HOUSEHOLD')} reads the type index or scans with a server-side
 * filter on the type. The folded filters are still applied to every element returned.
 *
 * <p>Two flags set by {@link AeroGraphStepStrategy} trim what is read: without edges, vertices
 * are read without their edge bins, when nothing in the traversal reads edges; count only, a
 * scan returns keys alone, for {@code g.V().count()} and {@code g.V().hasLabel(x).count()}.
 */
final class AeroGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    /** Vertex properties that are not identifiers. */
    private static final Set<String> NON_IDENTIFIERS = Set.of("created", "individual_links", "hub");

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private boolean withEdges = true;
    private boolean countOnly;

    @SuppressWarnings("unchecked")
    AeroGraphStep(GraphStep<S, E> original) {
        super(original.getTraversal(), original.getReturnClass(), original.isStartStep(), original.getIds());
        original.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass)
                ? this.vertices() : this.edges()));
    }

    private AeroGraph graph() {
        return (AeroGraph) this.getTraversal().getGraph().get();
    }

    private Iterator<? extends Vertex> vertices() {
        AeroGraph graph = graph();
        if (this.ids != null && this.ids.length > 0) {
            return filter(graph.vertices(withEdges, this.ids));
        }
        String label = null;
        for (HasContainer hc : hasContainers) {
            if (hc.getBiPredicate() != Compare.eq || !(hc.getValue() instanceof String)) {
                continue;
            }
            String key = hc.getKey();
            if (!key.startsWith("~") && !NON_IDENTIFIERS.contains(key)) {
                // An identifier: one bucket read, then the candidates
                return filter(graph.lookupIdentifier(key, (String) hc.getValue(), withEdges));
            }
            if (key.equals(T.label.getAccessor())) {
                label = (String) hc.getValue();
            }
        }
        if (countOnly) {
            // Only label filters, all applied by the scan: keys are enough to count
            return graph.scanVertices(label, AeroGraph.ScanBins.NONE);
        }
        return filter(graph.scanVertices(label, withEdges ? AeroGraph.ScanBins.ALL : AeroGraph.ScanBins.NO_EDGES));
    }

    void omitEdges() {
        withEdges = false;
    }

    /**
     * Whether the folded filters are all equalities to one label, which a scan applies by itself.
     * A scan filters on one label only, so hasLabel('A').hasLabel('B') needs its filters applied.
     */
    boolean onlyLabelFilters() {
        Object label = null;
        for (HasContainer hc : hasContainers) {
            if (!hc.getKey().equals(T.label.getAccessor()) || hc.getBiPredicate() != Compare.eq || !(hc.getValue() instanceof String)) {
                return false;
            }
            if (label != null && !label.equals(hc.getValue())) {
                return false;
            }
            label = hc.getValue();
        }
        return true;
    }

    void countOnly() {
        countOnly = true;
    }

    private Iterator<? extends Edge> edges() {
        return filter(graph().edges(this.ids != null ? this.ids : new Object[0]));
    }

    private <X extends Element> Iterator<X> filter(Iterator<X> it) {
        if (hasContainers.isEmpty()) {
            return it;
        }
        return IteratorUtils.filter(it, e -> HasContainer.testAll(e, hasContainers));
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        hasContainers.add(hasContainer);
    }

    @Override
    public String toString() {
        if (hasContainers.isEmpty() && withEdges && !countOnly) {
            return super.toString();
        }
        String mode = countOnly ? "keys only" : withEdges ? null : "no edge bins";
        return this.ids.length == 0
                ? StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), hasContainers, mode)
                : StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), java.util.Arrays.toString(this.ids), hasContainers, mode);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ hasContainers.hashCode();
    }
}
