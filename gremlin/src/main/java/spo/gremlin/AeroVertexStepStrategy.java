package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites vertex steps that can be answered with less data:
 * <ul>
 *   <li>a vertex step directly before count() becomes an {@link AeroLocalCountStep}, counting on
 *       the server instead of reading every edge;</li>
 *   <li>outE/inE/bothE with labels followed by has('created', ...) becomes an
 *       {@link AeroVertexStep}, selecting a hub's edges by created on the server.</li>
 * </ul>
 */
final class AeroVertexStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AeroVertexStepStrategy INSTANCE = new AeroVertexStepStrategy();

    private AeroVertexStepStrategy() {
    }

    static AeroVertexStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) {
            return;
        }
        for (VertexStep vs : TraversalHelper.getStepsOfClass(VertexStep.class, traversal)) {
            Step<?, ?> next = vs.getNextStep();
            while (next instanceof NoOpBarrierStep) {
                next = next.getNextStep();
            }
            if (next instanceof CountGlobalStep && vs.getLabels().isEmpty()) {
                TraversalHelper.replaceStep(vs, new AeroLocalCountStep(traversal, vs.getDirection(), vs.getEdgeLabels()), traversal);
                continue;
            }
            if (!vs.returnsEdge() || vs.getEdgeLabels().length == 0) {
                continue;
            }
            List<HasContainer> filters = new ArrayList<>();
            while (next instanceof HasStep) {
                filters.addAll(((HasContainerHolder) next).getHasContainers());
                next = next.getNextStep();
            }
            CreatedRange range = CreatedRange.of(filters);
            if (range != null) {
                AeroVertexStep<?> step = new AeroVertexStep<>(traversal, vs.getReturnClass(), vs.getDirection(), range, vs.getEdgeLabels());
                vs.getLabels().forEach(l -> step.addLabel((String) l));
                TraversalHelper.replaceStep(vs, step, traversal);
            }
        }
    }
}
