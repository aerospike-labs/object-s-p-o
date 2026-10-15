package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * Replaces each GraphStep with an {@link AeroGraphStep} holding the has() filters after it, and
 * trims what it reads: vertices without their edge bins when nothing in the whole traversal reads
 * edges, and keys alone when the vertices are only counted.
 */
final class AeroGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AeroGraphStepStrategy INSTANCE = new AeroGraphStepStrategy();

    private AeroGraphStepStrategy() {
    }

    static AeroGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) {
            return;
        }
        for (GraphStep original : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            AeroGraphStep<?, ?> step = new AeroGraphStep<>(original);
            TraversalHelper.replaceStep(original, step, traversal);
            Step<?, ?> current = step.getNextStep();
            while (current instanceof HasStep || current instanceof NoOpBarrierStep) {
                if (current instanceof HasStep) {
                    for (HasContainer hc : ((HasContainerHolder) current).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(step, hc)) {
                            step.addHasContainer(hc);
                        }
                    }
                    TraversalHelper.copyLabels(current, current.getPreviousStep(), false);
                    traversal.removeStep(current);
                }
                current = current.getNextStep();
            }
            if (!step.returnsVertex()) {
                continue;
            }
            while (current instanceof NoOpBarrierStep) {
                current = current.getNextStep();
            }
            if (current instanceof CountGlobalStep && step.getLabels().isEmpty() && step.getIds().length == 0 && step.onlyLabelFilters()) {
                step.countOnly();
            } else if (!readsEdges(TraversalHelper.getRootTraversal(traversal))) {
                step.omitEdges();
            }
        }
    }

    /**
     * Whether anything in a traversal or its children may read vertices' edges: a vertex step,
     * a local count, or a lambda. A vertex read without its edge bins reads them if they are
     * needed after all, so this only has to be right to save the read.
     */
    private static boolean readsEdges(Traversal.Admin<?, ?> root) {
        return !TraversalHelper.getStepsOfAssignableClassRecursively(root,
                VertexStep.class, AeroLocalCountStep.class, LambdaHolder.class).isEmpty();
    }
}
