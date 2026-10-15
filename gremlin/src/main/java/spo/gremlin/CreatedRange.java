package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * A range of edge creation times, [lo, hi), from has('created', ...) filters on edges. Stored
 * edges are lists that start with created, so a map value range from [lo] to [hi] selects
 * exactly these edges on the server.
 */
final class CreatedRange {
    final long lo; // inclusive
    final long hi; // exclusive

    private CreatedRange(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    boolean isEmpty() {
        return lo >= hi;
    }

    /**
     * The range satisfying every has('created', eq|gt|gte|lt|lte number) filter among
     * containers, or null if there is none.
     */
    static CreatedRange of(List<HasContainer> containers) {
        long lo = Long.MIN_VALUE;
        long hi = Long.MAX_VALUE;
        boolean any = false;
        for (HasContainer hc : containers) {
            if (!"created".equals(hc.getKey()) || !(hc.getValue() instanceof Number)) {
                continue;
            }
            long v = ((Number) hc.getValue()).longValue();
            BiPredicate<?, ?> p = hc.getBiPredicate();
            if (p == Compare.eq) {
                lo = Math.max(lo, v);
                hi = Math.min(hi, v + 1);
            } else if (p == Compare.gt) {
                lo = Math.max(lo, v + 1);
            } else if (p == Compare.gte) {
                lo = Math.max(lo, v);
            } else if (p == Compare.lt) {
                hi = Math.min(hi, v);
            } else if (p == Compare.lte) {
                hi = Math.min(hi, v + 1);
            } else {
                continue;
            }
            any = true;
        }
        return any ? new CreatedRange(lo, hi) : null;
    }

    @Override
    public String toString() {
        return "created[" + (lo == Long.MIN_VALUE ? "-inf" : lo) + "," + (hi == Long.MAX_VALUE ? "inf" : hi) + ")";
    }
}
