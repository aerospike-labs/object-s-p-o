package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * A range of edge creation times, [lo, hi), from has('created', ...) filters on edges. Stored
 * edges are lists that start with created, so a map value range from [lo] to [hi] selects
 * exactly these edges on the server.
 */
final class CreatedRange {
    private static final BigInteger MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger MAX = BigInteger.valueOf(Long.MAX_VALUE);

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
     * containers, or null if there is none. created is a whole number, so a fractional bound
     * rounds toward the values it admits, and eq of a fraction admits none.
     */
    static CreatedRange of(List<HasContainer> containers) {
        BigInteger lo = MIN;
        BigInteger hi = MAX;
        boolean any = false;
        for (HasContainer hc : containers) {
            if (!"created".equals(hc.getKey()) || !(hc.getValue() instanceof Number)) {
                continue;
            }
            BigDecimal v = decimal((Number) hc.getValue());
            if (v == null) {
                continue;
            }
            BigInteger floor = v.setScale(0, RoundingMode.FLOOR).toBigInteger();
            BigInteger ceil = v.setScale(0, RoundingMode.CEILING).toBigInteger();
            BiPredicate<?, ?> p = hc.getBiPredicate();
            if (p == Compare.eq) {
                if (floor.equals(ceil)) {
                    lo = lo.max(floor);
                    hi = hi.min(floor.add(BigInteger.ONE));
                } else {
                    hi = lo;
                }
            } else if (p == Compare.gt) {
                lo = lo.max(floor.add(BigInteger.ONE));
            } else if (p == Compare.gte) {
                lo = lo.max(ceil);
            } else if (p == Compare.lt) {
                hi = hi.min(ceil);
            } else if (p == Compare.lte) {
                hi = hi.min(floor.add(BigInteger.ONE));
            } else {
                continue;
            }
            any = true;
        }
        if (!any) {
            return null;
        }
        if (lo.compareTo(hi) >= 0 || lo.compareTo(MAX) >= 0 || hi.compareTo(MIN) <= 0) {
            return new CreatedRange(0, 0);
        }
        return new CreatedRange(clamp(lo), clamp(hi));
    }

    /** The exact value of a number, or null for NaN and infinities, which aren't pushed down. */
    private static BigDecimal decimal(Number n) {
        if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
            return BigDecimal.valueOf(n.longValue());
        }
        if (n instanceof BigInteger) {
            return new BigDecimal((BigInteger) n);
        }
        if (n instanceof BigDecimal) {
            return (BigDecimal) n;
        }
        double d = n.doubleValue();
        return Double.isNaN(d) || Double.isInfinite(d) ? null : new BigDecimal(d);
    }

    private static long clamp(BigInteger v) {
        return v.max(MIN).min(MAX).longValue();
    }

    @Override
    public String toString() {
        return "created[" + (lo == Long.MIN_VALUE ? "-inf" : lo) + "," + (hi == Long.MAX_VALUE ? "inf" : hi) + ")";
    }
}
