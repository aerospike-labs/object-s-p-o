package spo.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreatedRangeTest {

    private static CreatedRange of(P<?>... ps) {
        HasContainer[] hcs = new HasContainer[ps.length];
        for (int i = 0; i < ps.length; i++) {
            hcs[i] = new HasContainer("created", ps[i]);
        }
        return CreatedRange.of(Arrays.asList(hcs));
    }

    private static void assertRange(long lo, long hi, CreatedRange r) {
        assertEquals(lo, r.lo, "lo of " + r);
        assertEquals(hi, r.hi, "hi of " + r);
    }

    @Test
    void wholeBounds() {
        assertRange(11, Long.MAX_VALUE, of(P.gt(10L)));
        assertRange(10, Long.MAX_VALUE, of(P.gte(10)));
        assertRange(Long.MIN_VALUE, 10, of(P.lt(10L)));
        assertRange(Long.MIN_VALUE, 11, of(P.lte(10L)));
        assertRange(10, 11, of(P.eq(10L)));
        assertRange(10, 21, of(P.gte(10L), P.lte(20L)));
    }

    @Test
    void fractionalBoundsRoundTowardTheValuesTheyAdmit() {
        assertRange(2, Long.MAX_VALUE, of(P.gt(1.5)));
        assertRange(2, Long.MAX_VALUE, of(P.gte(1.5)));
        assertRange(Long.MIN_VALUE, 2, of(P.lt(1.5)));
        assertRange(Long.MIN_VALUE, 2, of(P.lte(1.5)));
        assertRange(-1, Long.MAX_VALUE, of(P.gt(-1.5)));
        assertTrue(of(P.eq(1.5)).isEmpty());
        assertRange(2, 3, of(P.eq(2.0)));
    }

    @Test
    void boundsAtTheEndsDontOverflow() {
        assertTrue(of(P.gt(Long.MAX_VALUE)).isEmpty());
        assertTrue(of(P.lt(Long.MIN_VALUE)).isEmpty());
        assertRange(Long.MIN_VALUE, Long.MAX_VALUE, of(P.lte(Long.MAX_VALUE)));
        assertTrue(of(P.eq(Long.MAX_VALUE)).isEmpty() || of(P.eq(Long.MAX_VALUE)).lo == Long.MAX_VALUE);
        assertRange(Long.MIN_VALUE, Long.MIN_VALUE + 1, of(P.eq(Long.MIN_VALUE)));
        assertTrue(of(P.gt(1e30)).isEmpty());
    }

    @Test
    void nanIsNotPushedDown() {
        assertNull(of(P.gt(Double.NaN)));
    }

    @Test
    void contradictoryBoundsAreEmpty() {
        assertTrue(of(P.gt(20L), P.lt(10L)).isEmpty());
    }
}
