package spo.gremlin;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeIdTest {

    @Test
    void plainIdsAreUnchanged() {
        assertEquals("INDIVIDUAL:1|IS_PART_OF|HOUSEHOLD:2", AeroGraph.edgeId("INDIVIDUAL:1", "IS_PART_OF", "HOUSEHOLD:2"));
        assertArrayEquals(new String[]{"INDIVIDUAL:1", "IS_PART_OF", "HOUSEHOLD:2"},
                AeroGraph.parseEdgeId("INDIVIDUAL:1|IS_PART_OF|HOUSEHOLD:2"));
    }

    @Test
    void separatorsAndEscapesRoundTrip() {
        String[][] cases = {
                {"a|b", "L", "c"},
                {"a", "L|M", "c|"},
                {"a\\", "L", "\\|c"},
                {"", "", ""},
                {"|||", "\\\\", "x"},
        };
        for (String[] c : cases) {
            String id = AeroGraph.edgeId(c[0], c[1], c[2]);
            assertArrayEquals(c, AeroGraph.parseEdgeId(id), id);
        }
    }

    @Test
    void malformedIdsAreRejected() {
        assertNull(AeroGraph.parseEdgeId("a|b"));
        assertNull(AeroGraph.parseEdgeId("a|b|c|d"));
        assertNull(AeroGraph.parseEdgeId("abc"));
    }

    @Test
    void labelIndexMustBeReadable() {
        String rw = "ns=test:indexname=idx_vertices_type:set=vertices:bin=type:type=string:indextype=default:context=NULL:state=RW";
        assertTrue(AeroGraph.indexReadable(rw, AeroGraph.LABEL_INDEX));
        assertFalse(AeroGraph.indexReadable(rw.replace("state=RW", "state=WO"), AeroGraph.LABEL_INDEX));
        assertFalse(AeroGraph.indexReadable("ns=test:indexname=other:state=RW", AeroGraph.LABEL_INDEX));
        assertFalse(AeroGraph.indexReadable(null, AeroGraph.LABEL_INDEX));
    }
}
