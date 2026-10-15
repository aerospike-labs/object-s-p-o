package spo.gremlin;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The same values as TestHashGolden in lpg/model_test.go, so both sides find the same buckets. */
class AeroGraphHashTest {

    @Test
    void edgeBucketsMatchGo() {
        String[] ids = {"INDIVIDUAL:1", "HOUSEHOLD:HUB-0", "BENCH-demo-INDIVIDUAL:4522699", "é-ünïcode"};
        long[][] want = {{15, 319, 13119}, {2, 130, 9346}, {6, 6, 24582}, {8, 88, 31320}};
        for (int i = 0; i < ids.length; i++) {
            assertEquals(want[i][0], AeroGraph.hubBucket(ids[i], 0), ids[i]);
            assertEquals(want[i][1], AeroGraph.hubBucket(ids[i], 5), ids[i]);
            assertEquals(want[i][2], AeroGraph.hubBucket(ids[i], 11), ids[i]);
        }
    }

    @Test
    void identifierSlotMatchesGo() throws Exception {
        byte[] h = MessageDigest.getInstance("SHA-256")
                .digest("EMAIL_E\0EMAIL_E_100524_4c9dc9".getBytes(StandardCharsets.UTF_8));
        ByteBuffer bb = ByteBuffer.wrap(h);
        assertEquals(43688L, Long.remainderUnsigned(bb.getLong(0), 65536));
        assertEquals(-8205386307581299651L, bb.getLong(8));
    }
}
