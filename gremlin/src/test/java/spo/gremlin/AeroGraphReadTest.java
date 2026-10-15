package spo.gremlin;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reads a small graph written in the lpg layout directly, through embedded Gremlin. Runs only
 * with LPG_TEST_HOST=host:port (and LPG_TEST_NAMESPACE, default test), like the Go tests.
 *
 * <p>Fixture, under a per-run prefix: hub H with one inline incoming IS_PART_OF edge (I0,
 * created 5) and 40 bucketed ones (I1..I40, created 10..400, I1 with a src property); hub H2 with
 * 60 outgoing LINKS edges spread over bucket levels 0 and 1.
 */
@EnabledIfEnvironmentVariable(named = "LPG_TEST_HOST", matches = ".+")
class AeroGraphReadTest {

    static String ns;
    static String p;
    static AerospikeClient client;
    static AeroGraph graph;
    static GraphTraversalSource g;

    @BeforeAll
    static void setUp() {
        String[] hp = System.getenv("LPG_TEST_HOST").split(":");
        ns = System.getenv().getOrDefault("LPG_TEST_NAMESPACE", "test");
        p = "jt" + System.nanoTime() + "-";
        client = new AerospikeClient(hp[0], Integer.parseInt(hp[1]));
        WritePolicy create = new WritePolicy();
        create.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        try {
            client.put(create, new Key(ns, "meta", "lpg_format"), new Bin("format", AeroGraph.FORMAT));
            client.put(create, new Key(ns, "meta", "lpg_config"), new Bin("id_buckets", "16777216"));
        } catch (Exception ignored) {
            // already configured
        }

        String h = p + "H";
        put(h, new Bin("type", "HOUSEHOLD"), new Bin("indiv_links", 41), new Bin("hub_in_level", 0));
        putEdge(h, "in_edges", "IS_PART_OF", p + "I0", List.of(5L, 5L));
        put(p + "I0", new Bin("type", "INDIVIDUAL"));
        putEdge(p + "I0", "out_edges", "IS_PART_OF", h, List.of(5L, 5L));
        for (int i = 1; i <= 40; i++) {
            String ind = p + "I" + i;
            List<Object> tuple = i == 1 ? List.of(10L, 10L, Map.of("src", "x")) : List.of(i * 10L, i * 10L);
            Key bucket = bucketKey(h, false, 0, AeroGraph.hubBucket(ind, 0));
            putEdgeAt(bucket, "edges", "IS_PART_OF", ind, tuple);
            put(ind, new Bin("type", "INDIVIDUAL"));
            putEdge(ind, "out_edges", "IS_PART_OF", h, tuple);
        }

        String h2 = p + "H2";
        put(h2, new Bin("type", "ACCOUNT"), new Bin("hub_out_level", 1));
        for (int i = 0; i < 60; i++) {
            String t = p + "T" + i;
            int level = i < 30 ? 0 : 1;
            Key bucket = bucketKey(h2, true, level, AeroGraph.hubBucket(t, level));
            putEdgeAt(bucket, "edges", "LINKS", t, List.of(1000L + i, 1000L + i));
            put(t, new Bin("type", "CREDIT_DEVICE"));
            putEdge(t, "in_edges", "LINKS", h2, List.of(1000L + i, 1000L + i));
        }

        put(p + PIPE_ID, new Bin("type", "ACCOUNT"));
        putEdge(p + PIPE_ID, "out_edges", "LINKS", p + "T0", List.of(7L, 7L));

        graph = AeroGraph.open(System.getenv("LPG_TEST_HOST"), ns);
        g = traversal().withEmbedded(graph);

        // V1 holds an identifier left out of the index, as a writer with -id-index=false leaves it
        put(p + "V1", new Bin("type", "INDIVIDUAL"),
                new Bin("identifiers", Map.of("EMAIL_E", Map.of(p + "mail", List.of(1L, 1L, 0.0)))));
        // V2 holds an identifier whose index tag is full, and isn't among the vertices it keeps
        put(p + "V2", new Bin("type", "INDIVIDUAL"),
                new Bin("identifiers", Map.of("PHONE_E", Map.of(p + "shared", List.of(1L, 1L, 0.0)))));
        long[] slot = AeroGraph.identifierSlot("PHONE_E", p + "shared", graph.idBuckets);
        Map<Value, Value> full = new java.util.HashMap<>();
        for (int i = 0; i < AeroGraph.MAX_IDENTIFIER_SHARE; i++) {
            full.put(Value.get(p + "F" + i), Value.get(1L));
        }
        client.operate(null, new Key(ns, "identifier_index", slot[0]),
                MapOperation.putItems(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT), "vertices_by_tag", full,
                        CTX.mapKeyCreate(Value.get(slot[1]), MapOrder.KEY_ORDERED)));
    }

    /** A vertex id with the edge id separator and escape in it. */
    static final String PIPE_ID = "A|B\\C";

    /** An edge bucket key, as lpg/store.go and AeroGraph.edgeBucketKey build it. */
    private static Key bucketKey(String id, boolean outgoing, int level, long bucket) {
        return new Key(ns, "edge_buckets", id + "|" + (outgoing ? "out" : "in") + "|" + level + "|" + bucket);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (graph != null) {
            graph.close();
        }
        client.close();
    }

    @Test
    void countsAreLocalAndExact() {
        String h = p + "H";
        assertEquals(41L, g.V(h).inE("IS_PART_OF").count().next());
        assertEquals(41L, g.V(h).in("IS_PART_OF").count().next());
        assertEquals(41L, g.V(h).inE().count().next());
        assertEquals(0L, g.V(h).outE().count().next());
        assertEquals(60L, g.V(p + "H2").outE("LINKS").count().next());
        assertTrue(g.V(h).inE("IS_PART_OF").count().explain().toString().contains("AeroLocalCountStep"));
    }

    @Test
    void createdRangesAreSelectedOnTheServer() {
        String h = p + "H";
        assertEquals(20L, g.V(h).inE("IS_PART_OF").has("created", P.gt(200L)).count().next());
        assertEquals(10L, g.V(h).inE("IS_PART_OF").has("created", P.lt(100L)).count().next()); // I0 inline + I1..I9
        assertEquals(1L, g.V(h).inE("IS_PART_OF").has("created", P.eq(250L)).count().next());
        assertEquals(11L, g.V(h).inE("IS_PART_OF").has("created", P.gte(100L)).has("created", P.lte(200L)).count().next());
        for (long t : new long[]{0, 5, 95, 100, 255, 400, 401}) {
            assertEquals(g.V(h).inE("IS_PART_OF").has("last_seen", P.gt(t)).count().next(),
                    g.V(h).inE("IS_PART_OF").has("created", P.gt(t)).count().next(), "gt " + t);
        }
        assertTrue(g.V(h).inE("IS_PART_OF").has("created", P.gt(200L)).explain().toString().contains("AeroVertexStep"));
        assertEquals(15L, g.V(p + "H2").outE("LINKS").has("created", P.gte(1045L)).count().next());
    }

    @Test
    void edgesById() {
        Map<Object, Object> e = g.E(p + "I1|IS_PART_OF|" + p + "H").elementMap().next();
        assertEquals("x", e.get("src"));
        assertEquals(10L, e.get("created"));
        assertEquals(1L, g.E(p + "H2|LINKS|" + p + "T45").count().next()); // bucket level 1
        assertEquals(1L, g.E(p + "H2|LINKS|" + p + "T3").count().next());  // bucket level 0
        assertEquals(0L, g.E(p + "H2|LINKS|" + p + "T99").count().next());
    }

    @Test
    void edgeIdsRoundTripWithSeparatorsInIds() {
        Object id = g.V(p + PIPE_ID).outE("LINKS").id().next();
        assertEquals(List.of(p + PIPE_ID), g.E(id).outV().id().toList());
        assertEquals(List.of(p + "T0"), g.E(id).inV().id().toList());
        assertEquals("LINKS", g.E(id).label().next());
    }

    @Test
    void countWithTwoLabelsAppliesBoth() {
        assertEquals(0L, g.V().hasLabel("HOUSEHOLD").hasLabel("ACCOUNT").count().next());
        assertTrue(g.V().hasLabel("HOUSEHOLD").count().next() >= 1L);
    }

    @Test
    void identifierLookupScansWhenTheIndexIsIncomplete() {
        BaseConfiguration c = new BaseConfiguration();
        c.setProperty("aerospike.hosts", System.getenv("LPG_TEST_HOST"));
        c.setProperty("aerospike.namespace", ns);
        c.setProperty("aerospike.cache.ttl.ms", 0);
        Key off = new Key(ns, "meta", AeroGraph.ID_INDEX_OFF);
        try (AeroGraph fresh = AeroGraph.open(c)) {
            GraphTraversalSource g2 = traversal().withEmbedded(fresh);
            boolean wasOff = client.exists(null, off);
            if (!wasOff) {
                assertEquals(0L, g2.V().has("EMAIL_E", p + "mail").count().next()); // the index is trusted
                client.put(null, off, new Bin("off", true));
            }
            try {
                assertEquals(List.of(p + "V1"), g2.V().has("EMAIL_E", p + "mail").id().toList());
            } finally {
                if (!wasOff) {
                    client.delete(null, off);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void identifierLookupScansWhenATagIsFull() {
        assertEquals(List.of(p + "V2"), g.V().has("PHONE_E", p + "shared").id().toList());
    }

    @Test
    void supernodeProperty() {
        assertEquals(List.of(p + "H"), g.V(p + "H", p + "I0").has(AeroVertex.SUPERNODE).id().toList());
        assertEquals(List.of(p + "I0"), g.V(p + "H", p + "I0").hasNot(AeroVertex.SUPERNODE).id().toList());
    }

    @Test
    void neighborsStreamFromBuckets() {
        assertEquals(5L, g.V(p + "H").in("IS_PART_OF").limit(5).count().next());
        assertEquals(41L, g.V(p + "H").in("IS_PART_OF").dedup().toList().size());
        assertEquals(Arrays.asList("CREDIT_DEVICE"), g.V(p + "H2").out("LINKS").label().dedup().toList());
    }

    private static void put(String id, Bin... bins) {
        WritePolicy wp = new WritePolicy();
        wp.sendKey = true;
        client.put(wp, new Key(ns, "vertices", id), bins);
    }

    private static void putEdge(String id, String bin, String label, String nbr, List<Object> tuple) {
        putEdgeAt(new Key(ns, "vertices", id), bin, label, nbr, tuple);
    }

    private static void putEdgeAt(Key key, String bin, String label, String nbr, List<Object> tuple) {
        WritePolicy wp = new WritePolicy();
        wp.sendKey = true;
        MapPolicy mp = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        client.operate(wp, key,
                MapOperation.put(mp, bin, Value.get(nbr), Value.get(tuple), CTX.mapKeyCreate(Value.get(label), MapOrder.KEY_ORDERED)),
                Operation.add(new Bin("edge_count", 1)));
    }
}
