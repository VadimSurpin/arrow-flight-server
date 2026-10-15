package pro.surpin.data.arrowflight.server;

import com.hazelcast.map.IMap;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiNodeFailoverTest {

    private TestFlightServerHelper helper;
    private FlightSqlClient sqlClient;

    @BeforeAll
    void setUp() throws Exception {
        helper = TestFlightServerHelper.builder().start();
        sqlClient = helper.sqlClient();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (helper != null) helper.close();
    }

    @Test @Order(1)
    void serverRegistered() {
        IMap<String, Long> registry = helper.hazelcastAdapter.serverRegistry();
        String uri = helper.location.getUri().toString();
        assertTrue(registry.containsKey(uri), "Server should be registered");
    }

    @Test @Order(2)
    void serverHasHeartbeat() {
        IMap<String, Long> heartbeats = helper.hazelcastAdapter.serverHeartbeats();
        String uri = helper.location.getUri().toString();
        Long hb = heartbeats.get(uri);
        assertNotNull(hb, "Server should have heartbeat");
        assertTrue(hb > 0);
    }

    @Test @Order(3)
    void queryReturnsEndpoints() throws Exception {
        FlightInfo fi = sqlClient.execute("SELECT * FROM test_schema.test_table");
        Set<String> endpointServers = new HashSet<>();
        for (FlightEndpoint ep : fi.getEndpoints()) {
            for (Location loc : ep.getLocations()) {
                endpointServers.add(loc.getUri().toString());
            }
        }
        assertTrue(endpointServers.size() >= 1,
                "Should have endpoints on at least one server");
    }

    @Test @Order(4)
    void staleHeartbeatServerIsExcluded() throws Exception {
        IMap<String, Long> registry = helper.hazelcastAdapter.serverRegistry();
        IMap<String, Long> heartbeats = helper.hazelcastAdapter.serverHeartbeats();
        String selfUri = helper.location.getUri().toString();

        long now = System.currentTimeMillis();
        heartbeats.put(selfUri, now - 120_000);

        // Cluster filters out servers with heartbeat older than threshold
        Set<String> live = helper.clusterService.filterLiveServers(Set.of(selfUri));
        assertTrue(live.isEmpty(), "Server with stale heartbeat should be excluded");

        // Simulate the node's registration and heartbeat after recovery.
        registry.put(selfUri, 0L);
        heartbeats.put(selfUri, now);
    }

    @Test @Order(5)
    void serverHandlesQueriesAfterHeartbeatRestore() throws Exception {
        FlightInfo fi = sqlClient.execute("SELECT * FROM test_schema.test_table");
        assertNotNull(fi, "Server should handle queries");
        assertFalse(fi.getEndpoints().isEmpty(), "Should have at least one endpoint");
    }

    private static int findFreePort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
