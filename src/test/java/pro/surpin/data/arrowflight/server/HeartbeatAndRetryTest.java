package pro.surpin.data.arrowflight.server;

import com.hazelcast.map.IMap;
import org.junit.jupiter.api.*;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HeartbeatAndRetryTest {

    private TestFlightServerHelper helper;
    private IMap<String, Long> heartbeats;
    private IMap<String, Long> registry;
    private String selfUri;

    @BeforeAll
    void setUp() throws Exception {
        helper = TestFlightServerHelper.builder().start();
        selfUri = helper.location.getUri().toString();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (helper != null) helper.close();
    }

    @BeforeEach
    void initMaps() {
        heartbeats = helper.hazelcastAdapter.serverHeartbeats();
        registry = helper.hazelcastAdapter.serverRegistry();
        // Re-register self — may have been cleared by a previous test
        registry.put(selfUri, 0L);
        heartbeats.put(selfUri, System.currentTimeMillis());
    }

    @AfterEach
    void clearTestEntries() {
        // Remove all entries except self-registration
        for (String key : registry.keySet().toArray(new String[0])) {
            if (!selfUri.equals(key)) {
                registry.remove(key);
            }
        }
        for (String key : heartbeats.keySet().toArray(new String[0])) {
            if (!selfUri.equals(key)) {
                heartbeats.remove(key);
            }
        }
    }

    @Test
    void selfRegisteredAndHasHeartbeat() {
        assertTrue(registry.size() >= 1, "At least self should be registered");
        Long hb = heartbeats.get(selfUri);
        assertNotNull(hb, "Self should have heartbeat");
        assertTrue(hb > 0, "Heartbeat timestamp should be positive");
    }

    @Test
    void liveServerPassesFilter() {
        String liveUri = "grpc+tcp://server1:32010";
        registry.put(liveUri, 0L);
        heartbeats.put(liveUri, System.currentTimeMillis());

        Set<String> input = Set.of(liveUri);
        Set<String> result = helper.clusterService.filterLiveServers(input);

        assertEquals(1, result.size());
        assertTrue(result.contains(liveUri));
    }

    @Test
    void staleServerIsRemoved() {
        String staleUri = "grpc+tcp://dead-server:32010";
        registry.put(staleUri, 100L);
        // Heartbeat from 120 seconds ago — well past the 45s timeout
        heartbeats.put(staleUri, System.currentTimeMillis() - 120_000);

        Set<String> input = Set.of(staleUri);
        Set<String> result = helper.clusterService.filterLiveServers(input);

        assertTrue(result.isEmpty(), "Stale server should be filtered out");
        assertNull(registry.get(staleUri), "Stale server should be removed from registry");
        assertNull(heartbeats.get(staleUri), "Stale server should be removed from heartbeats");
    }

    @Test
    void noHeartbeatYetIsConsideredLive() {
        String newUri = "grpc+tcp://just-joined:32010";
        registry.put(newUri, 0L);
        // No heartbeat at all — just registered, should be treated as live

        Set<String> input = Set.of(newUri);
        Set<String> result = helper.clusterService.filterLiveServers(input);

        assertEquals(1, result.size());
        assertTrue(result.contains(newUri));
    }

    @Test
    void mixedLiveAndStaleServers() {
        String live1 = "grpc+tcp://live1:32010";
        String live2 = "grpc+tcp://live2:32010";
        String dead1 = "grpc+tcp://dead1:32010";
        String dead2 = "grpc+tcp://dead2:32010";

        registry.put(live1, 50L);
        registry.put(live2, 200L);
        registry.put(dead1, 100L);
        registry.put(dead2, 300L);

        heartbeats.put(live1, System.currentTimeMillis());
        heartbeats.put(live2, System.currentTimeMillis());
        heartbeats.put(dead1, System.currentTimeMillis() - 120_000);
        heartbeats.put(dead2, System.currentTimeMillis() - 60_000);

        Set<String> input = new LinkedHashSet<>();
        input.add(live1);
        input.add(dead1);
        input.add(live2);
        input.add(dead2);

        Set<String> result = helper.clusterService.filterLiveServers(input);

        assertEquals(2, result.size());
        assertTrue(result.contains(live1));
        assertTrue(result.contains(live2));
        assertFalse(result.contains(dead1));
        assertFalse(result.contains(dead2));

        assertNull(registry.get(dead1), "Dead server should be removed from registry");
        assertNull(registry.get(dead2), "Dead server should be removed from registry");
    }

    @Test
    void emptyInputReturnsEmpty() {
        Set<String> result = helper.clusterService.filterLiveServers(Set.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void heartbeatUpdatesPeriodically() throws Exception {
        Long firstHb = heartbeats.get(selfUri);
        assertNotNull(firstHb);

        registry.remove(selfUri);

        // Wait for one heartbeat cycle (15s interval)
        Thread.sleep(16_000);

        Long secondHb = heartbeats.get(selfUri);
        assertNotNull(secondHb);
        assertTrue(secondHb > firstHb,
                "Heartbeat should have been updated. First: " + firstHb + ", Second: " + secondHb);
        assertTrue(registry.containsKey(selfUri),
                "Heartbeat should re-register a server that recovered");
    }
}
