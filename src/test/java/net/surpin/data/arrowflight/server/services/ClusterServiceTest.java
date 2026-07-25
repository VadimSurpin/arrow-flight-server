package net.surpin.data.arrowflight.server.services;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ClusterServiceTest {

    @Mock
    private HazelcastAdapter hazelcast;

    @Mock
    private IMap<String, Long> serverRegistry;

    @Mock
    private IMap<String, Long> serverHeartbeats;

    @Mock
    private IMap<String, Serializable> statementCache;

    @Mock
    private IMap<String, Map<String, Long>> serverFiles;

    private static final AppConfig APP_CONFIG = new AppConfig(
            3, 4096, 1, 131072, 1, 1, 1,
            null, false, null, null,
            "true", "/var/lib/hadoop-hdfs/socket/dn_socket",
            false, 1048576, 67108864, 60000L, "/data/parquet", null, 32010, 5701, 60,
            3, 1000, 30000);

    /**
     * Creates a ClusterService with the required constructor mocks.
     */
    private ClusterService createService(String serverUri) {
        when(hazelcast.serverRegistry()).thenReturn(serverRegistry);
        when(hazelcast.serverHeartbeats()).thenReturn(serverHeartbeats);
        when(hazelcast.statementCache()).thenReturn(statementCache);
        when(hazelcast.serverFiles()).thenReturn(serverFiles);
        return new ClusterService(hazelcast, APP_CONFIG, serverUri);
    }

    // ── filterLiveServers ─────────────────────────────────────────────────

    @Test
    void filterLiveServersKeepsLiveServer() {
        String s1 = "grpc://127.0.0.1:32010";
        long recentHb = System.currentTimeMillis() - 1_000;
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of(s1, recentHb));

        ClusterService cs = createService(s1);
        Set<String> live = cs.filterLiveServers(Set.of(s1));
        assertEquals(Set.of(s1), live);
        cs.close();
    }

    @Test
    void filterLiveServersRemovesExpiredServer() {
        String s1 = "grpc://127.0.0.1:32010";
        long expiredHb = System.currentTimeMillis() - 120_000;
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of(s1, expiredHb));

        ClusterService cs = createService(s1);
        Set<String> live = cs.filterLiveServers(Set.of(s1));
        assertTrue(live.isEmpty(), "Expired server must be removed");
        cs.close();
    }

    @Test
    void filterLiveServersKeepsNewlyRegisteredNoHeartbeat() {
        String s1 = "grpc://127.0.0.1:32010";
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of());

        ClusterService cs = createService(s1);
        Set<String> live = cs.filterLiveServers(Set.of(s1));
        assertEquals(Set.of(s1), live,
                "Newly registered server without heartbeat must be kept");
        cs.close();
    }

    // ── allServerLoads / getLoads ─────────────────────────────────────────

    @Test
    void allServerLoadsReturnsRegistryContents() {
        when(serverRegistry.entrySet())
                .thenReturn(Set.of(Map.entry("s1", 100L), Map.entry("s2", 200L)));

        ClusterService cs = createService("s1");
        Map<String, Long> loads = cs.allServerLoads();
        assertEquals(2, loads.size());
        assertEquals(100L, loads.get("s1"));
        cs.close();
    }

    // ── addLoad ───────────────────────────────────────────────────────────

    @Test
    void addLoadDelegatesToCompute() {
        ClusterService cs = createService("s1");
        cs.addLoad("s2", 50);
        verify(serverRegistry).compute(eq("s2"), any());
        cs.close();
    }

    // ── handle lifecycle ──────────────────────────────────────────────────

    @Test
    void storeAndGetHandle() {
        HandleState state = HandleState.forQuery("SELECT * FROM t");

        ClusterService cs = createService("s1");
        cs.storeHandle("h1", state);

        HandleState retrieved = cs.getHandle("h1");
        assertEquals("SELECT * FROM t", retrieved.query());
        cs.close();
    }

    /** Verifies endpoint state round-trips without a distributed map lookup. */
    @Test
    void signedEndpointHandleRoundTripsLocally() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, false);
        ClusterService cs = createService("s1");

        byte[] handle = cs.createEndpointHandle(state);
        HandleState decoded = cs.resolveEndpointHandle(handle);

        assertEquals(state.query(), decoded.query());
        assertArrayEquals(state.filePaths(), decoded.filePaths());
        assertEquals(state.bytes(), decoded.bytes());
        verify(statementCache, never()).get(anyString());
        cs.close();
    }

    /** Verifies modified self-contained tickets fail authentication. */
    @Test
    void signedEndpointHandleRejectsTampering() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, false);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);
        handle[handle.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class,
                () -> cs.resolveEndpointHandle(handle));
        cs.close();
    }

    /** Verifies every node uses the same Hazelcast-backed ticket signing secret. */
    @Test
    void signedEndpointHandleRoundTripsAcrossClusterServices() {
        AtomicReference<Serializable> sharedSecret = new AtomicReference<>();
        when(statementCache.putIfAbsent(anyString(), any())).thenAnswer(invocation -> {
            Serializable candidate = invocation.getArgument(1);
            Serializable existing = sharedSecret.get();
            if (existing == null && sharedSecret.compareAndSet(null, candidate)) {
                return null;
            }
            return sharedSecret.get();
        });
        ClusterService first = createService("s1");
        ClusterService second = createService("s2");
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s2", 100L, false);

        byte[] handle = first.createEndpointHandle(state);
        HandleState decoded = second.resolveEndpointHandle(handle);

        assertEquals(state.query(), decoded.query());
        assertArrayEquals(state.filePaths(), decoded.filePaths());
        first.close();
        second.close();
    }

    /** Verifies load release is skipped for uniquely owned shards. */
    @Test
    void releaseEndpointLoadSkipsUntrackedShard() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, false);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);

        cs.releaseEndpointLoad(handle, state);

        verify(serverRegistry, never()).compute(eq("s1"), any());
        cs.close();
    }

    /** Verifies retries release tracked endpoint load only once. */
    @Test
    void releaseEndpointLoadIsIdempotent() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, true);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);
        when(statementCache.remove(anyString())).thenReturn(state, null);

        cs.releaseEndpointLoad(handle, state);
        cs.releaseEndpointLoad(handle, state);

        verify(serverRegistry, times(1)).compute(eq("s1"), any());
        cs.close();
    }

    @Test
    void getHandleReturnsNullWhenNotStored() {
        when(statementCache.get("nonexistent")).thenReturn(null);

        ClusterService cs = createService("s1");
        assertNull(cs.getHandle("nonexistent"));
        cs.close();
    }

    @Test
    void removeHandleDeletesFromCache() {
        ClusterService cs = createService("s1");
        cs.removeHandle("h1");
        verify(statementCache).remove("h1");
        cs.close();
    }

    // ── fileLocations ─────────────────────────────────────────────────────

    @Test
    void fileLocationsMergesMultipleServers() {
        when(serverFiles.entrySet()).thenReturn(Set.of(
                Map.entry("s1", Map.of("f1.parquet", 100L)),
                Map.entry("s2", Map.of("f1.parquet", 100L, "f2.parquet", 200L))));

        ClusterService cs = createService("s1");
        Map<String, FileAssignment> result = cs.fileLocations();

        assertEquals(2, result.size());
        assertEquals(100L, result.get("f1.parquet").size());
        assertEquals(2, result.get("f1.parquet").hosts().size());
        assertEquals(1, result.get("f2.parquet").hosts().size());
        cs.close();
    }

    @Test
    void fileLocationsConflictingSizesThrows() {
        when(serverFiles.entrySet()).thenReturn(Set.of(
                Map.entry("s1", Map.of("f1.parquet", 100L)),
                Map.entry("s2", Map.of("f1.parquet", 200L))));

        ClusterService cs = createService("s1");
        assertThrows(IllegalStateException.class, cs::fileLocations);
        cs.close();
    }

    @Test
    void fileLocationsEmptyInventory() {
        when(serverFiles.entrySet()).thenReturn(Set.of());

        ClusterService cs = createService("s1");
        Map<String, FileAssignment> result = cs.fileLocations();
        assertTrue(result.isEmpty());
        cs.close();
    }

    // ── hasFileInventory ──────────────────────────────────────────────────

    @Test
    void hasFileInventoryReturnsTrue() {
        when(serverFiles.containsKey("s1")).thenReturn(true);

        ClusterService cs = createService("s1");
        assertTrue(cs.hasFileInventory("s1"));
        cs.close();
    }

    @Test
    void hasFileInventoryReturnsFalse() {
        when(serverFiles.containsKey("other")).thenReturn(false);

        ClusterService cs = createService("s1");
        assertFalse(cs.hasFileInventory("other"));
        cs.close();
    }

    // ── registerLocalFiles ────────────────────────────────────────────────

    @Test
    void registerLocalFilesPublishesInventory() {
        Map<String, Long> files = new LinkedHashMap<>();
        files.put("dir/f1.parquet", 100L);

        ClusterService cs = createService("s1");
        cs.registerLocalFiles(files);
        verify(serverFiles).put(eq("s1"), anyMap());
        cs.close();
    }

    // ── serverUri ─────────────────────────────────────────────────────────

    @Test
    void serverUriReturnsConstructorArg() {
        ClusterService cs = createService("grpc://127.0.0.1:32010");
        assertEquals("grpc://127.0.0.1:32010", cs.serverUri());
        cs.close();
    }

    // ── close deregistration ──────────────────────────────────────────────

    @Test
    void closeRemovesFromRegistryAndHeartbeats() {
        ClusterService cs = createService("s1");
        cs.close();
        verify(serverRegistry).remove("s1");
        verify(serverHeartbeats).remove("s1");
        verify(hazelcast).close();
    }
}
