package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.adapters.HostUtils;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.QueryPlan;
import org.apache.arrow.flight.FlightEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryPlannerTest {

    // ── pickServer ──────────────────────────────────────────────────────────
    //
    // HostUtils.normalize() resolves hostnames via InetAddress.getByName().
    // Without real DNS, arbitrary names resolve to themselves. To test locality
    // matching, file hosts and server URIs must use names that normalize
    // identically — both "127.0.0.1" resolves to "127.0.0.1" regardless of form.

    private static final String S1 = "grpc://127.0.0.1:32010";
    private static final String S2 = "grpc://127.0.0.2:32010";
    private static final String H1 = "127.0.0.1";
    private static final String H2 = "127.0.0.2";

    @Test
    void pickServerPrefersLocal() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 100L);
        load.put(S2, 50L);

        // File blocks on H1 — prefer S1 (has locality)
        String server = QueryPlanner.pickServer(Set.of(H1), load);
        assertEquals(S1, server);
    }

    @Test
    void pickServerFallsBackToGlobalWhenNoLocality() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 200L);
        load.put(S2, 50L);

        // File blocks on unknown host — fallback to least-loaded
        String server = QueryPlanner.pickServer(Set.of("unknown-host"), load);
        assertEquals(S2, server);
    }

    @Test
    void pickServerLeastLoadedAmongMultipleLocals() {
        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 500L);
        load.put(S2, 0L);

        // File blocks on BOTH S1 and S2 — pick lower load
        String server = QueryPlanner.pickServer(Set.of(H1, H2), load);
        assertEquals(S2, server);
    }

    @Test
    void pickServerSingleServer() {
        Map<String, Long> load = Map.of(S1, 0L);
        String server = QueryPlanner.pickServer(Set.of("x"), load);
        assertEquals(S1, server);
    }

    @Test
    void pickServerThrowsOnEmptyLoad() {
        assertThrows(Exception.class, () ->
                QueryPlanner.pickServer(Set.of(H1), Map.of()));
    }

    // ── groupFilesByServer ──────────────────────────────────────────────────

    @Test
    void groupFilesByServerGroupsAllToSingleServer() {
        Map<String, FileAssignment> locations = new LinkedHashMap<>();
        locations.put("f1.parquet", new FileAssignment(100, Set.of(H1)));
        locations.put("f2.parquet", new FileAssignment(200, Set.of(H1)));

        Map<String, Long> load = Map.of(S1, 0L);

        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(locations, load);

        assertEquals(1, result.size());
        assertEquals(2, result.get(S1).size());
    }

    @Test
    void groupFilesByServerDistributesByLocality() {
        Map<String, FileAssignment> locations = new LinkedHashMap<>();
        locations.put("f1.parquet", new FileAssignment(100, Set.of(H1)));
        locations.put("f2.parquet", new FileAssignment(200, Set.of(H2)));

        Map<String, Long> load = new LinkedHashMap<>();
        load.put(S1, 0L);
        load.put(S2, 0L);

        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(locations, load);

        assertEquals(1, result.get(S1).size());
        assertEquals(1, result.get(S2).size());
    }

    @Test
    void groupFilesByServerEmptyInput() {
        Map<String, List<String>> result = QueryPlanner.groupFilesByServer(
                Map.of(), Map.of(S1, 0L));

        assertTrue(result.isEmpty());
    }

    // ── extractTableFromPath ────────────────────────────────────────────────

    @Test
    void extractTableFromUnixPath() {
        assertEquals(".data.db.table", QueryPlanner.extractTableFromPath("/data/db/table/file.parquet"));
    }

    @Test
    void extractTableFromPathSameSchemaSameTable() {
        String t1 = QueryPlanner.extractTableFromPath("/data/s1/t1/file.parquet");
        String t2 = QueryPlanner.extractTableFromPath("/data/s1/t1/other.parquet");

        assertEquals(t1, t2);
    }

    @Test
    void extractTableFromPathDifferentTables() {
        String t1 = QueryPlanner.extractTableFromPath("/data/s1/table_a/f.parquet");
        String t2 = QueryPlanner.extractTableFromPath("/data/s2/table_b/f.parquet");

        assertNotEquals(t1, t2);
    }

    // ── filterForQuery (private static, via reflection) ────────────────────

    @Test
    void filterForQueryKeepsMatchingTableFiles() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("s/t/f1.parquet", new FileAssignment(100, Set.of("H1")));
        inventory.put("other/f2.parquet", new FileAssignment(200, Set.of("H2")));

        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT * FROM s.t");
        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, pq);

        assertEquals(1, result.size());
        assertTrue(result.containsKey("s/t/f1.parquet"));
    }

    @Test
    void filterForQueryExcludesUnrelatedFiles() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("x/y/f.parquet", new FileAssignment(100, Set.of("H1")));

        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT * FROM s.t");
        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, pq);

        assertTrue(result.isEmpty());
    }

    // ── belongsToTable (private static, via reflection) ────────────────────

    @Test
    void belongsToTableWithSchemaMatchesPrefix() throws Exception {
        assertTrue(invokeBelongsToTable("s/t/f.parquet", "s", "t"));
        assertTrue(invokeBelongsToTable("s/t/sub/f.parquet", "s", "t"));
        assertFalse(invokeBelongsToTable("other/t/f.parquet", "s", "t"));
    }

    @Test
    void belongsToTableWithoutSchemaMatchesExtractedTable() throws Exception {
        assertTrue(invokeBelongsToTable("/data/p/q/f.parquet", null, "q"),
                "last segment should match table name");
        assertFalse(invokeBelongsToTable("/data/p/q/f.parquet", null, "x"),
                "unrelated table should not match");
    }

    @Test
    void belongsToTableCaseInsensitive() throws Exception {
        assertTrue(invokeBelongsToTable("Schema/Table/f.parquet", "Schema", "Table"));
    }

    // ── ownsTableShard (private static, via reflection) ────────────────────

    @Test
    void ownsTableShardTrueWhenOwnerAndMatch() throws Exception {
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100, Set.of("H1")));
        assertTrue(invokeOwnsTableShard(files, "H1", "s", "t"));
    }

    @Test
    void ownsTableShardFalseWhenWrongOwner() throws Exception {
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100, Set.of("H1")));
        assertFalse(invokeOwnsTableShard(files, "H2", "s", "t"));
    }

    @Test
    void ownsTableShardFalseWhenWrongTable() throws Exception {
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100, Set.of("H1")));
        assertFalse(invokeOwnsTableShard(files, "H1", "s", "other"));
    }

    // ── createEndpoint (public, needs ClusterService mock) ─────────────────

    @Mock
    private ClusterService clusterService;

    @Mock
    private ParquetAdapter parquetAdapter;

    @Test
    void createEndpointReturnsValidTicket() throws Exception {
        when(parquetAdapter.localFileInventory()).thenReturn(Map.of());
        when(clusterService.createEndpointHandle(any()))
                .thenReturn(new byte[]{1, 2, 3});
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        FlightEndpoint ep = planner.createEndpoint(
                "grpc://127.0.0.1:32010", List.of("s/t/f.parquet"), "SELECT * FROM s.t", 100L);

        assertNotNull(ep);
        assertNotNull(ep.getTicket());
        assertEquals(1, ep.getLocations().size());
        assertEquals("grpc://127.0.0.1:32010",
                ep.getLocations().get(0).getUri().toString());
        verify(clusterService).createEndpointHandle(any());
    }

    /** Verifies repeated scans reuse static file assignments and report statistics. */
    @Test
    void planCachesFilesAndReportsParquetStatistics() throws Exception {
        when(parquetAdapter.localFileInventory()).thenReturn(Map.of());
        when(clusterService.allServerLoads()).thenReturn(Map.of(S1, 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of(S1));
        when(clusterService.hasFileInventory(S1)).thenReturn(true);
        Map<String, FileAssignment> files = Map.of(
                "s/t/f.parquet", new FileAssignment(100L, Set.of(S1)));
        when(clusterService.fileLocations()).thenReturn(files);
        when(clusterService.createEndpointHandle(any()))
                .thenReturn(new byte[]{1, 2, 3});
        when(parquetAdapter.estimateRowCount(anyMap())).thenReturn(25L);
        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);

        QueryPlan first = planner.plan("SELECT * FROM s.t");
        QueryPlan second = planner.plan("SELECT id FROM s.t");

        assertEquals(100L, first.totalBytes());
        assertEquals(25L, first.totalRecords());
        assertEquals(1, second.endpoints().size());
        verify(clusterService, times(1)).fileLocations();
        verify(clusterService, never()).addLoad(anyString(), anyLong());
    }

    @Test
    void determineEndpoints_throwsWhenRegistryEmpty() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of());

        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        assertThrows(Exception.class, () ->
                planner.determineEndpoints("SELECT * FROM s.t"));
    }

    @Test
    void determineEndpoints_throwsWhenNoLiveServers() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of("s1", 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of());

        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        assertThrows(Exception.class, () ->
                planner.determineEndpoints("SELECT * FROM s.t"));
    }

    @Test
    void determineEndpoints_throwsWhenNoInventories() throws Exception {
        when(clusterService.allServerLoads()).thenReturn(Map.of("s1", 0L));
        when(clusterService.filterLiveServers(anySet())).thenReturn(Set.of("s1"));
        when(clusterService.hasFileInventory("s1")).thenReturn(false);

        QueryPlanner planner = new QueryPlanner(parquetAdapter, clusterService);
        assertThrows(Exception.class, () ->
                planner.determineEndpoints("SELECT * FROM s.t"));
    }

    // ── Reflection helpers ─────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static Map<String, FileAssignment> invokeFilterForQuery(
            Map<String, FileAssignment> inventory, ParquetQueryParser query) throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("filterForQuery",
                Map.class, ParquetQueryParser.class);
        m.setAccessible(true);
        return (Map<String, FileAssignment>) m.invoke(null, inventory, query);
    }

    private static boolean invokeBelongsToTable(String path, String schema, String table)
            throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("belongsToTable",
                String.class, String.class, String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, path, schema, table);
    }

    private static boolean invokeOwnsTableShard(Map<String, FileAssignment> files,
            String server, String schema, String table) throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("ownsTableShard",
                Map.class, String.class, String.class, String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, files, server, schema, table);
    }

    @Test
    void filterForQuery_matchesBySchemaTable() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("s/t/f1.parquet", new FileAssignment(100L, Set.of("H1")));
        inventory.put("other/f.parquet", new FileAssignment(200L, Set.of("H1")));

        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, false, "s", "t");
        assertEquals(1, result.size());
        assertTrue(result.containsKey("s/t/f1.parquet"));
    }

    @Test
    void filterForJoin_matchesMultipleTables() throws Exception {
        Map<String, FileAssignment> inventory = new LinkedHashMap<>();
        inventory.put("s/a/f.parquet", new FileAssignment(100L, Set.of("H1")));
        inventory.put("s/b/f.parquet", new FileAssignment(200L, Set.of("H1")));

        Map<String, FileAssignment> result = invokeFilterForQuery(inventory, true, "s", null);
        assertEquals(2, result.size());
    }

    private static Map<String, FileAssignment> invokeFilterForQuery(
            Map<String, FileAssignment> inventory, boolean isJoin,
            String schema, String table) throws Exception {
        Method m = QueryPlanner.class.getDeclaredMethod("filterForQuery",
                Map.class, ParquetQueryParser.class);
        m.setAccessible(true);
        ParquetQueryParser pq;
        if (isJoin) {
            pq = ParquetQueryParser.parse(
                    "SELECT * FROM " + schema + ".a JOIN " + schema + ".b ON a.id = b.id");
        } else {
            pq = ParquetQueryParser.parse("SELECT * FROM " + schema + "." + table);
        }
        @SuppressWarnings("unchecked")
        Map<String, FileAssignment> r = (Map<String, FileAssignment>) m.invoke(null, inventory, pq);
        return r;
    }
}
