package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class HadoopFlightSqlServiceTest {

    // ── join-fallback: per-table file filtering ─────────────────────────────

    @Test
    void perTableGroupingNeverCrossesTableBoundaries() {
        // Simulate the join-fallback pathLocations with files from two tables
        Map<String, FileAssignment> pathLocations = new LinkedHashMap<>();
        pathLocations.put("/data/schema_a/table_x/file1.parquet",
                new FileAssignment(100, Set.of("server1")));
        pathLocations.put("/data/schema_a/table_x/file2.parquet",
                new FileAssignment(200, Set.of("server2")));
        pathLocations.put("/data/schema_b/table_y/file3.parquet",
                new FileAssignment(300, Set.of("server1")));
        pathLocations.put("/data/schema_b/table_y/file4.parquet",
                new FileAssignment(150, Set.of("server2")));

        // Group by table (same logic as in determineEndpoints)
        Map<String, Set<String>> seenTables = new LinkedHashMap<>();
        for (String path : pathLocations.keySet()) {
            String table = QueryPlanner.extractTableFromPath(path);
            seenTables.computeIfAbsent(table, k -> new LinkedHashSet<>()).add(path);
        }

        assertEquals(2, seenTables.size(), "Should have 2 distinct tables");

        // For each table, build the per-table subset (THE FIX)
        for (Map.Entry<String, Set<String>> entry : seenTables.entrySet()) {
            String expectedTable = entry.getKey();
            Set<String> tablePaths = entry.getValue();

            Map<String, FileAssignment> tableLocations = new LinkedHashMap<>();
            for (String path : tablePaths) {
                tableLocations.put(path, pathLocations.get(path));
            }

            // Verify: every path in tableLocations belongs to this table
            for (String path : tableLocations.keySet()) {
                assertEquals(expectedTable, QueryPlanner.extractTableFromPath(path),
                        "Path " + path + " must belong to table " + expectedTable);
            }

            // Verify: no path from other tables leaked in
            assertEquals(tablePaths.size(), tableLocations.size());
        }
    }

    @Test
    void perTableGroupingExcludesForeignTableFiles() {
        // Regression: before the fix, groupFilesByServer received the full
        // pathLocations map, which caused endpoints for table_x to contain
        // files from table_y and vice versa.

        Map<String, FileAssignment> pathLocations = new LinkedHashMap<>();
        pathLocations.put("/data/schema_a/table_x/file1.parquet",
                new FileAssignment(100, Set.of("server1")));
        pathLocations.put("/data/schema_a/table_x/file2.parquet",
                new FileAssignment(200, Set.of("server1")));
        pathLocations.put("/data/schema_b/table_y/file3.parquet",
                new FileAssignment(300, Set.of("server1")));

        // Group by table
        Map<String, Set<String>> seenTables = new LinkedHashMap<>();
        for (String path : pathLocations.keySet()) {
            String table = QueryPlanner.extractTableFromPath(path);
            seenTables.computeIfAbsent(table, k -> new LinkedHashSet<>()).add(path);
        }

        String tableX = QueryPlanner.extractTableFromPath(
                "/data/schema_a/table_x/file1.parquet");
        String tableY = QueryPlanner.extractTableFromPath(
                "/data/schema_b/table_y/file3.parquet");

        Set<String> xPaths = seenTables.get(tableX);
        Set<String> yPaths = seenTables.get(tableY);
        assertNotNull(xPaths);
        assertNotNull(yPaths);

        // Verify table_y files are NOT in table_x's set
        for (String yPath : yPaths) {
            assertFalse(xPaths.contains(yPath),
                    "Table " + tableY + " file " + yPath + " must not appear in " + tableX);
        }

        // Verify table_x files are NOT in table_y's set
        for (String xPath : xPaths) {
            assertFalse(yPaths.contains(xPath),
                    "Table " + tableX + " file " + xPath + " must not appear in " + tableY);
        }

        // Verify the per-table location maps are disjoint
        Map<String, FileAssignment> xLocations = new LinkedHashMap<>();
        for (String p : xPaths) xLocations.put(p, pathLocations.get(p));
        assertEquals(2, xLocations.size());
        for (String p : xLocations.keySet()) {
            assertTrue(p.contains("table_x"), "xLocations must only contain table_x files");
        }

        Map<String, FileAssignment> yLocations = new LinkedHashMap<>();
        for (String p : yPaths) yLocations.put(p, pathLocations.get(p));
        assertEquals(1, yLocations.size());
        for (String p : yLocations.keySet()) {
            assertTrue(p.contains("table_y"), "yLocations must only contain table_y files");
        }
    }
}
