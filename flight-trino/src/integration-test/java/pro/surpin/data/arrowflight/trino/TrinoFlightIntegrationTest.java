package pro.surpin.data.arrowflight.trino;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises real Trino SQL planning and distributed reads against a Parquet Flight server. */
@Tag("integration")
class TrinoFlightIntegrationTest {
    private static TestFlightServerHelper flight;
    private static DistributedQueryRunner trino;
    @org.junit.jupiter.api.io.TempDir
    private static Path data;

    /**
     * Starts the Flight server and a local Trino coordinator with one worker.
     *
     * @throws Exception if either service cannot start
     */
    @BeforeAll
    static void startServices() throws Exception {
        Path original = Path.of("../flight-server/src/test/resources/test_db").toAbsolutePath().normalize();
        try (var paths = Files.walk(original)) {
            for (Path path : paths.toList()) {
                Path target = data.resolve(original.relativize(path));
                if (Files.isDirectory(path)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(path, target);
                }
            }
        }
        Path nested = Files.createDirectories(data.resolve("test_schema/partitioned_table")).resolve("part.parquet");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:"); var statement = connection.createStatement()) {
            statement.execute("""
                    COPY (SELECT * FROM (VALUES
                      (-5, 'first'), (5, ''), (15, NULL), (NULL, 'last')
                    ) t(id, label)) TO '
                    """.stripTrailing() + nested.toString().replace("'", "''") + "' (FORMAT PARQUET)");
        }
        flight = TestFlightServerHelper.builder()
                .dataDir(data.toString())
                .start();
        trino = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("flight").setSchema("test_schema").build())
                .setWorkerCount(1)
                .setCoordinatorProperties(Map.of("node-scheduler.include-coordinator", "false"))
                .build();
        trino.installPlugin(new FlightPlugin());
        Map<String, String> connection = Map.of(
                "flight.host", "localhost",
                "flight.port", String.valueOf(flight.location.getUri().getPort()),
                "flight.bearer-token", "integration-test",
                "flight.max-retries", "0");
        trino.createCatalog("flight", "arrow_flight", connection);
        for (String mode : List.of("range", "hash", "explicit")) {
            Map<String, String> properties = new HashMap<>(connection);
            properties.put("flight.partition.table", "test_schema.partitioned_table");
            properties.put("flight.partition.size", "3");
            if (mode.equals("explicit")) {
                properties.put("flight.partition.predicates", "id < 10 OR id IS NULL;id >= 10");
            } else {
                properties.put("flight.partition.by-column", "id");
                if (mode.equals("range")) {
                    properties.put("flight.partition.lower-bound", "0");
                    properties.put("flight.partition.upper-bound", "10");
                }
            }
            trino.createCatalog("flight_" + mode, "arrow_flight", properties);
        }
    }

    /**
     * Closes both services even when a test or setup failed.
     *
     * @throws Exception if service shutdown fails
     */
    @AfterAll
    static void stopServices() throws Exception {
        try {
            if (trino != null) {
                trino.close();
            }
        } finally {
            if (flight != null) {
                flight.close();
            }
        }
    }

    /** Checks Flight metadata through Trino SQL statements. */
    @Test
    void exposesSchemasTablesAndColumns() {
        assertTrue(trino.execute("SHOW SCHEMAS FROM flight").getOnlyColumnAsSet().contains("test_schema"));
        assertTrue(trino.execute("SHOW TABLES FROM flight.test_schema")
                .getOnlyColumnAsSet().contains("test_table"));
        MaterializedResult columns = trino.execute("DESCRIBE flight.test_schema.test_table");
        assertTrue(columns.getMaterializedRows().stream()
                .anyMatch(row -> row.getField(0).equals("id") && row.getField(1).equals("integer")));
    }

    /** Checks full scans, projection, filters, empty scans and Trino aggregation. */
    @Test
    void executesSqlOnWorker() {
        assertEquals(1000, trino.execute("SELECT * FROM test_table").getRowCount());
        assertEquals(1000L, trino.execute("SELECT count(*) FROM test_table").getOnlyValue());
        MaterializedResult filtered = trino.execute(
                "SELECT id, string_col FROM test_table WHERE id > 100 ORDER BY id");
        assertEquals(899, filtered.getRowCount());
        assertEquals(101, filtered.getMaterializedRows().getFirst().getField(0));
        assertEquals(0, trino.execute("SELECT id FROM test_table WHERE id < 0 AND id > 1000")
                .getRowCount());
        assertEquals(100L, trino.execute("SELECT count(*) FROM test_table WHERE tinyint_col = 0")
                .getOnlyValue());
    }

    /** Checks that Flight row estimates are available to the real optimizer. */
    @Test
    void reportsStatisticsAndExplainsQuery() {
        MaterializedResult stats = trino.execute("SHOW STATS FOR test_table");
        assertTrue(stats.getMaterializedRows().stream()
                .anyMatch(row -> row.getField(0) == null && Double.valueOf(1000).equals(row.getField(4))));
        String plan = (String) trino.execute("EXPLAIN SELECT id FROM test_table WHERE id > 100")
                .getOnlyValue();
        assertTrue(plan.contains("Scan"), plan);
        assertTrue(plan.contains("test_table"), plan);
    }

    /** Checks scalar projection and nulls through Parquet, Flight and a real Trino worker. */
    @Test
    void readsScalarTypesOnWorker() {
        MaterializedResult result = trino.execute("SELECT label FROM partitioned_table WHERE id = -5");
        assertEquals("first", result.getOnlyValue());
        assertEquals(1L, trino.execute("SELECT count(*) FROM partitioned_table WHERE label IS NULL").getOnlyValue());
        assertEquals("", trino.execute("SELECT label FROM partitioned_table WHERE id = 5").getOnlyValue());
    }

    /** Checks partitioned reads equal the baseline including nulls, outliers, filters and projections. */
    @Test
    void partitionedReadsPreserveResults() {
        for (String mode : List.of("range", "hash", "explicit")) {
            String table = "flight_" + mode + ".test_schema.partitioned_table";
            assertEquals(trino.execute("SELECT * FROM partitioned_table ORDER BY id NULLS LAST").getMaterializedRows(),
                    trino.execute("SELECT * FROM " + table + " ORDER BY id NULLS LAST").getMaterializedRows());
            assertEquals(4L, trino.execute("SELECT count(*) FROM " + table).getOnlyValue());
            assertEquals(trino.execute("SELECT label FROM partitioned_table WHERE id >= 5 ORDER BY id").getMaterializedRows(),
                    trino.execute("SELECT label FROM " + table + " WHERE id >= 5 ORDER BY id").getMaterializedRows());
            assertEquals(1000L, trino.execute("SELECT count(*) FROM flight_" + mode + ".test_schema.test_table").getOnlyValue());
        }
    }
}
