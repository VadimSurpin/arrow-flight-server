package pro.surpin.data.arrowflight.trino;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises real Trino SQL planning and distributed reads against a Parquet Flight server. */
@Tag("integration")
class TrinoFlightIntegrationTest {
    private static TestFlightServerHelper flight;
    private static DistributedQueryRunner trino;

    /**
     * Starts the Flight server and a local Trino coordinator with one worker.
     *
     * @throws Exception if either service cannot start
     */
    @BeforeAll
    static void startServices() throws Exception {
        flight = TestFlightServerHelper.builder()
                .dataDir(Path.of("../flight-server/src/test/resources/test_db")
                        .toAbsolutePath().normalize().toString())
                .start();
        trino = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("flight").setSchema("test_schema").build())
                .setWorkerCount(1)
                .setCoordinatorProperties(Map.of("node-scheduler.include-coordinator", "false"))
                .build();
        trino.installPlugin(new FlightPlugin());
        trino.createCatalog("flight", "arrow_flight", Map.of(
                "flight.host", "localhost",
                "flight.port", String.valueOf(flight.location.getUri().getPort()),
                "flight.bearer-token", "integration-test",
                "flight.max-retries", "0"));
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
}
