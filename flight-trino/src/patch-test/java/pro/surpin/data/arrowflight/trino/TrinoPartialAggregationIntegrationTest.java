package pro.surpin.data.arrowflight.trino;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.trino.client.CoreFlightService;
import pro.surpin.data.arrowflight.trino.config.FlightConfig;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import pro.surpin.data.arrowflight.client.query.Endpoint;

import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** Exercises native Trino final aggregation over real Flight numeric states without separate workers. */
@Tag("integration")
class TrinoPartialAggregationIntegrationTest {
    private static final List<String> QUERIES = Collections.synchronizedList(new ArrayList<>());
    private static TestFlightServerHelper flight;
    private static DistributedQueryRunner trino;
    @TempDir
    private static Path data;

    /**
     * Starts a Flight server and a Trino coordinator that also executes tasks.
     *
     * @throws Exception if the services cannot start
     */
    @BeforeAll
    static void start() throws Exception {
        Path parquet = Files.createDirectories(data.resolve("test_schema/grouped")).resolve("data.parquet");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:"); var statement = connection.createStatement()) {
            statement.execute("COPY (SELECT *, CASE id WHEN 0 THEN 10.0 WHEN 1 THEN 20.0 WHEN 3 THEN 30.0 END::DOUBLE d,"
                    + " CASE id WHEN 0 THEN 9007199254740993::BIGINT WHEN 3 THEN 1 ELSE 0 END AS exact_value"
                    + ", CASE id WHEN 0 THEN 9223372036854775807::BIGINT WHEN 1 THEN 1 ELSE 0 END AS overflow_local"
                    + ", CASE id WHEN 0 THEN 9223372036854775807::BIGINT WHEN 3 THEN 1 ELSE 0 END AS overflow_final"
                    + ", CASE id WHEN 0 THEN 'NaN'::DOUBLE WHEN 1 THEN 'Infinity'::DOUBLE"
                    + " WHEN 3 THEN '-Infinity'::DOUBLE END AS special_d"
                    + " FROM (VALUES (0::BIGINT,1::BIGINT,10::BIGINT),"
                    + "(1,1,NULL),(2,NULL,4),(3,1,9),(4,2,5),(5,NULL,NULL)) t(id,k,v)) TO '"
                    + parquet.toString().replace("'", "''") + "' (FORMAT PARQUET)");
        }
        flight = TestFlightServerHelper.builder().dataDir(data.toString()).start();
        trino = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("flight")
                        .setSchema("test_schema").build())
                .setWorkerCount(0)
                .setCoordinatorProperties(Map.of("node-scheduler.include-coordinator", "true"))
                .build();
        trino.installPlugin(new RecordingPlugin());
        Map<String, String> properties = Map.of(
                "flight.host", "localhost", "flight.port", String.valueOf(flight.location.getUri().getPort()),
                "flight.bearer-token", "integration-test", "flight.max-retries", "0",
                "flight.partition.table", "test_schema.grouped", "flight.partition.predicates", "id < 3;id >= 3");
        trino.createCatalog("flight", "recording_flight", properties);
        trino.createCatalog("empty_flight", "recording_flight", properties);
    }

    /**
     * Closes both services.
     *
     * @throws Exception if service shutdown fails
     */
    @AfterAll
    static void stop() throws Exception {
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

    /**
     * Creates a session with an explicit partial pushdown setting.
     *
     * @param enabled whether pushdown is enabled
     * @param concurrency task concurrency
     * @return query session
     */
    private static Session session(boolean enabled, int concurrency) {
        return Session.builder(trino.getDefaultSession())
                .setCatalogSessionProperty("flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setCatalogSessionProperty("empty_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setSystemProperty("task_concurrency", Integer.toString(concurrency)).build();
    }

    /** Checks global and grouped counts across two real endpoints, including repeated and null keys. */
    @Test
    void mergesPartialCountsWithoutWorkers() {
        for (int concurrency : List.of(1, 4)) {
            Session enabled = session(true, concurrency);
            Session disabled = session(false, concurrency);
            for (String sql : List.of("SELECT count(*) FROM grouped", "SELECT count(v) FROM grouped",
                    "SELECT k, count(*), count(v) FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                    "SELECT k, count(*) FROM grouped WHERE id >= 1 GROUP BY k HAVING count(*) > 1 ORDER BY k NULLS FIRST",
                    "SELECT count(*), count(v) FROM grouped WHERE id > 100",
                    "SELECT k, count(*) FROM grouped WHERE id > 100 GROUP BY k",
                    "SELECT count(*) FROM grouped WHERE id < 0 AND id > 100")) {
                var expected = trino.execute(disabled, sql).getMaterializedRows();
                QUERIES.clear();
                assertEquals(expected, trino.execute(enabled, sql).getMaterializedRows(), sql);
            }
            QUERIES.clear();
            assertEquals(6L, trino.execute(enabled, "SELECT count(*) FROM grouped").getOnlyValue());
            assertEquals(2, QUERIES.stream().filter(sql -> sql.contains("COUNT(*)")).count());
            assertTrue(QUERIES.stream().anyMatch(sql -> sql.contains("id < 3")), QUERIES.toString());
            assertTrue(QUERIES.stream().anyMatch(sql -> sql.contains("id >= 3")), QUERIES.toString());
            assertEquals(4L, trino.execute(enabled, "SELECT count(v) FROM grouped").getOnlyValue());
            String plan = (String) trino.execute(enabled,
                    "EXPLAIN SELECT k, count(*) FROM grouped GROUP BY k").getOnlyValue();
            assertTrue(plan.contains("__flight_count_0"), plan);
            assertTrue(plan.contains("FINAL"), plan);
        }
    }

    /** Checks unsupported aggregates and residual row filters retain the raw scan path. */
    @Test
    void fallsBackWithoutChangingResults() {
        for (String sql : List.of("SELECT count(DISTINCT k) FROM grouped", "SELECT count(*), max(d) FROM grouped",
                "SELECT avg(CAST(v AS DECIMAL(19, 0))) FROM grouped", "SELECT count(*) FROM grouped WHERE id % 2 = 0")) {
            var expected = trino.execute(session(false, 1), sql).getMaterializedRows();
            QUERIES.clear();
            assertEquals(expected, trino.execute(session(true, 1), sql).getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("COUNT(")), QUERIES.toString());
        }
    }

    /** Checks the global zero state when Flight returns no endpoints at all. */
    @Test
    void preservesDefaultsWithoutEndpoints() {
        assertEquals(0L, trino.execute(session(true, 1),
                "SELECT count(*) FROM empty_flight.test_schema.grouped").getOnlyValue());
        assertEquals(0L, trino.execute(session(true, 4),
                "SELECT count(v) FROM empty_flight.test_schema.grouped").getOnlyValue());
        assertEquals(0, trino.execute(session(true, 4),
                "SELECT k, count(*) FROM empty_flight.test_schema.grouped GROUP BY k").getRowCount());
        for (String function : List.of("min(v)", "max(v)", "sum(v)", "sum(d)", "avg(v)", "avg(d)")) {
            assertNull(trino.execute(session(true, 4),
                    "SELECT " + function + " FROM empty_flight.test_schema.grouped").getOnlyValue(), function);
        }
    }

    /** Checks numeric states merge across unequal endpoint counts and preserve exact integer sums. */
    @Test
    void mergesNumericStates() {
        for (int concurrency : List.of(1, 4)) {
            for (String sql : List.of(
                    "SELECT min(v), max(v), sum(v), sum(d), avg(v), avg(d), count(d) FROM grouped",
                    "SELECT k, min(v), max(v), sum(v), sum(d), avg(v), avg(d), count(*) FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                    "SELECT k, avg(d) FROM grouped GROUP BY k HAVING avg(d) > 15 ORDER BY k",
                    "SELECT min(v), max(v), sum(v), sum(d), avg(v), avg(d), count(*) FROM grouped WHERE id > 100",
                    "SELECT min(v), max(v), sum(v), sum(d), avg(v), avg(d), count(*) FROM grouped WHERE id < 0 AND id > 100",
                    "SELECT k, avg(d) FROM grouped WHERE id > 100 GROUP BY k",
                    "SELECT sum(exact_value) FROM grouped")) {
                var expected = trino.execute(session(false, concurrency), sql).getMaterializedRows();
                QUERIES.clear();
                assertEquals(expected, trino.execute(session(true, concurrency), sql).getMaterializedRows(), sql);
            }
            QUERIES.clear();
            assertEquals(20.0, trino.execute(session(true, concurrency), "SELECT avg(d) FROM grouped").getOnlyValue());
            assertEquals(2, QUERIES.stream().filter(sql -> sql.contains("SUM(d)") && sql.contains("COUNT(d)")).count(), QUERIES.toString());
            assertEquals(9007199254740994L,
                    trino.execute(session(true, concurrency), "SELECT sum(exact_value) FROM grouped").getOnlyValue());
            String plan = (String) trino.execute(session(true, concurrency),
                    "EXPLAIN SELECT k, min(v), max(v), sum(v), avg(d) FROM grouped GROUP BY k").getOnlyValue();
            for (String state : List.of("__flight_min_", "__flight_max_", "__flight_sum_", "__flight_avg_", "FINAL")) {
                assertTrue(plan.contains(state), plan);
            }
        }
    }

    /** Checks non-finite DOUBLE SUM/AVG preserve the baseline result. */
    @Test
    void preservesNonFiniteSumsAndAverages() {
        for (String filter : List.of("", " WHERE id = 0", " WHERE id = 1", " WHERE id = 3", " WHERE id IN (1, 3)")) {
            String sql = "SELECT sum(special_d), avg(special_d) FROM grouped" + filter;
            var expected = trino.execute(session(false, 1), sql).getMaterializedRows();
            QUERIES.clear();
            assertEquals(expected, trino.execute(session(true, 1), sql).getMaterializedRows(), sql);
            assertTrue(QUERIES.stream().anyMatch(query -> query.contains("SUM(special_d)")), QUERIES.toString());
        }
    }

    /** Checks both local partial overflow and overflow during native final merging fail. */
    @Test
    void rejectsIntegralSumOverflow() {
        for (String column : List.of("overflow_local", "overflow_final")) {
            String sql = "SELECT sum(" + column + ") FROM grouped";
            assertThrows(RuntimeException.class, () -> trino.execute(session(false, 1), sql));
            QUERIES.clear();
            assertThrows(RuntimeException.class, () -> trino.execute(session(true, 1), sql));
            assertTrue(QUERIES.stream().anyMatch(query -> query.contains("SUM(CAST(" + column)), QUERIES.toString());
        }
    }

    /** Registers a connector that records the SQL sent to Flight. */
    private static final class RecordingPlugin implements Plugin {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories() {
            return List.of(new RecordingFactory());
        }
    }

    /** Creates real Flight connectors with recorded query planning. */
    private static final class RecordingFactory implements ConnectorFactory {
        @Override
        public String getName() {
            return "recording_flight";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context) {
            FlightConfig config = FlightConfig.fromProperties(properties);
            CoreFlightService service = spy(new CoreFlightService(config.toClientConfiguration()));
            doAnswer(invocation -> {
                QUERIES.add(invocation.getArgument(0));
                if (catalogName.equals("empty_flight")) {
                    return new QueryEndpoints(null, new Endpoint[0]);
                }
                return invocation.callRealMethod();
            }).when(service).plan(anyString());
            return new FlightConnector(service, config.columnQuote(), config.partitioning());
        }
    }
}
