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
import java.math.BigDecimal;
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

/** Exercises native Trino final aggregation over real Flight states without separate workers. */
@Tag("integration")
class TrinoPartialAggregationIntegrationTest {
    private static final List<String> QUERIES = Collections.synchronizedList(new ArrayList<>());
    private static final java.util.concurrent.atomic.AtomicInteger FAILURE_BATCHES = new java.util.concurrent.atomic.AtomicInteger();
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
                    + ", CASE id WHEN 0 THEN 'NaN'::FLOAT WHEN 1 THEN 'Infinity'::FLOAT"
                    + " WHEN 3 THEN '-Infinity'::FLOAT END AS special_r"
                    + ", CASE id WHEN 0 THEN 'NaN'::DOUBLE WHEN 1 THEN 4.0 WHEN 3 THEN 20.0"
                    + " WHEN 4 THEN 'NaN'::DOUBLE END AS mixed_d"
                    + ", CASE id WHEN 0 THEN 'NaN'::FLOAT WHEN 1 THEN 4.0::FLOAT WHEN 3 THEN 20.0::FLOAT"
                    + " WHEN 4 THEN 'NaN'::FLOAT END AS mixed_r"
                    + ", CASE id WHEN 0 THEN '3.4028234663852886e38'::FLOAT WHEN 1 THEN '3.4028234663852886e38'::FLOAT"
                    + " WHEN 3 THEN '-3.4028234663852886e38'::FLOAT END AS range_r"
                    + ", CASE id WHEN 0 THEN true WHEN 1 THEN false WHEN 3 THEN true END AS bool_v"
                    + ", CASE id WHEN 0 THEN 'a ' WHEN 1 THEN 'a' WHEN 2 THEN '𝄞'"
                    + " WHEN 3 THEN 'a ' WHEN 4 THEN 'ä' END AS text_v"
                    + ", CASE id WHEN 0 THEN '00000000-0000-0000-0000-000000000000'::UUID"
                    + " WHEN 1 THEN '7fffffff-ffff-ffff-ffff-ffffffffffff'::UUID"
                    + " WHEN 3 THEN '80000000-0000-0000-0000-000000000000'::UUID"
                    + " WHEN 4 THEN 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID END::VARCHAR AS uuid_v"
                    + ", CASE id WHEN 0 THEN '-0.0'::DOUBLE WHEN 1 THEN '0.0'::DOUBLE"
                    + " WHEN 3 THEN '-0.0'::DOUBLE END AS zero_d"
                    + ", CASE id WHEN 0 THEN '-0.0'::FLOAT WHEN 1 THEN '0.0'::FLOAT"
                    + " WHEN 3 THEN '-0.0'::FLOAT END AS zero_r"
                    + ", CASE id WHEN 0 THEN '12345.67'::DECIMAL(9,2)"
                    + " WHEN 1 THEN '-0.01'::DECIMAL(9,2) WHEN 3 THEN '0.00'::DECIMAL(9,2) END AS decimal9"
                    + ", CASE id WHEN 0 THEN '90071992547409.93'::DECIMAL(18,2)"
                    + " WHEN 1 THEN '-0.01'::DECIMAL(18,2) WHEN 3 THEN '90071992547409.94'::DECIMAL(18,2) END AS decimal18"
                    + ", CASE id WHEN 0 THEN '-9999999999999999.999'::DECIMAL(19,3)"
                    + " WHEN 1 THEN '0.001'::DECIMAL(19,3) WHEN 3 THEN '9999999999999999.999'::DECIMAL(19,3) END AS decimal19"
                    + ", CASE id WHEN 0 THEN '-99999999999999999999.999999999999999999'::DECIMAL(38,18)"
                    + " WHEN 1 THEN '0.000000000000000001'::DECIMAL(38,18)"
                    + " WHEN 3 THEN '99999999999999999999.999999999999999999'::DECIMAL(38,18) END AS decimal38"
                    + ", CASE id WHEN 0 THEN '0.99999999999999999999999999999999999999'::DECIMAL(38,38)"
                    + " WHEN 1 THEN '-0.00000000000000000000000000000000000001'::DECIMAL(38,38)"
                    + " WHEN 3 THEN '0'::DECIMAL(38,38) END AS decimal_scale38"
                    + ", CASE WHEN id IN (0,1,3) THEN 18446744073709551615::UBIGINT END AS unsigned_decimal"
                    + ", CASE WHEN id IN (0,1,3) THEN DATE '1969-12-31' + id::INTEGER END AS date_v"
                    + ", CASE WHEN id IN (0,1,3) THEN '1969-12-31 23:59:59.123'::TIMESTAMP_MS END AS timestamp_ms"
                    + ", CASE WHEN id IN (0,1,3) THEN '2026-11-14 12:34:56.123456'::TIMESTAMP END AS timestamp_us"
                    + ", CASE WHEN id IN (0,1,3) THEN '2026-11-14 12:34:56.123456789'::TIMESTAMP_NS END AS timestamp_ns"
                    + " FROM (VALUES (0::BIGINT,1::BIGINT,10::BIGINT),"
                    + "(1,1,NULL),(2,NULL,4),(3,1,9),(4,2,5),(5,NULL,NULL)) t(id,k,v)) TO '"
                    + parquet.toString().replace("'", "''") + "' (FORMAT PARQUET)");
            Path exact = Files.createDirectories(data.resolve("test_schema/exact")).resolve("data.parquet");
            String bound = "9".repeat(38);
            String scaled = "9".repeat(20) + "." + "9".repeat(18);
            statement.execute("COPY (SELECT id::BIGINT id, 1::BIGINT k, (CASE WHEN id < 3 THEN '" + bound
                    + "' ELSE '-" + bound + "' END)::DECIMAL(38,0) wide, (CASE WHEN id < 3 THEN '" + scaled
                    + "' ELSE '-" + scaled + "' END)::DECIMAL(38,18) scaled,"
                    + " (CASE id WHEN 0 THEN -1 WHEN 3 THEN 0 END)::DECIMAL(3,0) rounding,"
                    + " (CASE id WHEN 0 THEN 0.01 WHEN 3 THEN 0.02 END)::DECIMAL(5,2) rounding_scale"
                    + " FROM range(6) t(id)) TO '" + exact.toString().replace("'", "''") + "' (FORMAT PARQUET)");
        }
        writeLogicalTypeTable();
        writeRandomizedTable();
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
        trino.createCatalog("failing_flight", "recording_flight", properties);
        Map<String, String> singleEndpoint = new java.util.HashMap<>(properties);
        singleEndpoint.remove("flight.partition.table");
        singleEndpoint.remove("flight.partition.predicates");
        trino.createCatalog("single_flight", "recording_flight", singleEndpoint);
        Map<String, String> exactEndpoints = new java.util.HashMap<>(properties);
        exactEndpoints.put("flight.partition.table", "test_schema.exact");
        trino.createCatalog("exact_flight", "recording_flight", exactEndpoints);
        Map<String, String> compositeEndpoints = new java.util.HashMap<>(properties);
        compositeEndpoints.put("flight.partition.table", "test_schema.composite");
        trino.createCatalog("composite_flight", "recording_flight", compositeEndpoints);
        Map<String, String> logicalEndpoints = new java.util.HashMap<>(properties);
        logicalEndpoints.put("flight.partition.table", "test_schema.logical");
        trino.createCatalog("logical_flight", "recording_flight", logicalEndpoints);
        Map<String, String> randomizedEndpoints = new java.util.HashMap<>(properties);
        randomizedEndpoints.put("flight.partition.table", "test_schema.randomized");
        randomizedEndpoints.put("flight.partition.predicates", "id % 6 = 0;id % 6 = 1;id % 6 = 2;id % 6 = 3;id % 6 = 4;id % 6 = 5");
        trino.createCatalog("randomized_flight", "recording_flight", randomizedEndpoints);
    }

    /**
     * Writes reproducible varied groups, exact decimals and nonfinite extrema for six endpoints.
     *
     * @throws Exception if the fixture cannot be written
     */
    private static void writeRandomizedTable() throws Exception {
        Path parquet = Files.createDirectories(data.resolve("test_schema/randomized")).resolve("data.parquet");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:"); var statement = connection.createStatement()) {
            statement.execute("COPY (SELECT id, CASE WHEN id % 17 != 0 THEN r % 11 END k,"
                    + " CASE WHEN id % 7 != 0 THEN r END v,"
                    + " CASE WHEN id % 7 != 0 THEN r::DECIMAL(18,2) END price,"
                    + " CASE WHEN id % 7 != 0 THEN r::DOUBLE / 10000 END f64,"
                    + " CASE WHEN id % 7 != 0 THEN (r::DOUBLE / 10000)::FLOAT END f32,"
                    + " CASE WHEN id % 13 = 0 THEN NULL WHEN id % 31 = 0 THEN 'NaN'::DOUBLE"
                    + " WHEN id % 37 = 0 THEN 'Infinity'::DOUBLE WHEN id % 41 = 0 THEN '-Infinity'::DOUBLE"
                    + " WHEN id % 43 = 0 THEN '-0.0'::DOUBLE ELSE r::DOUBLE END d"
                    + " FROM (SELECT id, ((id * 1103515245 + 12345) % 2000001) - 1000000 r FROM range(512) t(id))) TO '"
                    + parquet.toString().replace("'", "''") + "' (FORMAT PARQUET)");
        }
    }

    /**
     * Writes canonical months, milliseconds and fixed IP bytes with explicit logical types.
     *
     * @throws Exception if the logical source fixture cannot be written
     */
    private static void writeLogicalTypeTable() throws Exception {
        var schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("""
                message logical {
                  required int64 id;
                  required int64 k;
                  optional int32 ym;
                  optional int64 dt;
                  optional binary ip (STRING);
                  optional binary c (STRING);
                }
                """);
        var factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);
        var path = Files.createDirectories(data.resolve("test_schema/logical")).resolve("data.parquet");
        var hints = Map.of("arrowflight.trino.types",
                "{\"ym\":\"interval_year_month\",\"dt\":\"interval_day_time\",\"ip\":\"ipaddress\",\"c\":\"char(4)\"}");
        long[] months = {12, -1, 0, 30, -20, 0};
        long[] millis = {1000, 1501, 0, -1500, 0, 1};
        String[] strings = {"a", "a ", "a\u0000", "ä", "𝄞", "\uE000", null};
        try (var writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(
                new org.apache.hadoop.fs.Path(path.toUri())).withType(schema).withExtraMetaData(hints).build()) {
            for (int id = 0; id < strings.length; id++) {
                var group = factory.newGroup().append("id", (long) id).append("k", id == 4 ? 2L : 1L);
                if (id != 2 && id < months.length) {
                    group.append("ym", Math.toIntExact(months[id])).append("dt", millis[id]);
                    byte[] address = new byte[16];
                    java.util.Arrays.fill(address, id == 1 || id == 4 ? (byte) 0xff : (byte) 0);
                    address[0] = id == 1 ? (byte) 0x7f : id == 3 ? (byte) 0x80 : address[0];
                    group.append("ip", java.net.InetAddress.getByAddress(address).getHostAddress());
                }
                if (strings[id] != null) {
                    group.append("c", strings[id]);
                }
                writer.write(group);
            }
        }
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
                .setCatalogSessionProperty("failing_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setCatalogSessionProperty("single_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setCatalogSessionProperty("exact_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setCatalogSessionProperty("composite_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setCatalogSessionProperty("logical_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
                .setCatalogSessionProperty("randomized_flight", "partial_aggregation_pushdown", Boolean.toString(enabled))
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

    /** Checks additional COUNT types preserve NULL handling and native merging across endpoints. */
    @Test
    void countsAdditionalScalarInputs() {
        String functions = "count(mixed_r), count(decimal9), count(decimal18), count(decimal19), count(decimal38), "
                + "count(date_v), count(timestamp_ms), count(timestamp_us), count(timestamp_ns)";
        List<Long> expectedCounts = List.of(4L, 3L, 3L, 3L, 3L, 3L, 3L, 3L, 3L);
        for (int concurrency : List.of(1, 4)) {
            Session enabled = session(true, concurrency);
            Session disabled = session(false, concurrency);
            for (String sql : List.of("SELECT " + functions + " FROM grouped",
                    "SELECT k, " + functions + " FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                    "SELECT k, " + functions + " FROM grouped GROUP BY k HAVING count(mixed_r) > 1 ORDER BY k",
                    "SELECT " + functions + " FROM grouped WHERE id IN (2, 5)",
                    "SELECT " + functions + " FROM grouped WHERE id IN (0, 4)",
                    "SELECT " + functions + " FROM grouped WHERE id > 100",
                    "SELECT k, " + functions + " FROM grouped WHERE id > 100 GROUP BY k")) {
                var expected = trino.execute(disabled, sql);
                QUERIES.clear();
                var actual = trino.execute(enabled, sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
                assertTrue(QUERIES.stream().anyMatch(query -> query.contains("__flight_count_")), QUERIES.toString());
            }
            assertEquals(expectedCounts, trino.execute(enabled, "SELECT " + functions + " FROM grouped")
                    .getMaterializedRows().getFirst().getFields());
            assertEquals(Collections.nCopies(9, 0L), trino.execute(enabled,
                    "SELECT " + functions + " FROM empty_flight.test_schema.grouped")
                    .getMaterializedRows().getFirst().getFields());
            assertEquals(0, trino.execute(enabled, "SELECT k, " + functions
                    + " FROM empty_flight.test_schema.grouped GROUP BY k").getRowCount());
            String unfiltered = "SELECT " + functions + " FROM single_flight.test_schema.grouped";
            assertEquals(expectedCounts, trino.execute(disabled, unfiltered)
                    .getMaterializedRows().getFirst().getFields());
            QUERIES.clear();
            assertEquals(expectedCounts, trino.execute(enabled, unfiltered)
                    .getMaterializedRows().getFirst().getFields());
            assertEquals(1, QUERIES.size());
            assertTrue(QUERIES.getFirst().contains("COUNT(timestamp_ns)"), QUERIES.toString());
            assertFalse(QUERIES.getFirst().contains("WHERE"), QUERIES.toString());
            String plan = (String) trino.execute(enabled, "EXPLAIN SELECT k, " + functions
                    + " FROM grouped GROUP BY k").getOnlyValue();
            assertTrue(plan.contains("__flight_count_"), plan);
            assertTrue(plan.contains("FINAL"), plan);
        }
    }

    /** Checks decimal sums and averages fall back while preserving native results. */
    @Test
    void fallsBackForDecimalSumsAndAverages() {
        String functions = "sum(mixed_r), avg(mixed_r), sum(range_r), avg(range_r), "
                + "sum(decimal9), avg(decimal9), sum(decimal18), avg(decimal18), "
                + "sum(decimal19), avg(decimal19), sum(decimal38), avg(decimal38), "
                + "sum(decimal_scale38), avg(decimal_scale38), sum(unsigned_decimal), avg(unsigned_decimal)";
        for (int concurrency : List.of(1, 4)) {
            for (String sql : List.of("SELECT " + functions + " FROM grouped",
                    "SELECT k, " + functions + " FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                    "SELECT k, " + functions + " FROM grouped GROUP BY k HAVING avg(decimal18) > 0 ORDER BY k",
                    "SELECT " + functions + " FROM grouped WHERE id IN (2,5)",
                    "SELECT " + functions + " FROM grouped WHERE id > 100",
                    "SELECT k, " + functions + " FROM grouped WHERE id > 100 GROUP BY k")) {
                var expected = trino.execute(session(false, concurrency), sql);
                QUERIES.clear();
                var actual = trino.execute(session(true, concurrency), sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
                assertFalse(QUERIES.stream().anyMatch(query -> query.contains("FLIGHT_EXACT_SUM(")), QUERIES.toString());
                assertFalse(QUERIES.stream().anyMatch(query -> query.contains("SUM(CAST(mixed_r AS DOUBLE))")), QUERIES.toString());
            }
            for (String function : List.of("sum(mixed_r)", "avg(mixed_r)", "sum(decimal38)", "avg(decimal38)")) {
                assertNull(trino.execute(session(true, concurrency), "SELECT " + function
                        + " FROM empty_flight.test_schema.grouped").getOnlyValue());
            }
            assertEquals(new BigDecimal("55340232221128654845"), trino.execute(session(true, concurrency),
                    "SELECT sum(unsigned_decimal) FROM grouped").getOnlyValue());
            assertEquals(new BigDecimal("18446744073709551615"), trino.execute(session(true, concurrency),
                    "SELECT avg(unsigned_decimal) FROM grouped").getOnlyValue());
        }
    }

    /** Checks decimal partials can exceed 128 bits and cancel before native FINAL. */
    @Test
    void preservesWideDecimalCancellationAndRounding() {
        String table = "exact_flight.test_schema.exact";
        String functions = "sum(wide), avg(wide), sum(scaled), avg(scaled), avg(rounding), avg(rounding_scale)";
        for (int concurrency : List.of(1, 4)) {
            for (String sql : List.of("SELECT " + functions + " FROM " + table,
                    "SELECT k, " + functions + " FROM " + table + " GROUP BY k HAVING sum(wide) = 0",
                    "SELECT avg(wide), avg(scaled) FROM " + table + " WHERE id < 3",
                    "SELECT avg(wide), avg(scaled) FROM " + table + " WHERE id >= 3")) {
                var expected = trino.execute(session(false, concurrency), sql);
                QUERIES.clear();
                var actual = trino.execute(session(true, concurrency), sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
                assertFalse(QUERIES.stream().anyMatch(query -> query.contains("FLIGHT_EXACT_SUM(")), QUERIES.toString());
            }
            assertEquals(new BigDecimal("0"), trino.execute(session(true, concurrency),
                    "SELECT sum(wide) FROM " + table).getOnlyValue());
            assertEquals(new BigDecimal("-1"), trino.execute(session(true, concurrency),
                    "SELECT avg(rounding) FROM " + table).getOnlyValue());
            assertEquals(new BigDecimal("0.02"), trino.execute(session(true, concurrency),
                    "SELECT avg(rounding_scale) FROM " + table).getOnlyValue());
            for (String column : List.of("wide", "scaled")) {
                String sql = "SELECT sum(" + column + ") FROM " + table + " WHERE id < 3";
                assertThrows(RuntimeException.class, () -> trino.execute(session(false, concurrency), sql));
                assertThrows(RuntimeException.class, () -> trino.execute(session(true, concurrency), sql));
            }
        }
    }

    /** Checks temporal columns remain readable while unsupported aggregate combinations stay in Trino. */
    @Test
    void readsTypedColumnsAndPreservesFallback() {
        String sql = "SELECT date_v, timestamp_ms, timestamp_us, timestamp_ns FROM grouped WHERE id = 0";
        var expected = trino.execute("SELECT DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.123', "
                + "TIMESTAMP '2026-11-14 12:34:56.123456', TIMESTAMP '2026-11-14 12:34:56.123456789'");
        assertEquals(expected.getMaterializedRows(), trino.execute(session(true, 1), sql).getMaterializedRows());
        String mixed = "SELECT count(date_v), min(date_v), sum(decimal18) FROM grouped";
        var baseline = trino.execute(session(false, 4), mixed);
        QUERIES.clear();
        var actual = trino.execute(session(true, 4), mixed);
        assertEquals(baseline.getTypes(), actual.getTypes());
        assertEquals(baseline.getMaterializedRows(), actual.getMaterializedRows());
        assertFalse(QUERIES.stream().anyMatch(query -> query.contains("FLIGHT_EXACT_SUM(")), QUERIES.toString());
    }

    /** Checks scalar extrema and grouping preserve Trino order across endpoints. */
    @Test
    void mergesOrderableScalarExtremaAndGroups() {
        String aggregates = "min(bool_v), max(bool_v), min(text_v), max(text_v), min(date_v), max(date_v), "
                + "min(timestamp_ms), max(timestamp_ms), min(timestamp_ns), max(timestamp_ns), "
                + "min(CAST(uuid_v AS UUID)), max(CAST(uuid_v AS UUID))";
        for (String sql : List.of("SELECT " + aggregates + " FROM grouped",
                "SELECT " + aggregates + " FROM single_flight.test_schema.grouped",
                "SELECT text_v, min(bool_v), max(date_v) FROM grouped GROUP BY text_v ORDER BY text_v NULLS FIRST")) {
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("__flight_min_")), QUERIES.toString());
        }
    }

    /** Checks required composite extrema are constructed and evaluated by Trino. */
    @Test
    void retainsRequiredCompositeExtrema() {
        for (String sql : List.of("SELECT min(ARRAY[id, coalesce(v, BIGINT '0')]), max(ARRAY[id, coalesce(v, BIGINT '0')]) FROM grouped",
                "SELECT min(ROW(id, coalesce(text_v, ''))), max(ROW(id, coalesce(text_v, ''))) FROM grouped")) {
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("__flight_min_")), QUERIES.toString());
        }
    }

    /** Checks safe computed arguments execute remotely with exact native aggregation states. */
    @Test
    void retainsTypedAggregateExpressions() {
        for (String sql : List.of(
                "SELECT sum(decimal18 * (DECIMAL '1.00' - decimal9)), avg(decimal18 * (DECIMAL '1.00' - decimal9)) FROM grouped",
                "SELECT k, sum(decimal18 * (DECIMAL '1.00' - decimal9)) FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT min(CAST(v AS DECIMAL(19,0))), max(CAST(v AS DECIMAL(19,0))) FROM grouped",
                "SELECT id, decimal18 * (DECIMAL '1.00' - decimal9) FROM grouped ORDER BY id")) {
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("CAST(") && query.contains("DECIMAL(")),
                    () -> sql + "\n" + QUERIES);
            if (sql.contains("sum(")) {
                assertFalse(QUERIES.stream().anyMatch(query -> query.contains("FLIGHT_EXACT_SUM(CAST(")),
                        () -> sql + "\n" + QUERIES);
            }
        }
    }

    /** Checks conditionals push only fully safe branches and keep lazy errors native. */
    @Test
    void retainsConditionalAggregateExpressions() {
        for (String sql : List.of(
                "SELECT sum(CASE WHEN id < 3 THEN decimal18 ELSE DECIMAL '0.00' END) FROM grouped",
                "SELECT k, sum(coalesce(decimal18, DECIMAL '0.00')) FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT avg(CASE WHEN bool_v THEN decimal18 ELSE NULL END) FROM grouped")) {
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("FLIGHT_EXACT_SUM(CAST(")
                    && (query.contains("CASE WHEN") || query.contains("COALESCE("))), () -> sql + "\n" + QUERIES);
        }
        String lazy = "SELECT sum(CASE WHEN id < 0 THEN CAST(text_v AS BIGINT) ELSE v END) FROM grouped";
        var expected = trino.execute(session(false, 4), lazy);
        QUERIES.clear();
        assertEquals(expected.getMaterializedRows(), trino.execute(session(true, 4), lazy).getMaterializedRows());
        assertFalse(QUERIES.stream().anyMatch(query -> query.contains("CAST(\"text_v\" AS BIGINT)")), QUERIES.toString());
    }

    /** Checks aggregate masks preserve groups, NULLs and exact decimal states. */
    @Test
    void retainsFilteredAggregates() {
        for (String sql : List.of(
                "SELECT k, count(*) FILTER (WHERE v > 5), sum(decimal18) FILTER (WHERE id < 3), "
                        + "avg(decimal18) FILTER (WHERE id < 3) FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT count(*) FILTER (WHERE v > 500), sum(v) FILTER (WHERE v > 500), "
                        + "avg(v) FILTER (WHERE v > 500) FROM grouped",
                "SELECT count(v) FILTER (WHERE bool_v), sum(v) FILTER (WHERE bool_v), "
                        + "avg(v) FILTER (WHERE bool_v) FROM grouped",
                "SELECT count(*) FILTER (WHERE v > 5) FROM grouped WHERE id < 0")) {
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("COUNT(CASE WHEN")),
                    () -> sql + "\n" + QUERIES + "\n" + trino.execute(session(true, 4), "EXPLAIN " + sql).getOnlyValue());
        }
    }

    /** Checks a reproducible varied dataset merges exactly over six disjoint endpoints. */
    @Test
    void mergesVariedDataAcrossSixEndpoints() {
        String aggregates = "count(*), count(v), min(v), max(v), sum(v), avg(v), min(d), max(d), "
                + "sum(price), avg(price), sum(price * DECIMAL '0.75'), "
                + "count(*) FILTER (WHERE v > 0), sum(price) FILTER (WHERE v < 0)";
        for (int concurrency : List.of(1, 4)) {
            for (String sql : List.of("SELECT " + aggregates + " FROM randomized_flight.test_schema.randomized",
                    "SELECT k, " + aggregates + " FROM randomized_flight.test_schema.randomized GROUP BY k ORDER BY k NULLS FIRST")) {
                var expected = trino.execute(session(false, concurrency), sql);
                QUERIES.clear();
                var actual = trino.execute(session(true, concurrency), sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
                assertEquals(6, QUERIES.size(), QUERIES.toString());
                assertTrue(QUERIES.stream().noneMatch(query -> query.contains("FLIGHT_EXACT_SUM(")
                        || query.contains("COUNT(CASE WHEN") || query.contains("MIN(-(")), QUERIES.toString());
            }
        }
    }

    /** Checks finite floating reductions meet the explicit tolerance for the bounded fixture. */
    @Test
    void boundsFloatingReductionDifferences() {
        for (String catalog : List.of("single_flight", "randomized_flight")) {
            for (String function : List.of("sum(f64)", "avg(f64)", "sum(f32)", "avg(f32)")) {
                String sql = "SELECT " + function + " FROM " + catalog + ".test_schema.randomized";
                var expected = trino.execute(session(false, 4), sql);
                QUERIES.clear();
                var actual = trino.execute(session(true, 4), sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                double baseline = ((Number) expected.getOnlyValue()).doubleValue();
                double result = ((Number) actual.getOnlyValue()).doubleValue();
                double tolerance = function.contains("f32") ? 2.0 * Math.ulp((float) baseline)
                        : 1e-12 * Math.max(1.0, Math.abs(baseline));
                assertTrue(Double.isFinite(result) && Math.abs(result - baseline) <= tolerance,
                        () -> sql + ": expected=" + baseline + ", actual=" + result + ", tolerance=" + tolerance);
                assertTrue(QUERIES.stream().anyMatch(query -> query.contains("SUM(")), QUERIES.toString());
            }
        }
    }

    /** Checks a stream failure after delivered states fails the query instead of returning partial totals. */
    @Test
    void rejectsTruncatedAggregateStreams() {
        FAILURE_BATCHES.set(0);
        QUERIES.clear();
        assertThrows(RuntimeException.class, () -> trino.execute(session(true, 4),
                "SELECT count(*), sum(v), avg(d) FROM failing_flight.test_schema.grouped"));
        assertTrue(FAILURE_BATCHES.get() > 0, "Failure must occur after a real state batch");
        assertTrue(QUERIES.stream().anyMatch(query -> query.contains("__flight_avg_")), QUERIES.toString());
    }

    /** Checks analytics and unsupported expressions retain native SQL semantics. */
    @Test
    void executesAnalyticalFallbackQueries() {
        for (String sql : List.of(
                "SELECT sum(decimal18 * (DECIMAL '1.00' - decimal9)) FROM grouped",
                "SELECT sum(CASE WHEN id % 2 = 0 THEN decimal18 ELSE DECIMAL '0.00' END) FROM grouped",
                "SELECT sum(coalesce(decimal18, DECIMAL '0.00')), avg(CAST(v AS DECIMAL(19,0))) FROM grouped",
                "SELECT k, count(*) FILTER (WHERE v > 5), sum(decimal18) FILTER (WHERE id < 3) "
                        + "FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT count(DISTINCT k), sum(DISTINCT v) FROM grouped",
                "SELECT id, sum(v) OVER (PARTITION BY k ORDER BY id ROWS UNBOUNDED PRECEDING) FROM grouped ORDER BY id",
                "SELECT k, count(*) FROM grouped GROUP BY ROLLUP(k) ORDER BY k NULLS FIRST, count(*)",
                "SELECT k, count(*) FROM grouped GROUP BY CUBE(k) ORDER BY k NULLS FIRST, count(*)",
                "SELECT a.k, sum(a.v * b.v) FROM grouped a JOIN grouped b ON a.id = b.id "
                        + "GROUP BY a.k ORDER BY a.k NULLS FIRST",
                "SELECT k, max(s) FROM (SELECT k, sum(v) s FROM grouped GROUP BY k) t "
                        + "GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT min(ARRAY[id]), max(ARRAY[id]), min(ROW(id)), max(ROW(id)) FROM grouped")) {
            var expected = trino.execute(session(false, 4), sql);
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
        }
        String invalid = "SELECT min(ARRAY[CAST(1 AS BIGINT), CASE WHEN id = 0 THEN NULL ELSE id END]) FROM grouped";
        assertThrows(RuntimeException.class, () -> trino.execute(session(false, 4), invalid));
        QUERIES.clear();
        assertThrows(RuntimeException.class, () -> trino.execute(session(true, 4), invalid));
        assertFalse(QUERIES.stream().anyMatch(query -> query.contains("__flight_min_")), QUERIES.toString());
    }

    /** Checks canonical interval arithmetic, IP order and CHAR equality use native Trino contracts. */
    @Test
    void aggregatesLogicalSourceTypes() {
        String logical = "(SELECT k, ym * INTERVAL '1' MONTH AS ym, dt * INTERVAL '0.001' SECOND AS dt, "
                + "CAST(ip AS IPADDRESS) AS ip, CAST(c AS CHAR(4)) AS c FROM logical_flight.test_schema.logical) t";
        String functions = "sum(ym), avg(ym), min(ym), max(ym), count(ym), "
                + "sum(dt), avg(dt), min(dt), max(dt), count(dt), min(ip), max(ip), count(ip), min(c), max(c), count(c)";
        for (String sql : List.of("SELECT " + functions + " FROM " + logical,
                "SELECT k, " + functions + " FROM " + logical + " GROUP BY k ORDER BY k")) {
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertFalse(QUERIES.stream().anyMatch(query -> query.contains("__flight_avg_")), QUERIES.toString());
        }
        for (String column : List.of("ym", "dt", "ip", "c")) {
            String sql = "SELECT " + column + ", count(*) FROM " + logical + " GROUP BY "
                    + column + " ORDER BY " + column + " NULLS FIRST";
            assertEquals(trino.execute(session(false, 4), sql).getMaterializedRows(),
                    trino.execute(session(true, 4), sql).getMaterializedRows(), sql);
        }
    }

    /** Checks scalar group equality and null handling across endpoint partitions. */
    @Test
    void groupsAdditionalScalarKeys() {
        for (String column : List.of("mixed_d", "mixed_r", "zero_d", "decimal18", "date_v",
                "timestamp_ns", "bool_v", "uuid_v")) {
            String sql = "SELECT " + column + ", count(*), min(v), max(v) FROM grouped GROUP BY "
                    + column + " ORDER BY " + column + " NULLS FIRST";
            var expected = trino.execute(session(false, 4), sql);
            QUERIES.clear();
            var actual = trino.execute(session(true, 4), sql);
            assertEquals(expected.getTypes(), actual.getTypes(), sql);
            assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
            assertEquals(column.equals("bool_v") || column.equals("uuid_v"),
                    QUERIES.stream().anyMatch(query -> query.contains("__flight_count_")), QUERIES.toString());
        }
    }

    /** Checks unsupported aggregates and residual row filters retain the raw scan path. */
    @Test
    void fallsBackWithoutChangingResults() {
        for (String sql : List.of("SELECT count(DISTINCT k) FROM grouped", "SELECT count(*), max(CAST(d AS VARCHAR)) FROM grouped",
                "SELECT avg(CAST(v AS DECIMAL(5, 0))) FROM grouped",
                "SELECT count(*) FROM grouped WHERE id % 2 = 0")) {
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

    /** Checks non-finite and zero floating SUM/AVG preserve the baseline result. */
    @Test
    void preservesNonFiniteSumsAndAverages() {
        for (String column : List.of("special_d", "special_r", "zero_r")) {
            for (String filter : List.of("", " WHERE id = 0", " WHERE id = 1", " WHERE id = 3", " WHERE id IN (1, 3)")) {
                String sql = "SELECT sum(" + column + "), avg(" + column + ") FROM grouped" + filter;
                var expected = trino.execute(session(false, 1), sql);
                QUERIES.clear();
                var actual = trino.execute(session(true, 1), sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
                String remote = column.equals("special_d") ? "SUM(special_d)" : "SUM(CAST(" + column + " AS DOUBLE))";
                assertTrue(QUERIES.stream().anyMatch(query -> query.contains(remote)), QUERIES.toString());
            }
        }
    }

    /** Checks server-dependent extrema fall back and preserve exact decimal and NaN results. */
    @Test
    void fallsBackForFloatingMaxAndDecimalExtrema() {
        String functions = "min(mixed_d), max(mixed_d), min(mixed_r), max(mixed_r), "
                + "min(special_d), max(special_d), min(decimal18), max(decimal18), "
                + "min(decimal19), max(decimal19), min(decimal38), max(decimal38)";
        for (int concurrency : List.of(1, 4)) {
            for (String sql : List.of("SELECT " + functions + " FROM grouped",
                    "SELECT k, " + functions + " FROM grouped GROUP BY k ORDER BY k NULLS FIRST",
                    "SELECT k, " + functions + " FROM grouped GROUP BY k HAVING max(mixed_d) > 10 ORDER BY k NULLS FIRST",
                    "SELECT " + functions + " FROM grouped WHERE id = 0",
                    "SELECT " + functions + " FROM grouped WHERE id = 4",
                    "SELECT " + functions + " FROM grouped WHERE id IN (0, 4)",
                    "SELECT " + functions + " FROM grouped WHERE id IN (2, 5)",
                    "SELECT " + functions + " FROM grouped WHERE id > 100",
                    "SELECT k, " + functions + " FROM grouped WHERE id > 100 GROUP BY k")) {
                var expected = trino.execute(session(false, concurrency), sql);
                QUERIES.clear();
                var actual = trino.execute(session(true, concurrency), sql);
                assertEquals(expected.getTypes(), actual.getTypes(), sql);
                assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows(), sql);
                assertFalse(QUERIES.stream().anyMatch(query -> query.contains("__flight_min_")), QUERIES.toString());
                assertFalse(QUERIES.stream().anyMatch(query -> query.contains("__flight_max_")), QUERIES.toString());
                assertTrue(QUERIES.stream().allMatch(query -> !query.contains("MAX(mixed_d)")
                        && !query.contains("MAX(mixed_r)")), QUERIES.toString());
            }
            assertEquals(20.0, trino.execute(session(true, concurrency),
                    "SELECT max(mixed_d) FROM grouped").getOnlyValue());
            assertEquals(new BigDecimal("90071992547409.94"), trino.execute(session(true, concurrency),
                    "SELECT max(decimal18) FROM grouped").getOnlyValue());
            assertEquals(new BigDecimal("-99999999999999999999.999999999999999999"),
                    trino.execute(session(true, concurrency), "SELECT min(decimal38) FROM grouped").getOnlyValue());
            for (String column : List.of("mixed_d", "mixed_r", "decimal18", "decimal19", "decimal38")) {
                for (String function : List.of("min", "max")) {
                    assertNull(trino.execute(session(true, concurrency), "SELECT " + function + "(" + column
                            + ") FROM empty_flight.test_schema.grouped").getOnlyValue());
                }
            }
            String plan = (String) trino.execute(session(true, concurrency),
                    "EXPLAIN SELECT k, " + functions + " FROM grouped GROUP BY k").getOnlyValue();
            assertFalse(plan.contains("__flight_min_"), plan);
            assertFalse(plan.contains("__flight_max_"), plan);
            assertTrue(plan.contains("FINAL"), plan);
        }
    }

    /** Checks extrema preserve individual zero signs and remain numerically equal for mixed zero signs. */
    @Test
    void preservesFloatingZeroExtrema() {
        for (String function : List.of("min", "max")) {
            for (String filter : List.of(" WHERE id = 0", " WHERE id = 1")) {
                String sql = "SELECT " + function + "(zero_d) FROM grouped" + filter;
                double expected = (Double) trino.execute(session(false, 1), sql).getOnlyValue();
                QUERIES.clear();
                double actual = (Double) trino.execute(session(true, 1), sql).getOnlyValue();
                assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual), sql);
                assertEquals(function.equals("min"), QUERIES.stream().anyMatch(query -> query.contains("__flight_" + function)), QUERIES.toString());
            }
            double actual = (Double) trino.execute(session(true, 4),
                    "SELECT " + function + "(zero_d) FROM grouped").getOnlyValue();
            assertTrue(actual == 0.0);
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
            if (catalogName.equals("failing_flight")) {
                doAnswer(invocation -> {
                    var delegate = (pro.surpin.data.arrowflight.trino.client.FlightService.BatchReader) invocation.callRealMethod();
                    return new pro.surpin.data.arrowflight.trino.client.FlightService.BatchReader() {
                        private boolean delivered;

                        @Override
                        public boolean next() throws java.io.IOException {
                            if (delivered) {
                                throw new java.io.IOException("injected-after-first-batch");
                            }
                            delivered = delegate.next();
                            if (delivered) {
                                FAILURE_BATCHES.incrementAndGet();
                            }
                            return delivered;
                        }

                        @Override
                        public org.apache.arrow.vector.VectorSchemaRoot root() {
                            return delegate.root();
                        }

                        @Override
                        public void close() {
                            delegate.close();
                        }
                    };
                }).when(service).read(org.mockito.ArgumentMatchers.any());
            }
            return new FlightConnector(service, config.columnQuote(), config.partitioning(), context.getTypeManager());
        }
    }
}
