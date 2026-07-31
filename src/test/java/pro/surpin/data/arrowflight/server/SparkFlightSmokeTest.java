package pro.surpin.data.arrowflight.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke test for the Spark DataSource V2 "flight" connector.
 */
@Tag("integration")
@Tag("spark")
@Tag("smoke")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkFlightSmokeTest {

    private static TestFlightServerHelper helper;
    private static SparkSession spark;
    private static String host;
    private static int port;
    private static String flightTable;

    @BeforeAll
    static void startServerAndSpark() throws Exception {
        // Java 21+ UGI workaround — inject SPARK_USER before Spark initialises
        injectSparkUser(System.getProperty("user.name", "root"));

        helper = TestFlightServerHelper.builder().start();

        host = "localhost";
        port = helper.location.getUri().getPort();
        flightTable = "test_schema.test_table";

        spark = SparkSession.builder()
                .appName("SparkFlightSmokeTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (spark != null) spark.stop();
        if (helper != null) helper.close();
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private Dataset<Row> flightRead() {
        return spark.read()
                .format("flight")
                .option("host", host)
                .option("port", String.valueOf(port))
                .option("user", "test")
                .option("password", "test")
                .option("table", flightTable)
                .load();
    }

    private static String physicalPlan(Dataset<Row> df) {
        return df.queryExecution().executedPlan().toString();
    }

    // ── 1. minimal read ───────────────────────────────────────────────────

    @Test
    @Order(1)
    void fullTableRead_worksAndPrintsSchema() {
        Dataset<Row> df = flightRead();

        df.printSchema();
        df.show(10);

        List<String> fields = Arrays.asList(df.schema().fieldNames());
        assertTrue(fields.contains("id"), "schema must contain 'id'");
        assertTrue(fields.contains("string_col"), "schema must contain 'string_col'");
        assertEquals(1000L, df.count(), "sample table has 1000 rows (id 0..999)");
    }

    // ── 2. projection pushdown ────────────────────────────────────────────

    @Test
    @Order(2)
    void projectionPushdown_selectIdAndStringColSendsProjectedSql() {
        Dataset<Row> projected = flightRead().select("id", "string_col");

        String plan = physicalPlan(projected);
        System.out.println("[projection] physical plan:\n" + plan);

        assertTrue(plan.toLowerCase().contains("select id,string_col from " + flightTable.toLowerCase())
                        || plan.toLowerCase().contains("select id, string_col from " + flightTable.toLowerCase()),
                "expected a projected 'select id,string_col from " + flightTable + "' statement in the plan, got:\n" + plan);

        assertArrayEquals(new String[]{"id", "string_col"}, projected.schema().fieldNames(),
                "only the requested columns should be part of the read schema");
        assertEquals(1000L, projected.count());

        List<Row> sample = projected.limit(10).collectAsList();
        assertFalse(sample.isEmpty());
        sample.forEach(r -> assertEquals(2, r.size()));
    }

    // ── 3. filter pushdown ────────────────────────────────────────────────

    @Test
    @Order(3)
    void filterPushdown_whereIdGreaterThan100SendsWhereClause() {
        Dataset<Row> filtered = flightRead().where("id > 100").select("id", "string_col");

        String plan = physicalPlan(filtered);
        System.out.println("[filter] physical plan:\n" + plan);

        String lower = plan.toLowerCase();
        assertTrue(lower.contains("where"), "expected a 'where' clause pushed into the scan, got:\n" + plan);
        assertTrue(lower.contains("id > 100") || lower.contains("id>100"),
                "expected 'id > 100' pushed into the scan, got:\n" + plan);

        assertEquals(899L, filtered.count());

        long minId = filtered.selectExpr("min(id)").first().getInt(0);
        assertEquals(101L, minId, "smallest surviving id must be 101, confirming the filter really ran server-side/pruned rows");
    }

    /** Verifies Spark routes column comparisons through the V2 Flight filter API. */
    @Test
    @Order(4)
    void filterPushdown_columnComparisonRunsInFlight() {
        Dataset<Row> filtered = flightRead()
                .where("int_col < double_col")
                .select("id");

        String plan = physicalPlan(filtered);
        String lower = plan.toLowerCase();
        assertTrue(lower.contains("int_col") && lower.contains("double_col")
                        && lower.contains("where"),
                "expected column comparison pushed into the Flight SQL scan, got:\n" + plan);

        long rows = filtered.count();
        assertTrue(rows > 0 && rows < 1000,
                "column comparison must filter the server-side result, rows=" + rows);
    }

    // ── 5. aggregation pushdown ───────────────────────────────────────────

    @Test
    @Order(5)
    void aggregationPushdown_groupByTinyintColSendsGroupByCountSql() {
        Dataset<Row> grouped = flightRead().groupBy("tinyint_col").count();

        String plan = physicalPlan(grouped);
        System.out.println("[aggregation] physical plan:\n" + plan);

        String lower = plan.toLowerCase();
        assertTrue(lower.contains("group by"), "expected a 'group by' clause pushed into the scan, got:\n" + plan);
        assertTrue(lower.contains("count(*)"), "expected 'count(*)' pushed into the scan, got:\n" + plan);
        assertTrue(lower.contains("tinyint_col"), "expected grouping column pushed into the scan, got:\n" + plan);

        List<Row> rows = grouped.collectAsList();
        assertEquals(10, rows.size(), "tinyint_col has 10 distinct values (0..9)");
        rows.forEach(r -> assertEquals(100L, r.getLong(1), "each group should have 100 rows"));

        System.out.println("[aggregation] groupBy(tinyint_col).count() results: "
                + rows.stream().map(Row::toString).collect(Collectors.joining(", ")));
    }

    /** Verifies AVG is decomposed remotely and merged to its final value by Spark. */
    @Test
    @Order(6)
    void aggregationPushdown_avgUsesPartialSumAndCountWithSparkFinalMerge() {
        Dataset<Row> averaged = flightRead().selectExpr("avg(double_col) AS avg_value");

        String plan = physicalPlan(averaged);
        String lower = plan.toLowerCase();
        assertTrue(lower.contains("sum("),
                "expected AVG partial SUM pushed into the Flight scan, got:\n" + plan);
        assertTrue(lower.contains("count("),
                "expected AVG partial COUNT pushed into the Flight scan, got:\n" + plan);
        assertTrue(lower.contains("aggregate"),
                "expected Spark to retain the final aggregate merge, got:\n" + plan);

        List<Row> values = flightRead().select("double_col").collectAsList();
        double expected = values.stream().mapToDouble(row -> row.getDouble(0)).average().orElseThrow();
        assertEquals(expected, averaged.first().getDouble(0), 1.0e-9);
    }

    // ── 7. auth requirement / blocker ─────────────────────────────────────

    @Test
    @Order(7)
    void missingCredentials_isBlockedByMandatoryUserPasswordCheck() {
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                spark.read()
                        .format("flight")
                        .option("host", host)
                        .option("port", String.valueOf(port))
                        .option("table", flightTable)
                        .load()
        );

        System.out.println("[auth] expected failure without credentials: " + ex.getMessage());
        assertTrue(ex.getMessage() != null && ex.getMessage().toLowerCase().contains("mandatory"),
                "FlightSource.probeOptions() should fail fast complaining that host/user/password are mandatory");
    }

    // ── util ──────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static void injectSparkUser(String user) {
        try {
            Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
            Field theUnmod = pe.getDeclaredField("theUnmodifiableEnvironment");
            theUnmod.setAccessible(true);
            Map<String, String> unmodEnv = (Map<String, String>) theUnmod.get(null);
            Field m = unmodEnv.getClass().getDeclaredField("m");
            m.setAccessible(true);
            Map<String, String> strEnv = (Map<String, String>) m.get(unmodEnv);
            strEnv.put("SPARK_USER", user);
        } catch (Exception e) {
            System.err.println("Warning: could not inject SPARK_USER (" + e + ")");
        }
    }
}
