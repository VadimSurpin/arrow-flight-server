package pro.surpin.data.arrowflight.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests verifying that Spark reads from Arrow Flight and performs
 * a self-join in Spark (not pushed down to the server). Each test reads the
 * table twice as separate DataFrames and joins them on the `id` column.
 */
@Tag("integration")
@Tag("spark")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkJoinIntegrationTest {

    private static TestFlightServerHelper helper;
    private static SparkSession spark;
    private static String flightTable;
    private static String host;
    private static int port;

    @BeforeAll
    static void startServerAndSpark() throws Exception {
        injectSparkUser(System.getProperty("user.name", "root"));

        helper = TestFlightServerHelper.builder().start();

        host = "localhost";
        port = helper.location.getUri().getPort();
        flightTable = "test_schema.test_table";

        spark = SparkSession.builder()
                .appName("SparkJoinIntegrationTest")
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

    private Dataset<Row> flightRead(String query) {
        return spark.read()
                .format("flight")
                .option("host", host)
                .option("port", String.valueOf(port))
                .option("user", "test")
                .option("bearerToken", "test-token")
                .option("table", query)
                .option("column.quote", "\"")
                .load();
    }

    // ── tests ──────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void selfJoinOnIdPreservesRowCount() {
        Dataset<Row> df1 = flightRead("SELECT * FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT * FROM " + flightTable);

        Dataset<Row> joined = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner");

        long originalCount = df1.count();
        long joinedCount   = joined.count();

        assertTrue(originalCount > 0, "Source table must be non-empty");
        assertEquals(originalCount, joinedCount,
                "Self-join on unique key `id` must preserve row count");
    }

    @Test
    @Order(2)
    void joinedSchemaContainsColumnsFromBothSides() {
        Dataset<Row> df1 = flightRead("SELECT id, bool_col FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT id, bool_col FROM " + flightTable);

        Dataset<Row> joined = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner")
                .select(col("L.id"), col("L.bool_col").as("bool_L"), col("R.bool_col").as("bool_R"));

        List<String> fields = List.of(joined.schema().fieldNames());
        assertTrue(fields.contains("id"),     "Joined schema must contain 'id'");
        assertTrue(fields.contains("bool_L"), "Joined schema must contain 'bool_L'");
        assertTrue(fields.contains("bool_R"), "Joined schema must contain 'bool_R'");
    }

    @Test
    @Order(3)
    void joinIsExecutedBySparkNotPushedDown() {
        Dataset<Row> df1 = flightRead("SELECT id, bool_col FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT id, bool_col FROM " + flightTable);

        Dataset<Row> joined = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner");

        String plan = joined.queryExecution().executedPlan().toString();
        assertTrue(plan.contains("Join"),
                "Physical plan must contain 'Join' (Spark-side); plan was:\n" + plan);
    }

    @Test
    @Order(4)
    void joinedValuesAreConsistentAcrossBothSides() {
        Dataset<Row> df1 = flightRead("SELECT id, bool_col FROM " + flightTable);
        Dataset<Row> df2 = flightRead("SELECT id, bool_col FROM " + flightTable);

        long mismatches = df1.alias("L")
                .join(df2.alias("R"), col("L.id").equalTo(col("R.id")), "inner")
                .filter(col("L.bool_col").notEqual(col("R.bool_col")))
                .count();

        assertEquals(0, mismatches,
                "bool_col must be identical on both sides of the self-join — found mismatches: " + mismatches);
    }

    @Test
    @Order(5)
    void crossTypeJoinIntegerColumnToByteColumnReturnsExpectedCount() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        long count = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.tinyint_col")),
                        "inner")
                .count();

        assertEquals(1000L, count,
                "Join id(IntegerType) = tinyint_col(ByteType) must return 1000 pairs without ClassCastException");
    }

    @Test
    @Order(6)
    void crossTypeJoinIntegerToSmallintReturnsExpectedCount() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.smallint_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(1000L, count,
                "Join id(IntegerType) = smallint_col(ShortType) must return 1000 pairs without ClassCastException");
    }

    @Test
    @Order(7)
    void crossTypeJoinIntegerToBigintReturnsExpectedCount() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.bigint_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(1000L, count,
                "Join id(IntegerType) = bigint_col(LongType) must return 1000 pairs without ClassCastException");
    }

    @Test
    @Order(8)
    void crossTypeJoinIntegerToFloatCompletesWithoutClassCastException() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.float_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertTrue(count >= 0,
                "Join id(IntegerType) = float_col(FloatType) must not throw ClassCastException");
    }

    @Test
    @Order(9)
    void crossTypeJoinIntegerToDoubleCompletesWithoutClassCastException() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.id").equalTo(col("t2.double_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertTrue(count >= 0,
                "Join id(IntegerType) = double_col(DoubleType) must not throw ClassCastException");
    }

    @Test
    @Order(10)
    void crossTypeJoinFloatToDoubleCompletesWithoutClassCastException() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.float_col").equalTo(col("t2.double_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertTrue(count >= 0,
                "Join float_col(FloatType) = double_col(DoubleType) must not throw ClassCastException");
    }

    @Test
    @Order(11)
    void crossTypeJoinBoolToTinyintReturnsExpectedCount() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.bool_col").equalTo(col("t2.tinyint_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(100000L, count,
                "Join bool_col(BooleanType) = tinyint_col(ByteType) must return 100000 pairs without ClassCastException");
    }

    @Test
    @Order(12)
    void crossTypeJoinStringColumnsReturnsExpectedCount() {
        Dataset<Row> t1 = flightRead(flightTable);
        Dataset<Row> t2 = flightRead(flightTable);

        Dataset<Row> joined = t1.alias("t1")
                .join(t2.alias("t2"),
                        col("t1.date_string_col").equalTo(col("t2.string_col")),
                        "inner");

        assertNotNull(joined.schema(), "Schema must not be null");
        long count = joined.count();
        assertEquals(0L, count,
                "Join date_string_col(StringType) = string_col(StringType) must return 0 rows without ClassCastException");
    }

    // ── util ────────────────────────────────────────────────────────────────

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
