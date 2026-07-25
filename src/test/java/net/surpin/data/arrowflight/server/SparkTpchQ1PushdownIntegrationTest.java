package net.surpin.data.arrowflight.server;

import net.surpin.data.arrowflight.client.spark.FlightSessionCatalog;
import net.surpin.data.arrowflight.client.spark.FlightSource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies full TPC-H Q1 aggregation pushdown through the Spark Flight connector.
 */
@Tag("integration")
@Tag("spark")
@Tag("smoke")
class SparkTpchQ1PushdownIntegrationTest {

    private static final String Q1 = """
            SELECT
              l_returnflag,
              l_linestatus,
              SUM(l_quantity) AS sum_qty,
              SUM(l_extendedprice) AS sum_base_price,
              SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
              SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
              AVG(l_quantity) AS avg_qty,
              AVG(l_extendedprice) AS avg_price,
              AVG(l_discount) AS avg_disc,
              COUNT(*) AS count_order
            FROM %s
            WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
            """;

    private static TestFlightServerHelper helper;
    private static SparkSession spark;

    /**
     * Starts an embedded Flight server and ANSI-enabled local Spark session.
     *
     * @throws Exception when either service cannot start
     */
    @BeforeAll
    static void startServerAndSpark() throws Exception {
        injectSparkUser(System.getProperty("user.name", "root"));
        Path dataDir = Paths.get("src/test/resources/lineitem_tiny").toAbsolutePath();
        helper = TestFlightServerHelper.builder()
                .dataDir(dataDir.toString())
                .start();

        spark = SparkSession.builder()
                .appName("SparkTpchQ1PushdownIntegrationTest")
                .master("local[2]")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.catalog.spark_catalog", FlightSessionCatalog.class.getName())
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        flightLineitem().createOrReplaceTempView("flight_lineitem");
        spark.sql("DROP TABLE IF EXISTS persisted_flight_lineitem");
        spark.sql("""
                CREATE TABLE persisted_flight_lineitem
                USING %s
                OPTIONS (
                  host 'localhost',
                  port '%d',
                  user 'test',
                  password 'test',
                  table 'tpch.lineitem'
                )
                """.formatted(FlightSource.class.getName(), helper.location.getUri().getPort()));
        spark.read()
                .parquet(dataDir.resolve("tpch/lineitem").toString())
                .createOrReplaceTempView("direct_lineitem");
    }

    /**
     * Stops Spark and the embedded Flight server.
     *
     * @throws Exception when resource cleanup fails
     */
    @AfterAll
    static void stopAll() throws Exception {
        if (spark != null) {
            spark.sql("DROP TABLE IF EXISTS persisted_flight_lineitem");
            spark.stop();
        }
        if (helper != null) {
            helper.close();
        }
    }

    /**
     * Checks that Spark pushes Q1 partial aggregates and preserves direct Parquet results.
     */
    @Test
    void fullQ1PushesAggregatesAndMatchesDirectParquet() {
        assertQ1PushdownAndResults("flight_lineitem");
    }

    /**
     * Checks that a persisted Flight table retains Q1 aggregate pushdown.
     */
    @Test
    void persistedQ1PushesAggregatesAndMatchesDirectParquet() {
        assertQ1PushdownAndResults("persisted_flight_lineitem");
    }

    /**
     * Checks Q1 aggregate pushdown and results for one Flight-backed relation.
     *
     * @param table Flight-backed table or view name
     */
    private static void assertQ1PushdownAndResults(String table) {
        Dataset<Row> flightResult = q1(table);
        Dataset<Row> directResult = q1("direct_lineitem");

        String plan = flightResult.queryExecution().executedPlan().toString().toLowerCase();
        assertTrue(plan.contains("group by"),
                "Q1 must push GROUP BY into the Flight scan:\n" + plan);
        assertTrue(plan.contains("sum("),
                "Q1 must push decimal SUM expressions into the Flight scan:\n" + plan);
        assertTrue(plan.contains("count("),
                "Q1 must push COUNT partials for AVG into the Flight scan:\n" + plan);

        List<Row> expected = directResult.collectAsList();
        List<Row> actual = flightResult.collectAsList();
        assertEquals(directResult.schema(), flightResult.schema(),
                "Flight and direct Q1 schemas must match");
        assertEquals(expected, actual, "Flight and direct Q1 results must match");
    }

    /**
     * Loads the tiny lineitem table through the Flight DataSource.
     *
     * @return Flight-backed lineitem dataset
     */
    private static Dataset<Row> flightLineitem() {
        return spark.read()
                .format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test")
                .option("password", "test")
                .option("table", "tpch.lineitem")
                .load();
    }

    /**
     * Builds the benchmark Q1 query for one registered lineitem view.
     *
     * @param table Spark view name
     * @return Q1 result dataset
     */
    private static Dataset<Row> q1(String table) {
        return spark.sql(Q1.formatted(table));
    }

    /**
     * Injects the Spark user required by Hadoop on Java 21.
     *
     * @param user Spark user name
     */
    @SuppressWarnings("unchecked")
    private static void injectSparkUser(String user) {
        try {
            Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
            Field unmodifiableField =
                    processEnvironment.getDeclaredField("theUnmodifiableEnvironment");
            unmodifiableField.setAccessible(true);
            Map<String, String> unmodifiableEnvironment =
                    (Map<String, String>) unmodifiableField.get(null);
            Field mutableField = unmodifiableEnvironment.getClass().getDeclaredField("m");
            mutableField.setAccessible(true);
            Map<String, String> environment =
                    (Map<String, String>) mutableField.get(unmodifiableEnvironment);
            environment.put("SPARK_USER", user);
        } catch (ReflectiveOperationException exception) {
            System.err.println("Warning: could not inject SPARK_USER (" + exception + ")");
        }
    }
}
