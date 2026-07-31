package pro.surpin.data.arrowflight.server;

import com.hazelcast.map.IMap;
import pro.surpin.data.arrowflight.server.model.HandleState;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@Tag("spark")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiRowGroupGroupByIntegrationTest {

    private static final int  NUM_ROWS       = 10_000;

    private static Path tempDir;

    private TestFlightServerHelper helper;
    private FlightSqlClient sqlClient;

    @BeforeAll
    void setUp() throws Exception {
        tempDir = generateData();
        helper = TestFlightServerHelper.builder()
                .dataDir(tempDir.toAbsolutePath().toString())
                .start();
        sqlClient = helper.sqlClient();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (helper != null) helper.close();
        if (tempDir != null) deleteTree(tempDir);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test @Order(1)
    void totalCountStarReturnsAllRows() throws Exception {
        long count = totalCountStar("SELECT count(*) FROM test_schema.test_table");
        assertEquals(NUM_ROWS, count,
                "Total count(*) across all endpoints must be " + NUM_ROWS + ", got " + count);
    }

    @Test @Order(2)
    void countReturnsAllRows() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT count(*) FROM test_schema.test_table");
        long total = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            total += streamCount(helper.flightClient(), ep.getTicket());
        }
        assertEquals(NUM_ROWS, total, "Sum of per-endpoint counts must equal NUM_ROWS");
    }

    @Test @Order(3)
    void countIsStableAcrossIndependentQueries() throws Exception {
        long first  = totalCountStar("SELECT count(*) FROM test_schema.test_table");
        long second = totalCountStar("SELECT count(*) FROM test_schema.test_table");
        assertEquals(NUM_ROWS, first,  "First query total must equal NUM_ROWS");
        assertEquals(NUM_ROWS, second, "Second query total must equal NUM_ROWS");
    }

    @SuppressWarnings("unchecked")
    @Test @Order(4)
    void ttlExpiryOnlyDecrementsOwningServer() throws Exception {
        IMap<String, Long> registry = helper.hazelcastAdapter.serverRegistry();
        IMap<String, java.io.Serializable> cache = helper.hazelcastAdapter.statementCache();

        String uri = helper.location.getUri().toString();

        assertTrue(registry.containsKey(uri), "Server must be registered before TTL test");

        String handle = UUID.randomUUID().toString();
        cache.put(handle,
                new HandleState("SELECT 1", new String[0], uri, 500L),
                1, TimeUnit.SECONDS);

        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (cache.get(handle) == null) break;
            Thread.sleep(200);
        }
        assertNull(cache.get(handle), "Statement must have expired via TTL");

        assertTrue(registry.containsKey(uri), "Server must remain in registry after TTL expiry");
        assertEquals(0L, registry.get(uri).longValue(), "Server load must be 0 after TTL expiry");
    }

    // ── data generation ───────────────────────────────────────────────────────

    private static Path generateData() throws Exception {
        Path dir = Files.createTempDirectory("mrg-test-");
        Path tableDir = dir.resolve("test_schema/test_table");
        Files.createDirectories(tableDir);

        SparkSession spark = SparkSession.builder()
                .appName("MultiRowGroupTestGen")
                .master("local[2]")
                .config("parquet.block.size", "16384")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        try {
            Dataset<Row> df = spark.range(0, NUM_ROWS, 1, 1)
                    .select(
                            functions.col("id").cast("integer").alias("id"),
                            functions.col("id").mod(2).equalTo(0).alias("bool_col"),
                            functions.col("id").mod(10).cast("tinyint").alias("tinyint_col"),
                            functions.col("id").mod(10).cast("smallint").alias("smallint_col"),
                            functions.col("id").mod(10).cast("integer").alias("int_col"),
                            functions.col("id").mod(10).multiply(10).cast("bigint").alias("bigint_col"),
                            functions.rand(47).cast("float").alias("float_col"),
                            functions.rand(48).cast("double").alias("double_col"),
                            functions.concat(
                                    functions.lit("str_"),
                                    functions.lpad(functions.col("id").cast("string"), 7, "0")
                            ).alias("string_col")
                    );
            df.coalesce(1)
              .write()
              .mode("overwrite")
              .parquet(tableDir.toAbsolutePath().toString());
        } finally {
            spark.stop();
        }
        return dir;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private long totalCountStar(String query) throws Exception {
        FlightInfo info = sqlClient.execute(query);
        long total = 0;
        for (FlightEndpoint ep : info.getEndpoints())
            total += streamCount(helper.flightClient(), ep.getTicket());
        return total;
    }

    private static long streamCount(FlightClient fc, Ticket ticket) throws Exception {
        long total = 0;
        try (FlightStream stream = fc.getStream(ticket)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                for (int i = 0; i < root.getRowCount(); i++) {
                    Object val = root.getVector(0).getObject(i);
                    if (val != null) total += ((Number) val).longValue();
                }
            }
        }
        return total;
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
        }
    }
}
