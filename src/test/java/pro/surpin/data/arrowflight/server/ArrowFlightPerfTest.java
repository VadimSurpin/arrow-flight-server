package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance comparison between embedded DuckDB and remote Arrow Flight reads.
 *
 * Can be run two ways:
 *   1. As a JUnit test:  mvn test -DexcludedGroups="" -Dgroups=perf
 *   2. As a main class:  java -cp <fat-jar> ...ArrowFlightPerfTest [data-dir] [schema] [table]
 *
 * Results are printed to stdout. For Spark-based comparison (requires SPARK_USER env var set
 * and Hadoop 3.4+ or a real cluster), see SparkPerfTest.
 */
@Tag("integration")
class ArrowFlightPerfTest {

    private static final int WARMUP_RUNS = 1;
    private static final int BENCH_RUNS  = Integer.getInteger("perf.runs", 3);
    private static final int PERF_DATA_SIZE = Integer.getInteger("perf.rows", 100_000);

    // ─── JUnit entry-point ───────────────────────────────────────────────

    @Test
    void compareLocalArrowVsArrowFlight() throws Exception {
        String genDir = generatePerfData("target/perf-data", "test_schema", "test_table");
        try {
            run(genDir, "test_schema", "test_table");
        } finally {
            deleteDir(new File("target/perf-data"));
        }
    }

    // ─── standalone entry-point ──────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String genDir = generatePerfData("target/perf-data", "test_schema", "test_table");
        try {
            run(genDir, "test_schema", "test_table");
        } finally {
            deleteDir(new File("target/perf-data"));
        }
    }

    // ─── benchmark logic ─────────────────────────────────────────────────

    static void run(String dataDir, String schema, String table) throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        System.setProperty("dataDir", dataDir);
        System.setProperty("duckDbWarmConnections", "1");
        System.setProperty("duckDbGroups", "1");
        System.setProperty("duckDbThreads", "1");
        System.setProperty("ioParallelism", "2");
        AppConfig appConfig = ConfigAdapter.getConfig();

        System.out.println("Data directory : " + dataDir);
        System.out.println("Table          : " + schema + "." + table);
        System.out.println();

        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dataDir)
                .start()) {

            int port = helper.location.getUri().getPort();
            System.out.println("Arrow Flight server started on port " + port);

            // ── build parquet paths for local DuckDB ──────────────────────
            String parquetDir = dataDir + "/" + schema + "/" + table;
            String[] parquetUris = findParquetFiles(helper.parquetAdapter.fileSystem(), parquetDir);
            System.out.println("Parquet files  : " + Arrays.toString(parquetUris));
            System.out.println();

            // ── Flight client ─────────────────────────────────────────────
            FlightSqlClient sqlClient = helper.sqlClient();

            // ── scenarios ─────────────────────────────────────────────────
            String flightTable = schema + "." + table;
            int batchSize = appConfig.batchSize();
            BufferAllocator allocator = helper.allocator;

            List<Scenario> scenarios = List.of(
                new Scenario("Full scan",
                    () -> localScan(allocator, parquetUris, null, batchSize),
                    () -> flightCount(sqlClient, helper.flightClient(),
                            "SELECT * FROM " + flightTable)),
                new Scenario("Filtered (tinyint_col = 0)",
                    () -> localScanWithFilter(allocator, parquetUris,
                            "\"tinyint_col\" = 0", batchSize),
                    () -> flightCount(sqlClient, helper.flightClient(),
                            "SELECT * FROM " + flightTable + " WHERE tinyint_col = 0")),
                new Scenario("Column projection (3 cols)",
                    () -> localScan(allocator, parquetUris,
                            new String[]{"id", "bool_col", "double_col"}, batchSize),
                    () -> flightCount(sqlClient, helper.flightClient(),
                            "SELECT id, bool_col, double_col FROM " + flightTable)),
                new Scenario("Filter + projection",
                    () -> localScanWithFilter(allocator, parquetUris,
                            "\"int_col\" < 5", batchSize),
                    () -> flightCount(sqlClient, helper.flightClient(),
                            "SELECT * FROM " + flightTable + " WHERE int_col < 5")),
                new Scenario("Multi-predicate AND",
                    () -> localScanWithFilter(allocator, parquetUris,
                            "\"id\" > 100 and \"id\" < 900", batchSize),
                    () -> flightCount(sqlClient, helper.flightClient(),
                            "SELECT * FROM " + flightTable + " WHERE id > 100 AND id < 900"))
            );

            // ── warmup ────────────────────────────────────────────────────
            System.out.println("Warming up (" + WARMUP_RUNS + " run(s))...");
            for (Scenario s : scenarios) {
                for (int i = 0; i < WARMUP_RUNS; i++) {
                    s.localRead.countRows();
                    s.flightRead.countRows();
                }
            }

            // ── benchmark ─────────────────────────────────────────────────
            System.out.println("Benchmarking (" + BENCH_RUNS + " run(s) each)...\n");
            List<BenchResult> results = new ArrayList<>();
            for (Scenario s : scenarios) {
                long[] localTimes  = new long[BENCH_RUNS];
                long[] flightTimes = new long[BENCH_RUNS];
                long rowCount = 0;

                for (int i = 0; i < BENCH_RUNS; i++) {
                    long t0 = System.nanoTime();
                    rowCount = s.localRead.countRows();
                    localTimes[i] = System.nanoTime() - t0;

                    t0 = System.nanoTime();
                    s.flightRead.countRows();
                    flightTimes[i] = System.nanoTime() - t0;
                }
                results.add(new BenchResult(s.label, median(localTimes) / 1_000_000, median(flightTimes) / 1_000_000, rowCount));
            }

            // ── print results ─────────────────────────────────────────────
            printResults(results);

            // ── validate correctness ──────────────────────────────────────
            for (BenchResult r : results) {
                assertTrue(r.localMs >= 0 && r.flightMs >= 0,
                        "Negative time in result: " + r.label);
            }
            System.out.println("\nAll scenarios completed successfully.");
        }
    }

    // ─── data generation ─────────────────────────────────────────────────

    static String generatePerfData(String baseDir, String schema, String table) throws Exception {
        String tableDir = baseDir + "/" + schema + "/" + table;
        new File(tableDir).mkdirs();
        String parquetPath = tableDir + "/gen.parquet";

        System.out.println("Generating " + PERF_DATA_SIZE + " rows of perf data...");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE gen AS SELECT" +
                    " range AS id," +
                    " (range % 2 = 0) AS bool_col," +
                    " CAST(range % 128 AS TINYINT) AS tinyint_col," +
                    " CAST(range % 32767 AS SMALLINT) AS smallint_col," +
                    " range % 1000000 AS int_col," +
                    " CAST(range AS BIGINT) AS bigint_col," +
                    " CAST(range AS REAL) AS float_col," +
                    " CAST(range AS DOUBLE) AS double_col," +
                    " 'd' || LPAD(CAST(range % 31 + 1 AS VARCHAR), 2, '0') AS date_string_col," +
                    " 'val_' || range AS string_col," +
                    " CAST(range * 1000000 AS BIGINT) AS timestamp_col," +
                    " CAST(range % 4 + 2022 AS INTEGER) AS year," +
                    " CAST(range % 12 + 1 AS INTEGER) AS month" +
                    " FROM range(0, " + PERF_DATA_SIZE + ")");
            stmt.execute("COPY gen TO '" + parquetPath + "' (FORMAT PARQUET)");
        }
        System.out.println("Generated: " + parquetPath);
        System.out.println();
        return new File(baseDir).getAbsolutePath();
    }

    static void deleteDir(File dir) {
        if (dir == null || !dir.exists()) return;
        try (Stream<Path> files = Files.walk(dir.toPath())) {
            files.sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException ignored) {
        }
    }

    // ─── reading helpers ─────────────────────────────────────────────────

    static long localScan(BufferAllocator allocator, String[] uris,
                          String[] columns, int batchSize) throws Exception {
        String projection = columns == null
                ? "*"
                : Arrays.stream(columns)
                        .map(column -> "\"" + column.replace("\"", "\"\"") + "\"")
                        .collect(java.util.stream.Collectors.joining(", "));
        return countWithDuckDb(allocator, uris, projection, null, batchSize);
    }

    static long localScanWithFilter(BufferAllocator allocator, String[] uris,
                                    String filterExpr, int batchSize) throws Exception {
        return countWithDuckDb(allocator, uris, "*", filterExpr, batchSize);
    }

    private static long countWithDuckDb(BufferAllocator allocator, String[] uris,
            String projection, String filter, int batchSize) throws Exception {
        String files = Arrays.stream(uris)
                .map(uri -> "'" + uri.replace("'", "''") + "'")
                .collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT " + projection + " FROM read_parquet([" + files + "])"
                + (filter == null ? "" : " WHERE " + filter);
        long count = 0;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement();
                org.duckdb.DuckDBResultSet result =
                        (org.duckdb.DuckDBResultSet) statement.executeQuery(sql);
                ArrowReader reader = (ArrowReader) result.arrowExportStream(allocator, batchSize)) {
            while (reader.loadNextBatch()) {
                count += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return count;
    }

    static long flightCount(FlightSqlClient sqlClient, FlightClient client, String query)
            throws Exception {
        long count = 0;
        FlightInfo info = sqlClient.execute(query);
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(ep.getTicket())) {
                while (stream.next()) {
                    count += stream.getRoot().getRowCount();
                }
            }
        }
        return count;
    }

    // ─── output ──────────────────────────────────────────────────────────

    static void printResults(List<BenchResult> results) {
        int W = 110;
        System.out.println("=".repeat(W));
        System.out.printf("%-45s | %14s | %14s | %12s | %8s%n",
                "Scenario", "Local Arrow ms", "Flight ms", "Delta ms", "Rows");
        System.out.println("-".repeat(W));
        for (BenchResult r : results) {
            System.out.printf("%-45s | %14d | %14d | %+12d | %8d%n",
                    r.label, r.localMs, r.flightMs, r.flightMs - r.localMs, r.rowCount);
        }
        System.out.println("=".repeat(W));
        System.out.println("Delta: positive = Flight slower (gRPC overhead), negative = Flight faster");
        System.out.println("Local Arrow = embedded DuckDB scan, same engine used by the server");
        System.out.println("Flight      = same scan served over gRPC from embedded server");
    }

    // ─── filesystem helpers ───────────────────────────────────────────────

    static String[] findParquetFiles(FileSystem fs, String dir) throws IOException {
        List<String> uris = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it =
                fs.listFiles(new org.apache.hadoop.fs.Path(dir), true);
        while (it.hasNext()) {
            LocatedFileStatus f = it.next();
            if (f.isFile() && f.getPath().getName().endsWith(".parquet")) {
                uris.add(f.getPath().toUri().toString());
            }
        }
        return uris.toArray(new String[0]);
    }

    static long median(long[] arr) {
        long[] copy = arr.clone();
        Arrays.sort(copy);
        return copy[copy.length / 2];
    }

    // ─── data types ───────────────────────────────────────────────────────

    @FunctionalInterface
    interface RowCounter { long countRows() throws Exception; }

    record Scenario(String label, RowCounter localRead, RowCounter flightRead) {}

    record BenchResult(String label, long localMs, long flightMs, long rowCount) {}
}
