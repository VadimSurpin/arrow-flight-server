package pro.surpin.data.arrowflight.debug;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;

/**
 * Benchmarks Arrow Flight vs Spark direct-Parquet for several query patterns.
 *
 * <p>Modes:
 * <ul>
 *   <li>{@code --local} (default when no --server given): generates ~1 GB of local Parquet data,
 *       starts a single-node Flight server, runs all benchmark queries, then cleans up.</li>
 *   <li>{@code --server HOST --port PORT}: cluster mode — reads from HDFS paths hardcoded below.</li>
 * </ul>
 *
 * <p>Tuning: {@code --rows N} overrides the default row count (default 20 000 000).
 */
public class SparkArrowClientBenchmark {

    private static final String SCHEMA = "perf_schema";
    private static final String TABLE  = "perf_table";
    private static final String PORT_ARGUMENT = "--port";
    private static final String ARROW_COUNT_PREFIX = "SELECT count(*) FROM ";
    private static final String CLICKHOUSE_COUNT_PREFIX = "SELECT count() FROM ";

    // ── entry point ───────────────────────────────────────────────────────────

    /**
     * Entry point. Runs local or cluster benchmark based on flags.
     * @param args CLI arguments
     * @throws Exception on failure
     */
    public static void main(String[] args) throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());

        if (flag(args, "--server-only")) {
            runServerOnly(arg(args, "--datadir", "/tmp/arrow-perf"),
                    Integer.parseInt(arg(args, PORT_ARGUMENT, "32010")));
            return;
        }

        boolean localMode = flag(args, "--local") || !has(args, "--server");
        if (localMode) {
            int rows = Integer.parseInt(arg(args, "--rows", "20000000"));
            runLocalBenchmark(rows);
        } else {
            runClusterBenchmark(args);
        }
    }

    // ── local benchmark ───────────────────────────────────────────────────────

    /**
     * Runs full benchmark: generate data, start server JVM, execute queries, print results.
     * @param numRows number of rows to generate
     * @throws Exception on failure
     */
    private static void runLocalBenchmark(int numRows) throws Exception {
        Path dataDir = Files.createTempDirectory(
                Path.of(System.getProperty("user.home")), "arrow-perf-");
        Process serverProc = null;
        try {
            // 1. Generate data (client JVM — its own SparkSession)
            System.out.printf("%nGenerating %,d rows → %s%n", numRows, dataDir);
            SparkSession spark = SparkSession.builder()
                    .appName("ArrowFlightPerfTest")
                    .master("local[*]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.sql.shuffle.partitions", "8")
                    .getOrCreate();

            generateData(spark, numRows, dataDir);
            printDataSize(dataDir);

            // 2. Register Parquet view for the Spark direct-comparison queries
            spark.read()
                    .parquet(dataDir.resolve(SCHEMA + "/" + TABLE).toString())
                    .createOrReplaceTempView("perf_parquet");

            // 3. Start the Flight server in a SEPARATE JVM so it has its own heap
            //    and SparkSession — no GC / memory contention with data generation.
            int port = freePort();
            String jarPath = SparkArrowClientBenchmark.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI().getPath();
            String javaExe = ProcessHandle.current().info().command().orElse("java");

            List<String> cmd = new ArrayList<>(List.of(
                    javaExe, "-Xmx4g",
                    "--add-opens=java.base/java.lang=ALL-UNNAMED",
                    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
                    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                    "--add-opens=java.base/java.io=ALL-UNNAMED",
                    "--add-opens=java.base/java.net=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "--add-opens=java.base/java.util=ALL-UNNAMED",
                    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
                    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
                    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
                    "-cp", jarPath,
                    "pro.surpin.data.arrowflight.debug.SparkArrowClientBenchmark",
                    "--server-only",
                    "--datadir", dataDir.toAbsolutePath().toString(),
                    PORT_ARGUMENT, String.valueOf(port)
            ));

            System.out.printf("Starting server JVM on port %d...%n", port);
            serverProc = new ProcessBuilder(cmd)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();

            // The server prints "READY port=N" on stdout when it is accepting connections.
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(serverProc.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println("[server] " + line);
                    if (line.startsWith("READY")) {
                        break;
                    }
                }
            }
            if (!serverProc.isAlive()) {
                throw new IllegalStateException(
                        "Server process died before ready (exit=" + serverProc.exitValue() + ")");
            }

            // 4. Connect Arrow Flight client
            Location loc = Location.forGrpcInsecure("localhost", port);
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            FlightClient      client    = FlightClient.builder(allocator, loc).build();
            FlightSqlClient   sqlClient = new FlightSqlClient(client);

            try {
                // 5. ClickHouse glob covering all parquet files for this table
                String chGlob = "file('" + dataDir.resolve(SCHEMA + "/" + TABLE) + "/*.parquet', Parquet)";

                // 6. Warmup (JIT + page cache)
                System.out.println("\nWarming up Arrow Flight...");
                runArrow(sqlClient, client, ARROW_COUNT_PREFIX + SCHEMA + "." + TABLE);
                System.out.println("Warming up ClickHouse local...");
                timeClickHouseDirect(CLICKHOUSE_COUNT_PREFIX + chGlob);
                System.out.println("Warming up Spark...");
                spark.sql("SELECT count(*) FROM perf_parquet").collectAsList();

                // 7. Benchmarks
                System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
                System.out.println( "║                    PERFORMANCE RESULTS                        ║");
                System.out.println( "╚═══════════════════════════════════════════════════════════════╝");

                // arrowSql may be null for queries the Flight server doesn't support (e.g. JOIN)
                record BenchQuery(String label, String arrowSql, String chSql, String sparkSql) {}
                String chGlob2 = chGlob; // alias for JOIN queries referencing the same table twice
                List<BenchQuery> benchmarks = List.of(
                        new BenchQuery(
                                "count(*)",
                                ARROW_COUNT_PREFIX + SCHEMA + "." + TABLE,
                                CLICKHOUSE_COUNT_PREFIX + chGlob,
                                "SELECT count(*) FROM perf_parquet"),
                        new BenchQuery(
                                "count(*) GROUP BY tinyint_col",
                                ARROW_COUNT_PREFIX + SCHEMA + "." + TABLE + " GROUP BY tinyint_col",
                                "SELECT `tinyint_col`, count() FROM " + chGlob + " GROUP BY `tinyint_col`",
                                "SELECT count(*) FROM perf_parquet GROUP BY tinyint_col"),
                        new BenchQuery(
                                "sum(bigint_col)",
                                "SELECT sum(bigint_col) FROM " + SCHEMA + "." + TABLE,
                                "SELECT sum(`bigint_col`) FROM " + chGlob,
                                "SELECT sum(bigint_col) FROM perf_parquet"),
                        new BenchQuery(
                                "min(int_col), max(int_col)",
                                "SELECT min(int_col), max(int_col) FROM " + SCHEMA + "." + TABLE,
                                "SELECT min(`int_col`), max(`int_col`) FROM " + chGlob,
                                "SELECT min(int_col), max(int_col) FROM perf_parquet"),
                        new BenchQuery(
                                "count(*) WHERE tinyint_col = 3",
                                ARROW_COUNT_PREFIX + SCHEMA + "." + TABLE + " WHERE \"tinyint_col\" = 3",
                                CLICKHOUSE_COUNT_PREFIX + chGlob + " WHERE `tinyint_col` = 3",
                                "SELECT count(*) FROM perf_parquet WHERE tinyint_col = 3"),
                        new BenchQuery(
                                "JOIN t1.id = t2.tinyint_col",
                                null, // Arrow Flight doesn't support JOINs
                                CLICKHOUSE_COUNT_PREFIX + chGlob + " AS t1"
                                        + " INNER JOIN " + chGlob2 + " AS t2 ON t1.`id` = t2.`tinyint_col`",
                                "SELECT count(*) FROM perf_parquet t1 JOIN perf_parquet t2"
                                        + " ON t1.id = t2.tinyint_col")
                );

                List<Row> results = new ArrayList<>();
                for (BenchQuery bq : benchmarks) {
                    System.out.println("\n▶ " + bq.label());
                    long arrowMs = bq.arrowSql() != null
                            ? timeArrow(sqlClient, client, bq.arrowSql()) : -1;
                    long chMs    = timeClickHouseDirect(bq.chSql());
                    long sparkMs = timeSpark(spark, bq.sparkSql());
                    String arrowStr  = arrowMs >= 0 ? formatDuration(arrowMs) : "N/A";
                    String chVsSpark = chMs > 0
                            ? String.format("%.2f×", (double) sparkMs / chMs) : "—";
                    results.add(RowFactory.create(bq.label(),
                            arrowStr, formatDuration(chMs),
                            formatDuration(sparkMs), chVsSpark));
                }

                // 8. Summary table
                System.out.println();
                spark.createDataFrame(results, new StructType(new StructField[]{
                        new StructField("query",            DataTypes.StringType, false, Metadata.empty()),
                        new StructField("arrow_flight",     DataTypes.StringType, false, Metadata.empty()),
                        new StructField("clickhouse_local", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("spark_parquet",    DataTypes.StringType, false, Metadata.empty()),
                        new StructField("ch_vs_spark",      DataTypes.StringType, false, Metadata.empty()),
                })).show(20, false);

            } finally {
                sqlClient.close();
                client.close();
                allocator.close();
                spark.stop();
            }
        } finally {
            if (serverProc != null) {
                serverProc.destroyForcibly();
            }
            deleteTree(dataDir);
        }
    }

    // ── server-only mode (used as subprocess by runLocalBenchmark) ────────────

    /**
     * Starts a standalone Flight server (used as subprocess by runLocalBenchmark).
     * @param dataDir path to Parquet data directory
     * @param port server port
     * @throws Exception on failure
     */
    private static void runServerOnly(String dataDir, int port) throws Exception {
        LocalFileSystem fs = newLocalFs();
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Location loc = Location.forGrpcInsecure("localhost", port);

        System.setProperty("dataDir", dataDir);
        System.setProperty("hazelcastPort", String.valueOf(port + 1));
        AppConfig appConfig = ConfigAdapter.getConfig();

        ParquetAdapter parquetAdapter = new ParquetAdapter(appConfig, fs);
        HazelcastAdapter hazelcastAdapter = new HazelcastAdapter(appConfig);
        DuckDbAdapter duckDbAdapter = new DuckDbAdapter(appConfig, Executors.newCachedThreadPool());
        MetadataService metadataService = new MetadataService(parquetAdapter);
        ClusterService clusterService = new ClusterService(hazelcastAdapter, appConfig,
                loc.getUri().toString());
        QueryPlanner queryPlanner = new QueryPlanner(parquetAdapter, clusterService);

        ExecutionService executionService = new ExecutionService(parquetAdapter, duckDbAdapter,
                metadataService, appConfig, Executors.newCachedThreadPool());
        FlightSqlProducer flightSqlProducer = new FlightSqlProducer(loc, allocator,
                metadataService, queryPlanner, executionService, clusterService);

        FlightServer server = FlightServer.builder(allocator, loc, flightSqlProducer).build();
        server.start();
        System.out.printf("READY port=%d%n", port);
        System.out.flush();
        server.awaitTermination();
        hazelcastAdapter.close();
        allocator.close();
    }

    // ── cluster benchmark (original behaviour) ────────────────────────────────

    /**
     * Runs benchmark against a remote Flight server.
     * @param args CLI arguments containing --server and --port
     */
    private static void runClusterBenchmark(String[] args) {
        String server = arg(args, "--server", "127.0.0.1");
        int    port   = Integer.parseInt(arg(args, PORT_ARGUMENT, "32010"));

        SparkSession spark = SparkSession.builder()
                .appName("ArrowFlight Client")
                .master("local[*]")
                .config("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
                .getOrCreate();

//        Dataset<Row> parquetDF = spark.read()
//                .parquet("hdfs:///data/vsurpin/test_db/test_schema/test_1tb_table");
//        parquetDF.createOrReplaceTempView("parquet_table");

        Dataset<Row> flightDF = spark.read()
                .format("flight")
                .option("host", server)
                .option("port", port)
                .option("user", "user")
                .option("password", benchmarkPassword())
                .option("tls.enabled", false)
                .option("table", "test_schema.test_1tb_table")
                .load();
        flightDF.createOrReplaceTempView("flight_table");

        spark.sql("select count(*) from flight_table t1, flight_table t2 where t1.id = t2.tinyint_col").show();
//
//        List<Row> results = new ArrayList<>();
//        results.add(RowFactory.create("select * where id=0",
//                timeOf(() -> spark.sql("select * from flight_table where id = 0").show()),
//                timeOf(() -> spark.sql("select * from parquet_table where id = 0").show())));
//        results.add(RowFactory.create("count(*)",
//                timeOf(() -> spark.sql("select count(*) from flight_table").show()),
//                timeOf(() -> spark.sql("select count(*) from parquet_table").show())));
//        results.add(RowFactory.create("count(*) group by tinyint_col",
//                timeOf(() -> spark.sql("select count(*) from flight_table group by tinyint_col").show()),
//                timeOf(() -> spark.sql("select count(*) from parquet_table group by tinyint_col").show())));
//
//        spark.createDataFrame(results, new StructType(new StructField[]{
//                new StructField("description",    DataTypes.StringType, true, Metadata.empty()),
//                new StructField("flightDuration", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("parquetDuration",DataTypes.StringType, true, Metadata.empty()),
//        })).show(false);
    }

    // ── data generation ───────────────────────────────────────────────────────

    /**
     * Generates Parquet test data with various column types.
     * @param spark SparkSession
     * @param numRows number of rows to generate
     * @param dataDir output directory
     */
    private static void generateData(SparkSession spark, int numRows, Path dataDir) {
        Path tableDir = dataDir.resolve(SCHEMA + "/" + TABLE);
        spark.range(0, numRows, 1, 8)
                .select(
                        functions.col("id").cast("integer").alias("id"),
                        functions.col("id").mod(2).equalTo(0).alias("bool_col"),
                        functions.col("id").mod(10).cast("tinyint").alias("tinyint_col"),
                        functions.col("id").mod(100).cast("smallint").alias("smallint_col"),
                        functions.col("id").mod(10000).cast("integer").alias("int_col"),
                        functions.col("id").multiply(7L).cast("bigint").alias("bigint_col"),
                        functions.rand(42).cast("float").alias("float_col"),
                        functions.rand(43).cast("double").alias("double_col"),
                        functions.concat(
                                functions.lit("str_"),
                                functions.lpad(functions.col("id").cast("string"), 8, "0")
                        ).alias("string_col")
                )
                .write()
                .mode("overwrite")
                .parquet(tableDir.toAbsolutePath().toString());
    }

    /**
     * Prints total disk size of generated data.
     * @param dataDir data directory
     * @throws IOException on filesystem error
     */
    private static void printDataSize(Path dataDir) throws IOException {
        long[] size = {0};
        Files.walkFileTree(dataDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path f, BasicFileAttributes a) {
                size[0] += a.size();
                return FileVisitResult.CONTINUE;
            }
        });
        System.out.printf("Data size on disk: %.1f MB%n", size[0] / 1_048_576.0);
    }

    // ── timing helpers ────────────────────────────────────────────────────────

    /**
     * Times query execution via Arrow Flight SQL.
     * @param sql FlightSqlClient
     * @param client FlightClient
     * @param query SQL query string
     * @return elapsed time in milliseconds
     * @throws Exception on failure
     */
    private static long timeArrow(FlightSqlClient sql, FlightClient client, String query) throws Exception {
        long start = System.nanoTime();
        long resultRows = runArrow(sql, client, query);
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("  Arrow Flight  : %s  (%,d result rows)%n", formatDuration(ms), resultRows);
        return ms;
    }

    /**
     * Times query execution via clickhouse-local CLI.
     * @param sql ClickHouse SQL query
     * @return elapsed time in milliseconds
     * @throws Exception on failure
     */
    private static long timeClickHouseDirect(String sql) throws Exception {
        long start = System.nanoTime();
        Process proc = new ProcessBuilder("clickhouse", "local",
                "--query", sql, "--format", "TabSeparated")
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        long resultRows = 0;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(proc.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultRows++;
            }
        }
        proc.waitFor();
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("  ClickHouse Local: %s  (%,d result rows)%n", formatDuration(ms), resultRows);
        return ms;
    }

    /**
     * Times query execution via Spark SQL on Parquet.
     * @param spark SparkSession
     * @param query SQL query string
     * @return elapsed time in milliseconds
     */
    private static long timeSpark(SparkSession spark, String query) {
        long start = System.nanoTime();
        List<Row> rows = spark.sql(query).collectAsList();
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("  Spark Parquet : %s  (%,d result rows)%n", formatDuration(ms), rows.size());
        return ms;
    }

    /** Executes query via FlightSqlClient, consumes all result batches, returns number of result rows. */
    private static long runArrow(FlightSqlClient sql, FlightClient client, String query) throws Exception {
        FlightInfo info = sql.execute(query);
        long resultRows = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(ep.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    resultRows += root.getRowCount();
                }
            }
        }
        return resultRows;
    }

    // ── original cluster-mode helper ──────────────────────────────────────────

    /**
     * Times a Runnable task and returns formatted duration.
     * @param task task to run
     * @return formatted duration string
     */
    private static String timeOf(Runnable task) {
        long start = System.nanoTime();
        task.run();
        return formatDuration((System.nanoTime() - start) / 1_000_000);
    }

    // ── formatting / infra ────────────────────────────────────────────────────

    /**
     * Formats milliseconds into human-readable duration string.
     * @param ms duration in milliseconds
     * @return formatted string (ms, s, or m s)
     */
    private static String formatDuration(long ms) {
        if (ms < 1_000) {
            return ms + " ms";
        }
        if (ms < 60_000) {
            return String.format("%.2f s", ms / 1_000.0);
        }
        long min = ms / 60_000;
        double sec = (ms % 60_000) / 1_000.0;
        return String.format("%d m %.2f s", min, sec);
    }

    /**
     * Creates a local Hadoop filesystem instance.
     * @return LocalFileSystem
     * @throws IOException on failure
     */
    private static LocalFileSystem newLocalFs() throws IOException {
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(URI.create("file:///"), new Configuration());
        return fs;
    }

    /**
     * Finds a free TCP port on localhost.
     * @return available port number
     * @throws IOException on socket error
     */
    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /**
     * Recursively deletes a directory tree.
     * @param dir directory to delete
     * @throws IOException on filesystem error
     */
    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException ignored) {
                    }
                });
    }

    /**
     * Gets the value of a named CLI argument.
     * @param args CLI arguments array
     * @param key argument name (e.g. --port)
     * @param def default value if not found
     * @return argument value or default
     */
    private static String arg(String[] args, String key, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return def;
    }

    /**
     * Checks if a boolean flag is present in CLI arguments.
     * @param args CLI arguments array
     * @param key flag name (e.g. --local)
     * @return true if flag is present
     */
    private static boolean flag(String[] args, String key) {
        for (String a : args) {
            if (a.equals(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a key is present anywhere in CLI arguments.
     * @param args CLI arguments array
     * @param key key to search for
     * @return true if key is found
     */
    private static boolean has(String[] args, String key) {
        for (String a : args) {
            if (a.equals(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads the benchmark password from a runtime-only configuration source.
     * @return configured benchmark password
     * @throws IllegalStateException when no password is configured
     */
    private static String benchmarkPassword() {
        String password = System.getProperty("arrowflight.benchmark.password");
        if (password == null || password.isEmpty()) {
            password = System.getenv("ARROW_FLIGHT_BENCHMARK_PASSWORD");
        }
        if (password == null || password.isEmpty()) {
            throw new IllegalStateException("Configure arrowflight.benchmark.password or ARROW_FLIGHT_BENCHMARK_PASSWORD");
        }
        return password;
    }
}
