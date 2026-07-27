package net.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.*;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test for Q1 query behavior through the Flight protocol (server side).
 *
 * <p>Starts an embedded Flight server with a tiny TPC-H lineitem Parquet file
 * and runs Q1 partial aggregates directly via Flight SQL. This exercises
 * the SUM and COUNT representation produced by Spark aggregation pushdown.
 *
 * <p>Usage:
 * <pre>
 * python generate_test_lineitem.py
 * mvn test -Dtest=FlightQ1DebugTest -DexcludedGroups="" -Dorg.slf4j.simpleLogger.defaultLogLevel=info
 * </pre>
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightQ1DebugTest {

    private static TestFlightServerHelper helper;
    private static FlightSqlClient sqlClient;

    @BeforeAll
    static void startAll() throws Exception {
        String dataDir = Paths.get("src/test/resources/lineitem_tiny").toAbsolutePath().toString();
        System.out.println("=== TestFlightServerHelper dataDir=" + dataDir);
        helper = TestFlightServerHelper.builder()
                .dataDir(dataDir)
                .start();
        sqlClient = helper.sqlClient();
        System.out.println("=== Server started at " + helper.location.getUri());
        System.out.println();
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) helper.close();
        System.out.println("=== Server stopped");
    }

    /**
     * Runs the partial aggregate representation of TPC-H Q1.
     * Spark reconstructs AVG from the corresponding SUM and COUNT columns.
     */
    @Test
    @Order(1)
    void q1PartialAggregatesViaDirectFlightSql() throws Exception {
        String query = """
                SELECT
                  l_returnflag,
                  l_linestatus,
                  SUM(l_quantity) AS sum_qty,
                  SUM(l_extendedprice) AS sum_base_price,
                  SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price,
                  SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge,
                  SUM(l_quantity) AS avg_qty_sum,
                  COUNT(l_quantity) AS avg_qty_count,
                  SUM(l_extendedprice) AS avg_price_sum,
                  COUNT(l_extendedprice) AS avg_price_count,
                  SUM(l_discount) AS avg_disc_sum,
                  COUNT(l_discount) AS avg_disc_count,
                  COUNT(*) AS count_order
                FROM tpch.lineitem
                WHERE l_shipdate <= '1998-12-01'
                GROUP BY l_returnflag, l_linestatus""";

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║  TEST 1: Q1 partials via direct Flight SQL      ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("SQL:");
        System.out.println(query);
        System.out.println();

        long t0 = System.nanoTime();
        FlightInfo info = sqlClient.execute(query);

        System.out.println("FlightInfo:");
        System.out.println("  schema fields: " + info.getSchema().getFields().size());
        System.out.println("  endpoints: " + info.getEndpoints().size());
        for (int i = 0; i < info.getEndpoints().size(); i++) {
            FlightEndpoint ep = info.getEndpoints().get(i);
            System.out.println("    [" + i + "] ticket=" + bytesToHex(ep.getTicket().getBytes())
                    + " locations=" + ep.getLocations());
        }
        System.out.println();

        long totalRows = 0;
        int epIdx = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            long epStart = System.nanoTime();
            System.out.println("--- Endpoint " + (epIdx++) + " ---");
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                int batchIdx = 0;
                int epRows = 0;
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    int rows = root.getRowCount();
                    epRows += rows;
                    totalRows += rows;
                    if (batchIdx < 3) {
                        System.out.println("  batch " + batchIdx + ": " + rows + " rows, "
                                + root.getFieldVectors().size() + " cols");
                    }
                    batchIdx++;
                }
                double epTime = (System.nanoTime() - epStart) / 1e9;
                System.out.println("  " + epRows + " rows across " + batchIdx + " batches, "
                        + String.format("%.3f s", epTime));
            }
        }

        double totalTime = (System.nanoTime() - t0) / 1e9;
        System.out.println();
        System.out.println("RESULT: " + totalRows + " rows in " + String.format("%.3f s", totalTime));
        assertTrue(totalRows > 0, "Q1 must return rows");

        // Print the actual aggregated values
        System.out.println();
        printResults(info, "Q1 partial aggregate results");
    }

    /**
     * Simplified Q1 with only pushdown-eligible aggregates (no AVG, no complex expressions).
     * This is what Spark FlightSource would push to the server after pushAggregation().
     * Note: SUM of decimal(15,2) is still rejected by safeSumType(), so even this
     * simplified query wouldn't be pushed by the current client.
     */
    @Test
    @Order(2)
    void simplifiedQ1PushdownEligible() throws Exception {
        // MIN/MAX/COUNT(*)/COUNT(col) are the only fully pushdown-eligible functions.
        // SUM(col) only for Float/Double columns.
        // AVG is not supported at all.
        // Complex expressions are rejected.
        String query = """
                SELECT
                  l_returnflag,
                  l_linestatus,
                  COUNT(*) AS count_order,
                  COUNT(l_quantity) AS count_qty,
                  MIN(l_quantity) AS min_qty,
                  MAX(l_quantity) AS max_qty
                FROM tpch.lineitem
                WHERE l_shipdate <= '1998-12-01'
                GROUP BY l_returnflag, l_linestatus""";

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║  TEST 2: Simplified Q1 (pushdown-eligible)      ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("SQL:");
        System.out.println(query);
        System.out.println("""
                NOTE: Even COUNT/MIN/MAX are only pushdown-eligible
                when Spark's pushAggregation() accepts them.
                This test shows the server CAN execute them.
                """);

        long t0 = System.nanoTime();
        FlightInfo info = sqlClient.execute(query);
        long totalRows = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        double totalTime = (System.nanoTime() - t0) / 1e9;
        System.out.println("RESULT: " + totalRows + " rows in " + String.format("%.3f s", totalTime));

        printResults(info, "Simplified Q1 results");
    }

    /**
     * Raw SELECT * - shows what Spark falls back to when pushdown is rejected.
     * All rows, all columns streamed from server to client.
     */
    @Test
    @Order(3)
    void selectAllRawData() throws Exception {
        String query = """
                SELECT l_orderkey, l_quantity, l_extendedprice, l_discount, l_tax,
                  l_returnflag, l_linestatus, l_shipdate
                FROM tpch.lineitem
                WHERE l_shipdate <= '1998-12-01'""";

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║  TEST 3: Raw SELECT (what Spark falls back to)  ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("SQL:");
        System.out.println(query);
        System.out.println("""
                This is what Spark reads when pushAggregation() returns false.
                All rows streamed, Spark aggregates locally.
                """);

        long t0 = System.nanoTime();
        FlightInfo info = sqlClient.execute(query);
        long totalRows = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        double totalTime = (System.nanoTime() - t0) / 1e9;
        System.out.println("RESULT: " + totalRows + " rows"
                + " (" + info.getSchema().getFields().size() + " cols)"
                + " in " + String.format("%.3f s", totalTime));
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private void printResults(FlightInfo info, String label) throws Exception {
        System.out.println("--- " + label + " ---");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(ep.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    List<String> colNames = root.getSchema().getFields().stream()
                            .map(f -> f.getName()).toList();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int c = 0; c < root.getFieldVectors().size(); c++) {
                            FieldVector vec = root.getVector(c);
                            row.put(colNames.get(c), vec.isNull(r) ? null : vec.getObject(r));
                        }
                        rows.add(row);
                    }
                }
            }
        }
        for (Map<String, Object> row : rows) {
            System.out.println("  " + row);
        }
        System.out.println("  (" + rows.size() + " rows total)");
        System.out.println();
    }

    private static String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "empty";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
