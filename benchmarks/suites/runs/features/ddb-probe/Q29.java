import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBResultSet;
public class Q29 {
    static String files(String t, int from, int n) { return IntStream.range(from, from + n).mapToObj(i -> String.format("'s3://learn/tpcds_sf100_asis/%s/part-%03d.parquet'", t, i)).collect(Collectors.joining(",")); }
    public static void main(String[] a) throws Exception {
        int threads = Integer.parseInt(a[0]); String mode = a[1];
        try (Connection c0 = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c0.createStatement(); RootAllocator alloc = new RootAllocator(4L << 30)) {
            s.execute("SET threads = " + threads); s.execute("SET preserve_insertion_order = false"); s.execute("SET streaming_buffer_size = '64MiB'");
            s.execute("SET enable_external_file_cache = false"); s.execute("SET enable_object_cache = false"); s.execute("SET memory_limit = '5GB'");
            Class<?> k = Class.forName("pro.surpin.data.arrowflight.server.adapters.S3Settings"); Object o = k.getMethod("fromEnvironment").invoke(null);
            for (Object st : (List<?>) o.getClass().getMethod("duckDbStatements").invoke(o)) s.execute((String) st);
            s.execute("SET httpfs_connection_caching = true");
            List<String[]> work = new ArrayList<>();
            for (int sl = 0; sl < 3; sl++) {
                work.add(new String[]{"store_sales", "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_store_sk, ss_ticket_number, ss_quantity FROM read_parquet([" + files("store_sales", 16 * sl, 16) + "]) WHERE ss_customer_sk IS NOT NULL AND ss_item_sk IS NOT NULL AND ss_ticket_number IS NOT NULL"});
                work.add(new String[]{"catalog_sales", "SELECT cs_item_sk, cs_sold_date_sk, cs_quantity, cs_bill_customer_sk FROM read_parquet([" + files("catalog_sales", 8 * sl, 8) + "]) WHERE cs_bill_customer_sk IS NOT NULL AND cs_item_sk IS NOT NULL"});
                work.add(new String[]{"store_returns", "SELECT sr_ticket_number, sr_return_quantity, sr_returned_date_sk, sr_item_sk, sr_customer_sk FROM read_parquet([" + files("store_returns", 2 * sl, 2) + "]) WHERE sr_customer_sk IS NOT NULL AND sr_item_sk IS NOT NULL AND sr_ticket_number IS NOT NULL"});
            }
            for (int rep = 0; rep < 3; rep++) {
                var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
                ExecutorService ex = Executors.newFixedThreadPool(work.size());
                long t0 = System.nanoTime(), cpu0 = os.getProcessCpuTime();
                List<Future<double[]>> fs = new ArrayList<>();
                for (String[] w : work) fs.add(ex.submit(() -> {
                    long t = System.nanoTime(); long rows = 0;
                    try (Connection c = ((org.duckdb.DuckDBConnection) c0).duplicate(); Statement st = c.createStatement()) {
                        if (mode.equals("count")) { try (ResultSet rs = st.executeQuery("SELECT count(*) FROM (" + w[1] + ")")) { rs.next(); rows = rs.getLong(1); } }
                        else try (ResultSet rs = st.executeQuery(w[1]); ArrowReader r = (ArrowReader) ((DuckDBResultSet) rs).arrowExportStream(alloc, 65536)) { while (r.loadNextBatch()) rows += r.getVectorSchemaRoot().getRowCount(); }
                    }
                    return new double[]{(System.nanoTime() - t) / 1e9, rows};
                }));
                Map<String, double[]> agg = new TreeMap<>(); int i = 0;
                for (var f : fs) { double[] r = f.get(); double[] x = agg.computeIfAbsent(work.get(i++)[0], z -> new double[3]); x[0] = Math.max(x[0], r[0]); x[1] += r[1]; }
                StringBuilder sb = new StringBuilder();
                for (var e : agg.entrySet()) sb.append(String.format(" %s: max %.1fs %.1fM rows;", e.getKey(), e.getValue()[0], e.getValue()[1] / 1e6));
                System.out.printf("%s wall %.2fs cpu %.1fs |%s%n", mode, (System.nanoTime() - t0) / 1e9, (os.getProcessCpuTime() - cpu0) / 1e9, sb);
                ex.shutdown();
            }
        }
    }
}
