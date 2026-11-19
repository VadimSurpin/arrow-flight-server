import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBResultSet;
public class Func {
    static String files(String t, int from, int n) { return IntStream.range(from, from + n).mapToObj(i -> String.format("'s3://learn/tpcds_sf100_asis/%s/part-%03d.parquet'", t, i)).collect(Collectors.joining(",")); }
    public static void main(String[] a) throws Exception {
        int threads = Integer.parseInt(a[0]); String extra = a[1];
        try (Connection c0 = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c0.createStatement(); RootAllocator alloc = new RootAllocator(4L << 30)) {
            s.execute("SET threads = " + threads); s.execute("SET preserve_insertion_order = false"); s.execute("SET streaming_buffer_size = '64MiB'");
            s.execute("SET enable_external_file_cache = false"); s.execute("SET enable_object_cache = false"); s.execute("SET memory_limit = '5GB'");
            Class<?> k = Class.forName("pro.surpin.data.arrowflight.server.adapters.S3Settings"); Object o = k.getMethod("fromEnvironment").invoke(null);
            for (Object st : (List<?>) o.getClass().getMethod("duckDbStatements").invoke(o)) s.execute((String) st);
            s.execute("SET httpfs_connection_caching = true");
            String base = "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_store_sk, ss_ticket_number, ss_quantity FROM read_parquet([%s]) WHERE ss_customer_sk IS NOT NULL AND ss_item_sk IS NOT NULL AND ss_ticket_number IS NOT NULL";
            List<String> work = new ArrayList<>();
            for (int sl = 0; sl < 3; sl++) work.add(String.format(base, files("store_sales", 16 * sl, 16)) + (extra.equals("none") ? "" : " AND " + extra));
            if (a.length > 2) { try (ResultSet rs = s.executeQuery("EXPLAIN " + work.get(0))) { while (rs.next()) System.out.println(rs.getString(2)); } }
            for (int rep = 0; rep < 3; rep++) {
                var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
                ExecutorService ex = Executors.newFixedThreadPool(work.size());
                long t0 = System.nanoTime(), cpu0 = os.getProcessCpuTime();
                List<Future<Long>> fs = new ArrayList<>();
                for (String w : work) fs.add(ex.submit(() -> {
                    long rows = 0;
                    try (Connection c = ((org.duckdb.DuckDBConnection) c0).duplicate(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(w); ArrowReader r = (ArrowReader) ((DuckDBResultSet) rs).arrowExportStream(alloc, 65536)) { while (r.loadNextBatch()) rows += r.getVectorSchemaRoot().getRowCount(); }
                    return rows;
                }));
                long rows = 0; for (var f : fs) rows += f.get();
                System.out.printf("%s: wall %.2fs cpu %.1fs rows %.1fM%n", extra, (System.nanoTime() - t0) / 1e9, (os.getProcessCpuTime() - cpu0) / 1e9, rows / 1e6);
                ex.shutdown();
            }
        }
    }
}
