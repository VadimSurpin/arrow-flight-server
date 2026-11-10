import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.*;
public class Q88 {
    public static void main(String[] a) throws Exception {
        String q = Files.readString(Path.of(a[0]));
        int threads = Integer.parseInt(a[1]);
        Matcher m = Pattern.compile("ss_sold_time_sk IN \\(([^)]*)\\)").matcher(q); m.find();
        long[] v = Arrays.stream(m.group(1).split(",\\s*")).mapToLong(Long::parseLong).toArray();
        long lo = Arrays.stream(v).min().getAsLong(), hi = Arrays.stream(v).max().getAsLong();
        Map<String,String> variants = new LinkedHashMap<>();
        variants.put("IN list (" + v.length + ")", q);
        variants.put("BETWEEN " + lo + ".." + hi, q.replace(m.group(0), "ss_sold_time_sk BETWEEN " + lo + " AND " + hi));
        variants.put("no time filter", q.replace("AND (" + m.group(0) + ")", ""));
        try (Connection c0 = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c0.createStatement()) {
            s.execute("SET threads = " + threads); s.execute("SET preserve_insertion_order = false");
            s.execute("SET enable_external_file_cache = false"); s.execute("SET enable_object_cache = false"); s.execute("SET memory_limit = '3GB'");
            Class<?> k = Class.forName("pro.surpin.data.arrowflight.server.adapters.S3Settings"); Object o = k.getMethod("fromEnvironment").invoke(null);
            for (Object st : (List<?>) o.getClass().getMethod("duckDbStatements").invoke(o)) s.execute((String) st);
            s.execute("SET httpfs_connection_caching = true");
            for (int rep = 0; rep < 2; rep++) for (var e : variants.entrySet()) {
                List<String> work = new ArrayList<>();
                for (int scan = 0; scan < 8; scan++) for (int slice = 0; slice < 3; slice++) {
                    String files = IntStream.range(slice * 16, slice * 16 + 16).mapToObj(i -> String.format("'s3://learn/tpcds_sf100_asis/store_sales/part-%03d.parquet'", i)).collect(Collectors.joining(","));
                    work.add("SELECT count(*), sum(ss_hdemo_sk), sum(ss_store_sk) FROM (" + e.getValue().replace("tpcds_sf100_asis.store_sales", "read_parquet([" + files + "])") + ")");
                }
                var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
                ExecutorService ex = Executors.newFixedThreadPool(work.size());
                long t0 = System.nanoTime(), cpu0 = os.getProcessCpuTime();
                List<Future<Long>> fs = new ArrayList<>();
                for (String w : work) fs.add(ex.submit(() -> { try (Connection c = ((org.duckdb.DuckDBConnection) c0).duplicate(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(w)) { rs.next(); return rs.getLong(1); } }));
                long rows = 0; for (var f : fs) rows += f.get();
                System.out.printf("%-26s wall %.2fs cpu %.1fs rows/pod %,d%n", e.getKey(), (System.nanoTime() - t0) / 1e9, (os.getProcessCpuTime() - cpu0) / 1e9, rows);
                ex.shutdown();
            }
        }
    }
}
