import java.sql.*;
import java.util.*;
import java.util.stream.*;
public class Bench {
    public static void main(String[] a) throws Exception {
        int threads = Integer.parseInt(a[0]);
        int nfiles = Integer.parseInt(a[1]);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("SET threads = " + threads);
            s.execute("SET preserve_insertion_order = false");
            s.execute("SET streaming_buffer_size = '64MiB'");
            s.execute("SET enable_external_file_cache = false");
            s.execute("SET enable_object_cache = false");
            s.execute("SET memory_limit = '3GB'");
            Class<?> k = Class.forName("pro.surpin.data.arrowflight.server.adapters.S3Settings"); Object o = k.getMethod("fromEnvironment").invoke(null); for (Object st : (java.util.List<?>) o.getClass().getMethod("duckDbStatements").invoke(o)) s.execute((String) st);
            String files = IntStream.range(0, nfiles).mapToObj(i -> String.format("'s3://learn/tpcds_sf100_asis/store_sales/part-%03d.parquet'", i)).collect(Collectors.joining(","));
            String from = " FROM read_parquet([" + files + "]) WHERE ss_quantity >= 81 AND ss_quantity <= 100";
            Map<String,String> qs = new LinkedHashMap<>();
            qs.put("pushed (decimal38 cast)", "SELECT SUM(CAST(ss_net_paid AS DECIMAL(38,2))) AS a, COUNT(ss_net_paid) AS b" + from);
            qs.put("sum no cast", "SELECT SUM(ss_net_paid) AS a, COUNT(ss_net_paid) AS b" + from);
            qs.put("count(*) only", "SELECT COUNT(*) AS a" + from);
            for (var e : qs.entrySet()) {
                for (int r = 0; r < 3; r++) {
                    long t0 = System.nanoTime();
                    try (ResultSet rs = s.executeQuery(e.getValue())) { while (rs.next()) {} }
                    System.out.printf("threads=%d files=%d %-26s rep%d %.2fs%n", threads, nfiles, e.getKey(), r, (System.nanoTime() - t0) / 1e9);
                }
            }
            if (a.length > 2) {
                s.execute("PRAGMA enable_profiling = 'no_output'");
                try (ResultSet rs = s.executeQuery("EXPLAIN ANALYZE " + qs.get("pushed (decimal38 cast)"))) { while (rs.next()) System.out.println(rs.getString(2)); }
            }
        }
    }
}
