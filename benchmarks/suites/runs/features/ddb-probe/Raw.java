import java.sql.*;
import java.util.*;
import java.util.stream.*;
public class Raw {
    public static void main(String[] a) throws Exception {
        int threads = Integer.parseInt(a[0]); int nfiles = Integer.parseInt(a[1]); int from = Integer.parseInt(a[2]);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("SET threads = " + threads);
            s.execute("SET enable_external_file_cache = false");
            s.execute("SET memory_limit = '3GB'");
            Class<?> k = Class.forName("pro.surpin.data.arrowflight.server.adapters.S3Settings"); Object o = k.getMethod("fromEnvironment").invoke(null);
            for (Object st : (List<?>) o.getClass().getMethod("duckDbStatements").invoke(o)) s.execute((String) st);
            String files = IntStream.range(from, from + nfiles).mapToObj(i -> String.format("'s3://learn/tpcds_sf100_asis/store_sales/part-%03d.parquet'", i)).collect(Collectors.joining(","));
            for (int r = 0; r < 2; r++) {
                long t0 = System.nanoTime(); long bytes = 0;
                try (ResultSet rs = s.executeQuery("SELECT sum(size) FROM (SELECT octet_length(content) AS size FROM read_blob([" + files + "]))")) { rs.next(); bytes = rs.getLong(1); }
                double w = (System.nanoTime() - t0) / 1e9;
                System.out.printf("read_blob threads=%d files=%d: %.2f GB in %.2fs = %.2f GB/s%n", threads, nfiles, bytes / 1e9, w, bytes / 1e9 / w);
            }
        }
    }
}
