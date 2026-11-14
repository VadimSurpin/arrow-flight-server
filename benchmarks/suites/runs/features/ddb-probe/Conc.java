import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
public class Conc {
    public static void main(String[] a) throws Exception {
        int threads = Integer.parseInt(a[0]);
        String mem = a[1];
        try (Connection c0 = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c0.createStatement()) {
            s.execute("SET threads = " + threads);
            s.execute("SET preserve_insertion_order = false");
            s.execute("SET streaming_buffer_size = '64MiB'");
            s.execute("SET enable_external_file_cache = false");
            s.execute("SET enable_object_cache = false");
            s.execute("SET memory_limit = '" + mem + "'");
            Class<?> k = Class.forName("pro.surpin.data.arrowflight.server.adapters.S3Settings"); Object o = k.getMethod("fromEnvironment").invoke(null);
            for (Object st : (List<?>) o.getClass().getMethod("duckDbStatements").invoke(o)) s.execute((String) st);
            String[] cols = {"SUM(CAST(ss_net_paid AS DECIMAL(38,2))) AS a, COUNT(ss_net_paid) AS b", "SUM(CAST(ss_ext_discount_amt AS DECIMAL(38,2))) AS a, COUNT(ss_ext_discount_amt) AS b", "COUNT(*) AS a"};
            List<String> work = new ArrayList<>();
            for (int scan = 0; scan < 15; scan++) for (int slice = 0; slice < 3; slice++) {  // one pod's share of q09: 15 scans x 3 endpoints
                String files = IntStream.range(slice * 16, slice * 16 + 16).mapToObj(i -> String.format("'s3://learn/tpcds_sf100_asis/store_sales/part-%03d.parquet'", i)).collect(Collectors.joining(","));
                int lo = 1 + 20 * (scan % 5);
                work.add("SELECT " + cols[scan % 3] + " FROM read_parquet([" + files + "]) WHERE ss_quantity >= " + lo + " AND ss_quantity <= " + (lo + 19));
            }
            for (int par : java.util.Arrays.stream(a[2].split(",")).mapToInt(Integer::parseInt).toArray()) {
                ExecutorService ex = Executors.newFixedThreadPool(par);
                long t0 = System.nanoTime(); long cpu0 = ((com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
                List<Future<Double>> fs = new ArrayList<>();
                for (String q : work) fs.add(ex.submit(() -> {
                    try (Connection c = ((org.duckdb.DuckDBConnection) c0).duplicate(); Statement st = c.createStatement()) {
                        long t = System.nanoTime();
                        try (ResultSet rs = st.executeQuery(q)) { while (rs.next()) {} }
                        return (System.nanoTime() - t) / 1e9;
                    }
                }));
                double sum = 0, max = 0;
                for (var f : fs) { double d = f.get(); sum += d; max = Math.max(max, d); }
                double cpu = (((com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() - cpu0) / 1e9;
                System.out.printf("cpu %.1fs; threads=%d mem=%s parallel=%d: wall %.2fs, mean query latency %.2fs, max %.2fs%n", cpu, threads, mem, par, (System.nanoTime() - t0) / 1e9, sum / work.size(), max);
                ex.shutdown();
            }
        }
    }
}
