import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
public class Pull {
    public static void main(String[] a) throws Exception {
        String uri = a[0];
        org.apache.arrow.memory.BufferAllocator ca = new RootAllocator();
        FlightClient cc = FlightClient.builder(ca, new Location(uri)).build();
        FlightInfo info = new org.apache.arrow.flight.sql.FlightSqlClient(cc).execute(a[2]);
        List<FlightEndpoint> eps = info.getEndpoints();
        System.out.println("endpoints " + eps.size());
        try (RootAllocator alloc = new RootAllocator()) {
            for (int rep = 0; rep < 3; rep++) {
                ExecutorService ex = Executors.newFixedThreadPool(eps.size());
                long t0 = System.nanoTime(); List<Future<double[]>> fs = new ArrayList<>();
                for (FlightEndpoint ep : eps) fs.add(ex.submit(() -> {
                    String loc = ep.getLocations().isEmpty() ? uri : ep.getLocations().get(0).getUri().toString();
                    long t = System.nanoTime(); long rows = 0; double first = -1;
                    try (FlightClient c = FlightClient.builder(alloc, new Location(loc)).build(); FlightStream s = c.getStream(ep.getTicket())) {
                        while (s.next()) { if (first < 0) first = (System.nanoTime() - t) / 1e9; rows += s.getRoot().getRowCount(); }
                    }
                    return new double[]{(System.nanoTime() - t) / 1e9, rows, first};
                }));
                StringBuilder sb = new StringBuilder(); double max = 0; long tot = 0;
                for (var f : fs) { double[] r = f.get(); max = Math.max(max, r[0]); tot += r[1]; sb.append(String.format("%.1f(%.1f) ", r[0], r[2])); }
                System.out.printf("rep %d: wall %.2fs rows %.1fM | per-endpoint total(first batch): %s%n", rep, (System.nanoTime() - t0) / 1e9, tot / 1e6, sb);
                ex.shutdown();
            }
        }
    }
}
