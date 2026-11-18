import java.util.*;
import java.util.concurrent.*;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
public class Local {
    public static void main(String[] a) throws Exception {
        String uri = a[0]; String me = a[1];
        String[] sqls = {
          "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_store_sk, ss_ticket_number, ss_quantity FROM tpcds_sf100_asis.store_sales WHERE ((ss_customer_sk IS NOT NULL) AND (ss_item_sk IS NOT NULL) AND (ss_ticket_number IS NOT NULL))",
          "SELECT cs_item_sk, cs_sold_date_sk, cs_quantity, cs_bill_customer_sk FROM tpcds_sf100_asis.catalog_sales WHERE ((cs_bill_customer_sk IS NOT NULL) AND (cs_item_sk IS NOT NULL))",
          "SELECT sr_ticket_number, sr_return_quantity, sr_returned_date_sk, sr_item_sk, sr_customer_sk FROM tpcds_sf100_asis.store_returns WHERE ((sr_customer_sk IS NOT NULL) AND (sr_item_sk IS NOT NULL) AND (sr_ticket_number IS NOT NULL))"};
        try (RootAllocator alloc = new RootAllocator(); FlightClient cc = FlightClient.builder(alloc, new Location(uri)).build()) {
            var sql = new org.apache.arrow.flight.sql.FlightSqlClient(cc);
            List<FlightEndpoint> mine = new ArrayList<>(); List<String> tag = new ArrayList<>();
            for (int q = 0; q < sqls.length; q++) for (FlightEndpoint ep : sql.execute(sqls[q]).getEndpoints()) {
                String loc = ep.getLocations().isEmpty() ? uri : ep.getLocations().get(0).getUri().toString();
                if (loc.contains(me)) { mine.add(ep); tag.add(new String[]{"store_sales", "catalog_sales", "store_returns"}[q]); }
            }
            System.out.println(me + " local endpoints " + mine.size());
            for (int rep = 0; rep < 3; rep++) {
                ExecutorService ex = Executors.newFixedThreadPool(mine.size());
                long t0 = System.nanoTime(); List<Future<double[]>> fs = new ArrayList<>();
                for (FlightEndpoint ep : mine) fs.add(ex.submit(() -> {
                    long t = System.nanoTime(); long rows = 0;
                    try (FlightClient c = FlightClient.builder(alloc, new Location(uri)).build(); FlightStream s = c.getStream(ep.getTicket())) {
                        while (s.next()) rows += s.getRoot().getRowCount();
                    }
                    return new double[]{(System.nanoTime() - t) / 1e9, rows};
                }));
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < fs.size(); i++) { double[] r = fs.get(i).get(); sb.append(String.format("%s %.1fs; ", tag.get(i).substring(0, 5), r[0])); }
                System.out.printf("%s rep %d wall %.2fs | %s%n", me, rep, (System.nanoTime() - t0) / 1e9, sb);
                ex.shutdown();
            }
        }
    }
}
