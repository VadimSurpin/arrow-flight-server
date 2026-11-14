import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
public class Stats {
    public static void main(String[] a) throws Exception {
        String[][] t = {{"store_sales", "ss_sold_date_sk,ss_item_sk,ss_customer_sk,ss_store_sk,ss_ticket_number,ss_quantity"}, {"catalog_sales", "cs_item_sk,cs_sold_date_sk,cs_quantity,cs_bill_customer_sk"}, {"store_returns", "sr_ticket_number,sr_return_quantity,sr_returned_date_sk,sr_item_sk,sr_customer_sk"}, {"item", "i_item_sk,i_item_id"}, {"date_dim", "d_date_sk,d_year"}, {"store", "s_store_sk"}};
        try (RootAllocator alloc = new RootAllocator(); FlightClient c = FlightClient.builder(alloc, new Location(a[0])).build()) {
            var sql = new org.apache.arrow.flight.sql.FlightSqlClient(c);
            for (int rep = 0; rep < Integer.parseInt(a[1]); rep++) {
                StringBuilder sb = new StringBuilder();
                for (String[] x : t) {
                    long t0 = System.nanoTime();
                    String cols = "[\"" + x[1].replace(",", "\",\"") + "\"]";
                    Iterator<Result> r = c.doAction(new Action("arrowflight.table_statistics", ("{\"schema\":\"tpcds_sf100_asis\",\"table\":\"" + x[0] + "\",\"columns\":" + cols + "}").getBytes(StandardCharsets.UTF_8)));
                    if (r.hasNext()) r.next();
                    sb.append(String.format("%s %.2fs  ", x[0], (System.nanoTime() - t0) / 1e9));
                }
                long t0 = System.nanoTime();
                try { var info = sql.execute("SELECT ss_store_sk FROM tpcds_sf100_asis.store_sales GROUP BY ss_store_sk"); var s = c.getStream(info.getEndpoints().get(0).getTicket()); while (s.next()) {} s.close(); } catch (Exception e) { sb.append("distinct-failed:" + e.getMessage()); }
                sb.append(String.format("distinctKeys(ss_store_sk) %.2fs", (System.nanoTime() - t0) / 1e9));
                System.out.println("stats rep " + rep + ": " + sb);
            }
        }
    }
}
