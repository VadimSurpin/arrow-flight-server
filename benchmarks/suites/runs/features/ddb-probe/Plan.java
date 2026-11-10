import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
public class Plan {
    public static void main(String[] a) throws Exception {
        String[] sqls = {
          "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_store_sk, ss_ticket_number, ss_quantity FROM tpcds_sf100_asis.store_sales WHERE ((ss_customer_sk IS NOT NULL) AND (ss_item_sk IS NOT NULL) AND (ss_ticket_number IS NOT NULL))",
          "SELECT cs_item_sk, cs_sold_date_sk, cs_quantity, cs_bill_customer_sk FROM tpcds_sf100_asis.catalog_sales WHERE ((cs_bill_customer_sk IS NOT NULL) AND (cs_item_sk IS NOT NULL))",
          "SELECT sr_ticket_number, sr_return_quantity, sr_returned_date_sk, sr_item_sk, sr_customer_sk FROM tpcds_sf100_asis.store_returns WHERE ((sr_customer_sk IS NOT NULL) AND (sr_item_sk IS NOT NULL) AND (sr_ticket_number IS NOT NULL))",
          "SELECT d_date_sk FROM tpcds_sf100_asis.date_dim WHERE (d_year IN (1999, 2000, 2001))"};
        try (RootAllocator alloc = new RootAllocator(); FlightClient cc = FlightClient.builder(alloc, new Location(a[0])).build()) {
            var sql = new org.apache.arrow.flight.sql.FlightSqlClient(cc);
            for (int rep = 0; rep < Integer.parseInt(a[1]); rep++) {
                StringBuilder sb = new StringBuilder();
                for (String q : sqls) { long t = System.nanoTime(); sql.execute(q); sb.append(String.format("%.2fs ", (System.nanoTime() - t) / 1e9)); }
                System.out.println("plan rep " + rep + ": " + sb);
            }
        }
    }
}
