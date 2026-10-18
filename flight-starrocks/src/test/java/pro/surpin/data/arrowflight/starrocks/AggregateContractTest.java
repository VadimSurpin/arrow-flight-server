package pro.surpin.data.arrowflight.starrocks;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins the aggregate SQL shapes the StarRocks pushdown relies on against a live Flight SQL server. */
@Tag("real-services")
class AggregateContractTest {
    private static final List<String> QUERIES = List.of(
            "SELECT SUM(int_col) AS __agg0, SUM(smallint_col) AS __agg1, SUM(tinyint_col) AS __agg2 FROM %s",
            "SELECT SUM(float_col) AS __agg0, SUM(double_col) AS __agg1 FROM %s",
            "SELECT MIN(float_col) AS __agg0, MAX(smallint_col) AS __agg1, MIN(bool_col) AS __agg2 FROM %s",
            "SELECT int_col, COUNT(id) AS __agg0, SUM(double_col) AS __agg1, MIN(int_col) AS __agg2 "
                    + "FROM %s WHERE id > 5 GROUP BY int_col",
            "SELECT year, month, COUNT(*) AS __agg0, SUM(float_col) AS __agg1 FROM %s GROUP BY year, month",
            "SELECT SUM(double_col) AS __agg0, COUNT(double_col) AS __agg1, COUNT(*) AS __agg2, "
                    + "MIN(int_col) AS __agg3 FROM %s WHERE id < 0",
            "SELECT bool_col, COUNT(*) AS __agg0 FROM %s GROUP BY bool_col");

    @Test
    void serverAcceptsPartialAggregateQueries() throws Exception {
        String host = required("FLIGHT_IT_HOST");
        int port = Integer.parseInt(required("FLIGHT_IT_PORT"));
        String table = required("FLIGHT_IT_SCHEMA") + "." + required("FLIGHT_IT_TABLE");
        try (FlightCatalog catalog = FlightCatalog.connect(Location.forGrpcInsecure(host, port))) {
            List<String> failures = new java.util.ArrayList<>();
            for (String template : QUERIES) {
                String sql = template.formatted(table);
                try {
                    FlightInfo info = catalog.rawQuery(sql);
                    long rows = catalog.countRows(info);
                    System.out.println("CONTRACT OK " + sql + "\n  schema=" + info.getSchema().getFields()
                            + " endpoints=" + info.getEndpoints().size() + " rows=" + rows);
                } catch (RuntimeException e) {
                    failures.add(sql + " -> " + e);
                    System.out.println("CONTRACT FAIL " + sql + " -> " + e);
                }
            }
            assertTrue(failures.isEmpty(), failures.toString());
        }
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Set " + name + " for the real-services test profile");
        }
        return value;
    }
}
