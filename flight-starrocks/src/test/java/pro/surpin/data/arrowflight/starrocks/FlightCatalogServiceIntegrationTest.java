package pro.surpin.data.arrowflight.starrocks;

import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the frontend catalog adapter against a live Flight SQL server. */
@Tag("real-services")
class FlightCatalogServiceIntegrationTest {
    @Test
    void discoversAndPlansARealFlightTable() throws Exception {
        String host = required("FLIGHT_IT_HOST");
        int port = Integer.parseInt(required("FLIGHT_IT_PORT"));
        String catalogName = required("FLIGHT_IT_CATALOG");
        String schemaName = required("FLIGHT_IT_SCHEMA");
        String tableName = required("FLIGHT_IT_TABLE");

        try (FlightCatalog catalog = FlightCatalog.connect(
                Location.forGrpcInsecure(host, port))) {
            assertTrue(catalog.catalogs().contains(catalogName));
            assertTrue(catalog.schemas(catalogName).contains(schemaName));
            assertTrue(catalog.tables(catalogName, schemaName).stream()
                    .anyMatch(table -> table.name().equals(tableName)));
            assertFalse(catalog.tableSchema(schemaName, tableName).getFields().isEmpty());
            assertFalse(catalog.planScan(schemaName, tableName, List.of()).getEndpoints().isEmpty());
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
