package pro.surpin.data.arrowflight.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class FlightCatalogTest {
    @Test
    void listsOnlyTablesInRequestedSchema() throws Exception {
        RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = root(allocator,
                List.of("catalog_name", "db_schema_name", "table_name", "table_type"),
                List.of(List.of("catalog", "sales", "orders", "TABLE"),
                        List.of("catalog", "other", "secret", "TABLE")));
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        FlightInfo info = flightInfo(client, root);
        when(sql.getTables("catalog", null, null, List.of("TABLE"), false))
                .thenReturn(info);
        FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0]);
        try {
            assertEquals(List.of(new FlightCatalog.Table("catalog", "sales",
                    "orders", "TABLE")), catalog.tables("catalog", "sales"));
        } finally {
            root.close();
            catalog.close();
        }
    }

    @Test
    void listsSchemasInRequestedCatalog() throws Exception {
        RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = root(allocator,
                List.of("catalog_name", "db_schema_name"),
                List.of(List.of("catalog", "sales"),
                        List.of("another", "private")));
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        FlightInfo info = flightInfo(client, root);
        when(sql.getSchemas("catalog", null)).thenReturn(info);
        FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0]);
        try {
            assertEquals(List.of("sales"), catalog.schemas("catalog"));
        } finally {
            root.close();
            catalog.close();
        }
    }

    @Test
    void listsCatalogs() throws Exception {
        RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = root(allocator, List.of("catalog_name"),
                List.of(List.of("catalog")));
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        FlightInfo info = flightInfo(client, root);
        when(sql.getCatalogs()).thenReturn(info);
        FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0]);
        try {
            assertEquals(List.of("catalog"), catalog.catalogs());
        } finally {
            root.close();
            catalog.close();
        }
    }

    @Test
    void obtainsSchemaWithoutExecutingScan() throws Exception {
        RootAllocator allocator = new RootAllocator();
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        org.apache.arrow.flight.SchemaResult result = mock(
                org.apache.arrow.flight.SchemaResult.class);
        Schema schema = new Schema(List.of());
        when(sql.getExecuteSchema("SELECT * FROM sales.orders")).thenReturn(result);
        when(result.getSchema()).thenReturn(schema);
        try (FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0])) {
            assertEquals(schema, catalog.tableSchema("sales", "orders"));
        }
    }

    @Test
    void plansScanWithPushedDownFilter() throws Exception {
        RootAllocator allocator = new RootAllocator();
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        FlightInfo info = mock(FlightInfo.class);
        when(sql.execute("SELECT id FROM sales.orders WHERE (id IS NOT NULL)")).thenReturn(info);
        try (FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0])) {
            assertEquals(info, catalog.planScan("sales", "orders", List.of("id"),
                    new ScanPredicate.IsNull("id", true)));
        }
    }

    @Test
    void plansOneScanForBackendTicketDistribution() throws Exception {
        RootAllocator allocator = new RootAllocator();
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        FlightInfo info = mock(FlightInfo.class);
        when(sql.execute("SELECT id FROM sales.orders")).thenReturn(info);
        try (FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0])) {
            assertEquals(info, catalog.planScan("sales", "orders", List.of("id")));
        }
    }

    private static FlightInfo flightInfo(FlightClient client, VectorSchemaRoot root) {
        FlightInfo info = mock(FlightInfo.class);
        FlightEndpoint endpoint = mock(FlightEndpoint.class);
        FlightStream stream = mock(FlightStream.class);
        Ticket ticket = new Ticket(new byte[] {1});
        when(info.getEndpoints()).thenReturn(List.of(endpoint));
        when(endpoint.getLocations()).thenReturn(List.of());
        when(endpoint.getTicket()).thenReturn(ticket);
        when(client.getStream(ticket)).thenReturn(stream);
        when(stream.next()).thenReturn(true, false);
        when(stream.getRoot()).thenReturn(root);
        return info;
    }

    private static VectorSchemaRoot root(RootAllocator allocator,
            List<String> columns, List<List<String>> rows) {
        List<FieldVector> vectors = new ArrayList<>();
        for (int col = 0; col < columns.size(); col++) {
            VarCharVector vector = new VarCharVector(columns.get(col), allocator);
            vector.allocateNew();
            for (int row = 0; row < rows.size(); row++) {
                vector.setSafe(row, rows.get(row).get(col)
                        .getBytes(StandardCharsets.UTF_8));
            }
            vector.setValueCount(rows.size());
            vectors.add(vector);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        root.setRowCount(rows.size());
        return root;
    }
}
