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

    private static FlightInfo twoEndpointInfo(FlightClient client, FlightStream first, FlightStream second) {
        FlightInfo info = mock(FlightInfo.class);
        FlightEndpoint a = mock(FlightEndpoint.class);
        FlightEndpoint b = mock(FlightEndpoint.class);
        Ticket ta = new Ticket(new byte[] {1});
        Ticket tb = new Ticket(new byte[] {2});
        when(info.getEndpoints()).thenReturn(List.of(a, b));
        when(a.getLocations()).thenReturn(List.of());
        when(b.getLocations()).thenReturn(List.of());
        when(a.getTicket()).thenReturn(ta);
        when(b.getTicket()).thenReturn(tb);
        when(client.getStream(ta)).thenReturn(first);
        when(client.getStream(tb)).thenReturn(second);
        return info;
    }

    private static FlightStream streamOf(VectorSchemaRoot root) {
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true, false);
        when(stream.getRoot()).thenReturn(root);
        return stream;
    }

    private static VectorSchemaRoot bigints(RootAllocator allocator, Long... values) {
        org.apache.arrow.vector.BigIntVector vector = new org.apache.arrow.vector.BigIntVector("k", allocator);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, values[i]);
            }
        }
        vector.setValueCount(values.length);
        return VectorSchemaRoot.of(vector);
    }

    @Test
    void collectsDistinctKeysAcrossEndpointsSkippingNulls() throws Exception {
        RootAllocator allocator = new RootAllocator();
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        VectorSchemaRoot one = bigints(allocator, 1L, 2L, null);
        VectorSchemaRoot two = bigints(allocator, 2L, 3L);
        FlightInfo info = twoEndpointInfo(client, streamOf(one), streamOf(two));
        when(sql.execute("SELECT k FROM sales.parts GROUP BY k")).thenReturn(info);
        FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0]);
        try {
            assertEquals(java.util.Optional.of(List.of(1L, 2L, 3L)),
                    catalog.distinctLongKeys("sales", "parts", "k", null, 10));
        } finally {
            one.close();
            two.close();
            catalog.close();
        }
    }

    @Test
    void givesUpAndCancelsWhenThereAreMoreKeysThanTheLimit() throws Exception {
        RootAllocator allocator = new RootAllocator();
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        VectorSchemaRoot one = bigints(allocator, 1L, 2L, 3L);
        FlightStream first = streamOf(one);
        VectorSchemaRoot unread = bigints(allocator, 4L);
        FlightStream second = streamOf(unread);
        FlightInfo info = twoEndpointInfo(client, first, second);
        when(sql.execute("SELECT k FROM sales.parts GROUP BY k")).thenReturn(info);
        FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0]);
        try {
            assertEquals(java.util.Optional.empty(), catalog.distinctLongKeys("sales", "parts", "k", null, 2));
            org.mockito.Mockito.verify(first).cancel(org.mockito.ArgumentMatchers.anyString(),
                    org.mockito.ArgumentMatchers.any());
            org.mockito.Mockito.verifyNoInteractions(second);
        } finally {
            one.close();
            unread.close();
            catalog.close();
        }
    }

    @Test
    void doesNotReduceByNonIntegerKeys() throws Exception {
        RootAllocator allocator = new RootAllocator();
        FlightClient client = mock(FlightClient.class);
        FlightSqlClient sql = mock(FlightSqlClient.class);
        VectorSchemaRoot names = root(allocator, List.of("k"), List.of(List.of("a")));
        FlightInfo info = flightInfo(client, names);
        when(sql.execute("SELECT k FROM sales.parts GROUP BY k")).thenReturn(info);
        FlightCatalog catalog = new FlightCatalog(client, sql, allocator,
                new org.apache.arrow.flight.CallOption[0]);
        try {
            assertEquals(java.util.Optional.empty(), catalog.distinctLongKeys("sales", "parts", "k", null, 10));
        } finally {
            names.close();
            catalog.close();
        }
    }
}
