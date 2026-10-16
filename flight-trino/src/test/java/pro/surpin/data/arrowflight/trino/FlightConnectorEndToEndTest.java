package pro.surpin.data.arrowflight.trino;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/** Runs the connector against an in-process Flight SQL server. */
@Tag("unit")
class FlightConnectorEndToEndTest {
    private static final Schema ORDERS = new Schema(List.of(
            new Field("ID", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("Name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    private static final ConnectorSession SESSION = mock(ConnectorSession.class);

    private BufferAllocator allocator;
    private FlightServer server;
    private final List<String> queries = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startServer() throws Exception {
        allocator = new RootAllocator();
        server = FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0), new Producer()).build();
        server.start();
    }

    @AfterEach
    void stopServer() throws Exception {
        server.close();
        allocator.close();
    }

    private Connector connector() {
        ConnectorFactory factory = new FlightPlugin().getConnectorFactories().iterator().next();
        assertEquals("arrow-flight", factory.getName());
        return factory.create("flight", Map.of(
                "flight.host", "localhost",
                "flight.port", String.valueOf(server.getPort()),
                "flight.bearer-token", "token",
                "flight.max-retries", "0"), mock(ConnectorContext.class));
    }

    @Test
    void readsFilteredProjectionFromAllEndpoints() throws Exception {
        Connector connector = connector();
        try {
            ConnectorMetadata metadata = connector.getMetadata(SESSION, FlightTransactionHandle.INSTANCE);
            assertEquals(List.of("sales"), metadata.listSchemaNames(SESSION));
            assertEquals(List.of(new SchemaTableName("sales", "orders")),
                    metadata.listTables(SESSION, java.util.Optional.of("sales")));

            ConnectorTableHandle table = metadata.getTableHandle(SESSION, new SchemaTableName("sales", "orders"));
            Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, table);
            ColumnHandle id = columns.get("id");
            ColumnHandle name = columns.get("name");
            List<ConnectorExpression> projections = List.of(
                    new Variable("name", VarcharType.VARCHAR), new Variable("id", BigintType.BIGINT));
            table = metadata.applyProjection(SESSION, table, projections, Map.of("name", name, "id", id))
                    .orElseThrow().getHandle();
            TupleDomain<ColumnHandle> filter = TupleDomain.withColumnDomains(
                    Map.of(id, Domain.multipleValues(BigintType.BIGINT, List.of(1L, 2L))));
            table = metadata.applyFilter(SESSION, table, new Constraint(filter)).orElseThrow().getHandle();

            ConnectorSplitSource source = connector.getSplitManager().getSplits(FlightTransactionHandle.INSTANCE,
                    SESSION, table, DynamicFilter.EMPTY, Constraint.alwaysTrue());
            List<ConnectorSplit> splits = source.getNextBatch(10).get().getSplits();
            assertEquals(2, splits.size());
            String query = queries.getFirst();
            assertEquals(true, query.equals("select Name,ID from sales.Orders where ID in (1,2)")
                    || query.equals("select ID,Name from sales.Orders where ID in (1,2)"), query);

            List<String> rows = new ArrayList<>();
            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pages = connector.getPageSourceProvider().createPageSource(
                        FlightTransactionHandle.INSTANCE, SESSION, split, table, List.of(id, name),
                        DynamicFilter.EMPTY)) {
                    for (Page page = pages.getNextPage(); page != null; page = pages.getNextPage()) {
                        for (int row = 0; row < page.getPositionCount(); row++) {
                            rows.add(BigintType.BIGINT.getLong(page.getBlock(0), row) + ":"
                                    + VarcharType.VARCHAR.getSlice(page.getBlock(1), row).toStringUtf8());
                        }
                    }
                }
            }
            assertEquals(List.of("1:first", "2:second"), rows);
        } finally {
            connector.shutdown();
        }
    }

    @Test
    void shutdownWithoutUseDoesNotConnect() {
        connector().shutdown();
        assertEquals(List.of(), queries);
    }

    @Test
    void rejectsMissingSchemaBytes() {
        assertThrows(IllegalStateException.class, () -> CoreFlightService.decodeSchema(null));
    }

    /** Minimal Flight SQL server with one table spread over two endpoints. */
    private final class Producer extends NoOpFlightSqlProducer {

        private FlightInfo info(Message command, FlightDescriptor descriptor, Schema schema) {
            Ticket ticket = new Ticket(Any.pack(command).toByteArray());
            return new FlightInfo(schema, descriptor, List.of(new FlightEndpoint(ticket)), -1, -1);
        }

        @Override
        public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context,
                                               FlightDescriptor descriptor) {
            return info(request, descriptor, FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA);
        }

        @Override
        public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context,
                                     ServerStreamListener listener) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA,
                    allocator)) {
                ((VarCharVector) root.getVector("db_schema_name")).setSafe(0, bytes("sales"));
                root.setRowCount(1);
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
        }

        @Override
        public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context,
                                              FlightDescriptor descriptor) {
            return info(request, descriptor, FlightSqlProducer.Schemas.GET_TABLES_SCHEMA);
        }

        @Override
        public void getStreamTables(FlightSql.CommandGetTables command, CallContext context,
                                    ServerStreamListener listener) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA,
                    allocator)) {
                ByteBuffer encoded = MessageSerializer.serializeMetadata(ORDERS, IpcOption.DEFAULT);
                byte[] schemaBytes = new byte[encoded.remaining()];
                encoded.get(schemaBytes);
                String[][] tables = {{"sales", "Orders"}, {"other", "Secret"}};
                for (int row = 0; row < tables.length; row++) {
                    ((VarCharVector) root.getVector("db_schema_name")).setSafe(row, bytes(tables[row][0]));
                    ((VarCharVector) root.getVector("table_name")).setSafe(row, bytes(tables[row][1]));
                    ((VarCharVector) root.getVector("table_type")).setSafe(row, bytes("TABLE"));
                    ((VarBinaryVector) root.getVector("table_schema")).setSafe(row, schemaBytes);
                }
                root.setRowCount(tables.length);
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
        }

        @Override
        public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context,
                                                 FlightDescriptor descriptor) {
            queries.add(command.getQuery());
            List<FlightEndpoint> endpoints = new ArrayList<>();
            for (int part = 0; part < 2; part++) {
                FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(ByteString.copyFromUtf8(String.valueOf(part + 1))).build();
                endpoints.add(new FlightEndpoint(new Ticket(Any.pack(ticket).toByteArray())));
            }
            return new FlightInfo(ORDERS, descriptor, endpoints, -1, -1);
        }

        @Override
        public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context,
                                       ServerStreamListener listener) {
            long id = Long.parseLong(ticket.getStatementHandle().toStringUtf8());
            try (VectorSchemaRoot root = VectorSchemaRoot.create(ORDERS, allocator)) {
                ((BigIntVector) root.getVector("ID")).setSafe(0, id);
                ((VarCharVector) root.getVector("Name")).setSafe(0, bytes(id == 1 ? "first" : "second"));
                root.setRowCount(1);
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
        }

        private byte[] bytes(String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
    }
}
