package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.TableProvider.IcebergSnapshot;
import pro.surpin.data.arrowflight.server.TableProvider.TableIdentifier;
import pro.surpin.data.arrowflight.server.TableProvider.TableScanPlan;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.services.BloomTestSupport;
import pro.surpin.data.arrowflight.server.services.RuntimeFilters;
import pro.surpin.data.arrowflight.server.services.RuntimeInFilters;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

/** Tests terminal Flight stream status handling in the SQL producer. */
@Tag("unit")
class FlightSqlProducerTest {

    /** Verifies prepared statements have a limit and closing one frees capacity. */
    @Test
    void preparedStatementsHaveBoundedLifetime() throws Exception {
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getQuerySchema(anyString())).thenReturn(new Schema(List.of()));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                metadataService, mock(QueryPlanner.class), mock(ExecutionService.class),
                mock(ClusterService.class));
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);
        FlightSql.ActionCreatePreparedStatementRequest request = FlightSql.ActionCreatePreparedStatementRequest
                .newBuilder().setQuery("SELECT 1").build();

        for (int i = 0; i < 1024; i++) {
            producer.createPreparedStatement(request, null, listener);
        }
        producer.createPreparedStatement(request, null, listener);
        ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onError(failure.capture());
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED,
                assertInstanceOf(FlightRuntimeException.class, failure.getValue()).status().code());

        ArgumentCaptor<Result> results = ArgumentCaptor.forClass(Result.class);
        verify(listener, times(1024)).onNext(results.capture());
        ByteString handle = Any.parseFrom(results.getAllValues().get(0).getBody())
                .unpack(FlightSql.ActionCreatePreparedStatementResult.class).getPreparedStatementHandle();
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<Result> closeListener = mock(FlightProducer.StreamListener.class);
        producer.closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest.newBuilder()
                .setPreparedStatementHandle(handle).build(), null, closeListener);
        producer.createPreparedStatement(request, null, listener);
        verify(listener, times(1025)).onCompleted();
        producer.close();
        ByteString stillOpen = Any.parseFrom(results.getAllValues().get(1).getBody())
                .unpack(FlightSql.ActionCreatePreparedStatementResult.class).getPreparedStatementHandle();
        FlightRuntimeException missing = org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                () -> producer.getSchemaPreparedStatement(FlightSql.CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(stillOpen).build(), null,
                        FlightDescriptor.command(new byte[]{1})));
        assertEquals(FlightStatusCode.NOT_FOUND, missing.status().code());
    }

    /** Verifies SQL metadata does not advertise unsupported DDL commands. */
    @Test
    void sqlInfoReportsReadOnlyCapabilities() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            FlightSqlProducer producer = new FlightSqlProducer(
                    Location.forGrpcInsecure("localhost", 32010), allocator,
                    mock(MetadataService.class), mock(QueryPlanner.class), mock(ExecutionService.class),
                    mock(ClusterService.class));
            FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
            AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
            Map<Integer, Object> values = new HashMap<>();
            org.mockito.Mockito.doAnswer(invocation -> {
                rootRef.set(invocation.getArgument(0));
                return null;
            }).when(listener).start(any(VectorSchemaRoot.class));
            org.mockito.Mockito.doAnswer(invocation -> {
                VectorSchemaRoot root = rootRef.get();
                UInt4Vector names = (UInt4Vector) root.getVector("info_name");
                DenseUnionVector data = (DenseUnionVector) root.getVector("value");
                for (int i = 0; i < root.getRowCount(); i++) {
                    values.put(names.get(i), data.getObject(i));
                }
                return null;
            }).when(listener).putNext();
            producer.getStreamSqlInfo(FlightSql.CommandGetSqlInfo.newBuilder()
                    .addInfo(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE)
                    .addInfo(FlightSql.SqlInfo.SQL_DDL_SCHEMA_VALUE)
                    .addInfo(FlightSql.SqlInfo.SQL_DDL_TABLE_VALUE)
                    .build(), null, listener);
            assertEquals(true, values.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE));
            assertEquals(false, values.get(FlightSql.SqlInfo.SQL_DDL_SCHEMA_VALUE));
            assertEquals(false, values.get(FlightSql.SqlInfo.SQL_DDL_TABLE_VALUE));
        }
    }

    /** Blocks schema lookup before any table metadata is returned. */
    @Test
    void rangerDeniesStatementMetadata() throws Exception {
        MetadataService metadataService = mock(MetadataService.class);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        String query = "select * from sales.orders";
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery("alice@EXAMPLE.COM", query);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                metadataService, mock(QueryPlanner.class), mock(ExecutionService.class),
                mock(ClusterService.class), authorization);
        FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build();
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                        () -> producer.getFlightInfoStatement(command, context,
                                FlightDescriptor.command(new byte[]{1}))).status().code());
        verify(metadataService, never()).getQuerySchema(anyString());
    }

    /** Rechecks access to a query when a saved endpoint ticket is streamed. */
    @Test
    void rangerDeniesSavedTicketStream() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("saved-query");
        String query = "select * from sales.orders";
        ClusterService clusterService = mock(ClusterService.class);
        HandleState state = new HandleState(query, new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery("alice@EXAMPLE.COM", query);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        ExecutionService executionService = mock(ExecutionService.class);
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService, authorization);
        producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle).build(), context, listener);
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                assertInstanceOf(FlightRuntimeException.class, error.getValue()).status().code());
        verify(executionService, never()).readParquet(any(), anyString(), any(), any(),
                anyBoolean(), any());
        verify(clusterService).releaseEndpointLoad(handle.toByteArray(), state);
    }

    /** Omits schemas denied by Ranger from metadata discovery. */
    @Test
    void rangerFiltersSchemaListing() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getSchemas(null)).thenReturn(List.of("sales", "private"));
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(metadataService.getSchemasRoot(any(), eq(allocator))).thenReturn(root);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        when(authorization.canSeeSchema("alice@EXAMPLE.COM", "sales")).thenReturn(true);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, mock(QueryPlanner.class), mock(ExecutionService.class),
                mock(ClusterService.class), authorization);
        producer.getStreamSchemas(FlightSql.CommandGetDbSchemas.getDefaultInstance(),
                context, listener);
        verify(metadataService).getSchemasRoot(eq(List.of("sales")), eq(allocator));
        verify(listener).completed();
    }

    /** Verifies one resolved scan supplies both the SQL schema and the signed ticket. */
    @Test
    void plansIcebergEndpointWithoutParquetInventory() throws Exception {
        var iceberg = mock(IcebergTableProvider.class);
        when(iceberg.sqlSchema()).thenReturn("iceberg");
        var pinned = new IcebergSnapshot(URI.create("hdfs://nn/warehouse/sales/orders/metadata/v2.metadata.json"), 43);
        Schema tableSchema = new Schema(List.of(Field.notNullable("id", new ArrowType.Int(32, true))));
        var table = new TableIdentifier("iceberg", "orders");
        when(iceberg.planScan(table)).thenReturn(new TableScanPlan(tableSchema, pinned));
        ClusterService cluster = mock(ClusterService.class);
        byte[] handle = {1, 2, 3};
        when(cluster.createEndpointHandle(any())).thenReturn(handle);
        QueryPlanner parquetPlanner = mock(QueryPlanner.class);
        Location location = Location.forGrpcInsecure("localhost", 32010);
        MetadataService metadata = new MetadataService(mock(ParquetAdapter.class), iceberg);
        try (var producer = new FlightSqlProducer(location, mock(BufferAllocator.class), metadata,
                parquetPlanner, mock(ExecutionService.class), cluster)) {
            String query = "SELECT COUNT(*) AS orders FROM iceberg.orders";
            var command = FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build();
            FlightInfo info = producer.getFlightInfoStatement(command, null, FlightDescriptor.command(new byte[]{1}));
            assertEquals(new ArrowType.Int(64, true), info.getSchema().getFields().get(0).getType());
            assertEquals(List.of(location), info.getEndpoints().get(0).getLocations());
            var request = Any.parseFrom(info.getEndpoints().get(0).getTicket().getBytes())
                    .unpack(FlightSql.TicketStatementQuery.class);
            assertEquals(ByteString.copyFrom(handle), request.getStatementHandle());
        }

        ArgumentCaptor<HandleState> state = ArgumentCaptor.forClass(HandleState.class);
        verify(cluster).createEndpointHandle(state.capture());
        assertEquals(pinned, state.getValue().tableScan().source());
        assertEquals(tableSchema, Schema.fromJSON(state.getValue().tableScan().schemaJson()));
        verify(iceberg).planScan(table);
        verify(parquetPlanner, never()).plan(anyString());
    }

    /** Verifies an Iceberg ticket is rejected explicitly before entering Parquet execution. */
    @Test
    void icebergTicketDoesNotEnterParquetExecution() throws Exception {
        var cluster = mock(ClusterService.class);
        byte[] handle = {2, 3, 4};
        String query = "SELECT * FROM iceberg.orders";
        var source = new IcebergSnapshot(URI.create("hdfs://nn/warehouse/orders/metadata/v2.metadata.json"), 49);
        HandleState state = HandleState.forTableScan(query, "grpc+tcp://localhost:32010",
                new TableScanPlan(new Schema(List.of()), source));
        when(cluster.resolveEndpointHandle(handle)).thenReturn(state);
        var runtime = mock(ExecutionService.class);
        var authorization = mock(FlightAuthorization.class);
        var context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        var listener = mock(FlightProducer.ServerStreamListener.class);
        try (var producer = new FlightSqlProducer(Location.forGrpcInsecure("localhost", 32010),
                mock(BufferAllocator.class), mock(MetadataService.class), mock(QueryPlanner.class),
                runtime, cluster, authorization)) {
            producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFrom(handle)).build(), context, listener);
        }
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        assertEquals(FlightStatusCode.UNIMPLEMENTED,
                assertInstanceOf(FlightRuntimeException.class, error.getValue()).status().code());
        verify(authorization).requireQuery("alice@EXAMPLE.COM", query);
        verify(runtime, never()).readParquet(any(), anyString(), any(), any(), anyBoolean(), any());
        verify(listener, never()).completed();
    }

    /** Verifies a backpressure timeout is reported as an error, never as completion. */
    @Test
    void timeoutDoesNotCompleteTruncatedStream() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        ByteString handle = ByteString.copyFromUtf8("timeout-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);

        FlightRuntimeException timeout = CallStatus.TIMED_OUT
                .withDescription("listener timeout")
                .toRuntimeException();
        doThrow(timeout).when(executionService).readParquet(
                eq(allocator), eq(state.query()), eq(state.filePaths()), eq(listener),
                anyBoolean(), any(ExecutionPathTracker.class));

        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();

        producer.getStreamStatement(ticket, context, listener);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.TIMED_OUT, reported.status().code());
        verify(listener, never()).completed();
    }

    /** Verifies statement planning exports Parquet statistics through FlightInfo. */
    @Test
    void statementFlightInfoContainsQueryStatistics() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        String query = "select * from tpch.lineitem";
        Schema schema = new Schema(List.of());
        when(metadataService.getQuerySchema(query)).thenReturn(schema);
        when(queryPlanner.plan(query)).thenReturn(
                new QueryPlan(List.of(), 4096L, 125L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build();
        FlightDescriptor descriptor = FlightDescriptor.command(new byte[]{1});

        FlightInfo info = producer.getFlightInfoStatement(
                command, context, descriptor);

        assertEquals(4096L, info.getBytes());
        assertEquals(125L, info.getRecords());
        verify(clusterService, never()).storeHandle(anyString(), any());
    }

    // ── runtime filters carried on the ticket ────────────────────────────────

    private static Ticket ticketWithFilterBlock(ByteString handle, byte[] block) {
        Any any = Any.pack(FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build());
        if (block != null) {
            any = any.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                    .addField(FlightSqlProducer.RUNTIME_FILTER_FIELD, UnknownFieldSet.Field.newBuilder()
                            .addLengthDelimited(ByteString.copyFrom(block)).build())
                    .build()).build();
        }
        return new Ticket(any.toByteArray());
    }

    private static FlightSqlProducer producerStreaming(ExecutionService executionService, ByteString handle,
            HandleState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        return new FlightSqlProducer(Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class), executionService, clusterService);
    }

    /** A client's runtime filters ride on the ticket and reach the scan. */
    @Test
    @SuppressWarnings("unchecked")
    void streamPassesRuntimeFiltersFromTheTicketToTheScan() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("filtered-query");
        HandleState state = new HandleState("select a from t", new String[]{"p.parquet"}, null, 0L);
        ExecutionService executionService = mock(ExecutionService.class);
        FlightSqlProducer producer = producerStreaming(executionService, handle, state);
        byte[] block = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("a", new long[]{1, 2})));

        producer.getStream(mock(FlightProducer.CallContext.class), ticketWithFilterBlock(handle, block),
                mock(FlightProducer.ServerStreamListener.class));

        ArgumentCaptor<RuntimeFilters> filters = ArgumentCaptor.forClass(RuntimeFilters.class);
        verify(executionService).readParquet(any(), eq(state.query()), eq(state.filePaths()), any(), eq(true),
                any(ExecutionPathTracker.class), filters.capture());
        assertEquals(1, filters.getValue().in().size());
        assertEquals("a", filters.getValue().in().get(0).column());
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{1, 2},
                filters.getValue().in().get(0).values());
    }

    /** A bloom filter in the ticket's block reaches the scan with the IN-filters. */
    @Test
    void streamPassesBloomFiltersFromTheTicketToTheScan() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("bloom-query");
        HandleState state = new HandleState("select a from t", new String[]{"p.parquet"}, null, 0L);
        ExecutionService executionService = mock(ExecutionService.class);
        FlightSqlProducer producer = producerStreaming(executionService, handle, state);
        byte[] block = BloomTestSupport.block(List.of(), List.of(
                BloomTestSupport.sent("a", 4, new long[]{3, 9}, new long[]{3, 9, 4, 5, 6})));

        producer.getStream(mock(FlightProducer.CallContext.class), ticketWithFilterBlock(handle, block),
                mock(FlightProducer.ServerStreamListener.class));

        ArgumentCaptor<RuntimeFilters> filters = ArgumentCaptor.forClass(RuntimeFilters.class);
        verify(executionService).readParquet(any(), eq(state.query()), eq(state.filePaths()), any(), eq(true),
                any(ExecutionPathTracker.class), filters.capture());
        assertEquals(1, filters.getValue().blooms().size());
        assertEquals("a", filters.getValue().blooms().get(0).column());
    }

    /** A ticket without a block streams exactly as before. */
    @Test
    void streamWithoutAFilterBlockUsesThePlainScan() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("plain-query");
        HandleState state = new HandleState("select a from t", new String[]{"p.parquet"}, null, 0L);
        ExecutionService executionService = mock(ExecutionService.class);
        FlightSqlProducer producer = producerStreaming(executionService, handle, state);

        producer.getStream(mock(FlightProducer.CallContext.class), ticketWithFilterBlock(handle, null),
                mock(FlightProducer.ServerStreamListener.class));

        verify(executionService).readParquet(any(), eq(state.query()), eq(state.filePaths()), any(), eq(true),
                any(ExecutionPathTracker.class));
    }

    /** A damaged filter block is ignored: a filter only removes rows the client would drop, so the full scan is right. */
    @Test
    void streamIgnoresADamagedFilterBlock() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("damaged-block");
        HandleState state = new HandleState("select a from t", new String[]{"p.parquet"}, null, 0L);
        ExecutionService executionService = mock(ExecutionService.class);
        FlightSqlProducer producer = producerStreaming(executionService, handle, state);

        producer.getStream(mock(FlightProducer.CallContext.class),
                ticketWithFilterBlock(handle, new byte[]{1, 2, 3}), mock(FlightProducer.ServerStreamListener.class));

        verify(executionService).readParquet(any(), eq(state.query()), eq(state.filePaths()), any(), eq(true),
                any(ExecutionPathTracker.class));
    }
}
