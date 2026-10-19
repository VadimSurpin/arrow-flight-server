package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.model.QueryStatistics;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

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

/** Tests terminal Flight stream status handling in the SQL producer. */
@Tag("unit")
class FlightSqlProducerTest {

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
        when(metadataService.getSchemas(null)).thenReturn(Map.of(
                "sales", new org.apache.hadoop.fs.Path("/sales"),
                "private", new org.apache.hadoop.fs.Path("/private")));
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

    /** Verifies the statistics action returns estimates without execution planning. */
    @Test
    void queryStatisticsActionReturnsEstimate() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);
        String query = "select * from tpch.lineitem";
        when(queryPlanner.estimate(query)).thenReturn(new QueryStatistics(4096L, 125L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);

        producer.doAction(context, new Action("arrow-flight-query-statistics",
                query.getBytes(java.nio.charset.StandardCharsets.UTF_8)), listener);

        ArgumentCaptor<Result> result = ArgumentCaptor.forClass(Result.class);
        verify(listener).onNext(result.capture());
        java.nio.ByteBuffer body = java.nio.ByteBuffer.wrap(result.getValue().getBody());
        assertEquals(4096L, body.getLong());
        assertEquals(125L, body.getLong());
        verify(listener).onCompleted();
        verify(queryPlanner, never()).plan(anyString());
    }
}
