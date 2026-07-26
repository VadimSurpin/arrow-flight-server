package net.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.surpin.data.arrowflight.server.LogUtil;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import net.surpin.data.arrowflight.server.model.QueryPlan;
import net.surpin.data.arrowflight.server.metrics.ExecutionPathRecorder;
import net.surpin.data.arrowflight.server.metrics.MetricsService;
import net.surpin.data.arrowflight.server.services.ClusterService;
import net.surpin.data.arrowflight.server.services.ExecutionService;
import net.surpin.data.arrowflight.server.services.MetadataService;
import net.surpin.data.arrowflight.server.services.QueryPlanner;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

/**
 * Flight SQL producer that handles metadata and query execution requests.
 */
public final class FlightSqlProducer extends BasicFlightSqlProducer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlProducer.class);
    private static final String SCHEMA_NOT_FOUND_MESSAGE = "Could not find Arrow schema for query";

    private final Location location;
    private final BufferAllocator allocator;
    private final MetadataService metadataService;
    private final QueryPlanner queryPlanner;
    private final ExecutionService executionService;
    private final ClusterService clusterService;
    private final SqlInfoBuilder sqlInfoBuilder;

    /**
     * @param location server location for endpoint registration
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning and endpoint determination
     * @param executionService query execution service
     * @param clusterService cluster state management
     */
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService) {
        this.location = location;
        this.allocator = allocator;
        this.metadataService = metadataService;
        this.queryPlanner = queryPlanner;
        this.executionService = executionService;
        this.clusterService = clusterService;

        this.sqlInfoBuilder = new SqlInfoBuilder();
        sqlInfoBuilder
                .withFlightSqlServerName("Hadoop-Arrow-Parquet Source")
                .withFlightSqlServerVersion("0.0.1")
                .withFlightSqlServerArrowVersion("0.0.1")
                .withFlightSqlServerReadOnly(true)
                .withFlightSqlServerSql(true)
                .withFlightSqlServerSubstrait(false)
                .withFlightSqlServerTransaction(
                        FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                .withSqlDdlCatalog(false)
                .withSqlDdlSchema(true)
                .withSqlDdlTable(true)
                .withSqlIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE)
                .withSqlQuotedIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                .withSqlAllTablesAreSelectable(true)
                .withSqlNullOrdering(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
                .withSqlMaxColumnsInTable(1000);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final ByteString handle = ticket.getStatementHandle();
        String qid = qid(handle);
        long tGet = LogUtil.mark();
        HandleState state;
        try {
            state = clusterService.resolveEndpointHandle(handle.toByteArray());
        } catch (IllegalArgumentException e) {
            listener.error(CallStatus.UNAUTHENTICATED
                    .withDescription("Invalid Flight endpoint ticket")
                    .withCause(e).toRuntimeException());
            return;
        }
        LogUtil.logTiming(tGet, "execution.getHandle");
        if (state == null) {
            LOGGER.error("qid={} No HandleState found", qid);
            listener.error(new IllegalStateException("No HandleState found for qid=" + qid));
            return;
        }

        String query = state.query();
        String[] filePaths = state.filePaths();
        if (filePaths == null) {
            LOGGER.error("qid={} No file paths in handle state", qid);
            listener.error(new IllegalStateException("No file paths for qid=" + qid));
            return;
        }

        String serverUri = state.serverUri() != null ? state.serverUri() : "local";
        long bytes = state.bytes();
        long tExec = LogUtil.mark();
        long executionStartNanos = System.nanoTime();
        LogUtil.setQid(qid);
        LOGGER.debug("qid={} node={} thread={} execution=start server={} files={} bytes={} endpoint={} query='{}'",
                qid, LogUtil.node(), Thread.currentThread().getName(),
                serverUri, filePaths.length, bytes, qid, query);

        MDC.put("qid", qid);
        MetricsService.QueryObservation observation =
                MetricsService.observeQuery(bytes);
        ExecutionPathTracker pathTracker = new ExecutionPathTracker();
        boolean success = false;
        String failureReason = null;
        try {
            executionService.readParquet(
                    allocator, query, filePaths, listener, true, pathTracker);
            listener.completed();
            success = true;
            LogUtil.logTiming(tExec, "execution.total", "files=" + filePaths.length);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - executionStartNanos);
            LOGGER.debug("qid={} node={} thread={} execution=completed server={} elapsedMs={} files={} result=completed query='{}'",
                    qid, LogUtil.node(), Thread.currentThread().getName(),
                    serverUri, elapsed, filePaths.length, query);
        } catch (Exception e) {
            observation.markFailed();
            LogUtil.logTiming(tExec, "execution.failed", "files=" + filePaths.length);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - executionStartNanos);
            String failure = failureDescription(e);
            failureReason = failure;
            LOGGER.error("qid={} node={} thread={} execution=failed server={} elapsedMs={} files={} result=failed error='{}'",
                    qid, LogUtil.node(), Thread.currentThread().getName(),
                    serverUri, elapsed, filePaths.length, failure, e);
            if (e instanceof FlightRuntimeException flightException) {
                listener.error(flightException);
            } else {
                listener.error(CallStatus.INTERNAL
                        .withDescription(failure)
                        .withCause(e)
                        .toRuntimeException());
            }
        } finally {
            observation.executionPath(pathTracker.path());
            ExecutionPathRecorder.record(
                    qid, query, pathTracker, success, failureReason);
            observation.close();
            MDC.remove("qid");
            LogUtil.setQid(null);
            clusterService.releaseEndpointLoad(handle.toByteArray(), state);
        }
    }

    private static boolean isFileNotFound(Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof java.io.FileNotFoundException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static String failureDescription(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        String description = "Query execution failed: " + root.getClass().getSimpleName();
        if (message != null && !message.isBlank()) {
            description += ": " + message;
        }
        return description.length() <= 1024 ? description : description.substring(0, 1024);
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        long t = LogUtil.mark();
        ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        String query = command.getQuery();
        String qid = qid(handle);
        LogUtil.logTiming(t, "schema.resolveQuery", "qid=" + qid);

        Schema arrowSchema;
        try {
            arrowSchema = metadataService.getQuerySchema(query);
        } catch (Exception e) {
            LOGGER.error("Error getting Arrow schema for query: {}", query, e);
            if (isFileNotFound(e)) {
                throw CallStatus.NOT_FOUND
                        .withDescription(SCHEMA_NOT_FOUND_MESSAGE)
                        .withCause(e).toRuntimeException();
            }
            throw CallStatus.INTERNAL
                    .withDescription("Error getting Arrow schema for query")
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null) {
            LOGGER.error("Arrow schema not found for query: {}", query);
            throw CallStatus.NOT_FOUND
                    .withDescription(SCHEMA_NOT_FOUND_MESSAGE)
                    .toRuntimeException();
        }

        try {
            QueryPlan plan = queryPlanner.plan(query);
            return new FlightInfo(arrowSchema, descriptor, plan.endpoints(),
                    plan.totalBytes(), plan.totalRecords());
        } catch (IOException e) {
            throw CallStatus.INTERNAL
                    .withDescription("Unable to plan Flight query")
                    .withCause(e).toRuntimeException();
        }
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String query = command.getQuery();
        LOGGER.debug("getSchemaStatement: {}", query);

        Schema arrowSchema;
        try {
            arrowSchema = metadataService.getQuerySchema(query);
        } catch (Exception e) {
            LOGGER.error("Error getting Arrow schema for query: {}", query, e);
            throw CallStatus.INTERNAL
                    .withDescription("Error getting Arrow schema for query")
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null) {
            throw CallStatus.NOT_FOUND
                    .withDescription(SCHEMA_NOT_FOUND_MESSAGE)
                    .toRuntimeException();
        }

        return new SchemaResult(arrowSchema);
    }

    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(
            T request, FlightDescriptor descriptor, Schema schema) {
        long t = LogUtil.mark();
        try {
            if (request instanceof FlightSql.TicketStatementQuery ticketQuery) {
                long tGet = LogUtil.mark();
                final ByteString handle = ticketQuery.getStatementHandle();
                HandleState state = clusterService.getHandle(handle.toStringUtf8());
                LogUtil.logTiming(tGet, "planning.getHandle");
                if (state == null) {
                    throw CallStatus.NOT_FOUND
                            .withDescription("No handle state found")
                            .toRuntimeException();
                }
                String query = state.query();
                List<FlightEndpoint> endpoints = queryPlanner.determineEndpoints(query);
                LogUtil.logTiming(t, "planning.determineEndpoints", "endpoints=" + endpoints.size());
                return endpoints;
            } else {
                Ticket ticket = new Ticket(Any.pack(request).toByteArray());
                return List.of(new FlightEndpoint(ticket, location));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ── Metadata handlers ────────────────────────────────────────────────

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        this.sqlInfoBuilder.send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getCatalogsRoot(allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern()
                ? command.getDbSchemaFilterPattern() : null;
        boolean errored = false;

        try {
            Map<String, org.apache.hadoop.fs.Path> schemas =
                    metadataService.getSchemas(schemaFilterPattern);
            try (VectorSchemaRoot root = metadataService.getSchemasRoot(
                    schemas.keySet(), allocator)) {
                listener.start(root);
                listener.putNext();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamSchemas", e);
            listener.error(e);
            errored = true;
        } finally {
            if (!errored) {
                listener.completed();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getTableTypesRoot(allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        Schema schemaToUse = org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    @SuppressWarnings("java:S3776")
    public void getStreamTables(FlightSql.CommandGetTables command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern()
                ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern = command.hasTableNameFilterPattern()
                ? command.getTableNameFilterPattern() : null;

        if (catalog != null
                && !MetadataService.CATALOG_NAME.equalsIgnoreCase(catalog)) {
            LOGGER.info("Catalog doesn't exist in getStreamTables: {}", catalog);
            throw CallStatus.NOT_FOUND
                    .withDescription("Could not getStreamTables for catalog: " + catalog)
                    .toRuntimeException();
        }

        final ProtocolStringList tableTypesList = command.getTableTypesList();
        if (!tableTypesList.isEmpty()
                && !tableTypesList.stream().allMatch(
                        MetadataService.TABLE_TYPE::equalsIgnoreCase)) {
            LOGGER.info("Table type not found in getStreamTables: {}", tableTypesList);
            throw CallStatus.NOT_FOUND
                    .withDescription("Table type not found in getStreamTables")
                    .toRuntimeException();
        }

        Map<String, Map<String, Schema>> tables = new java.util.LinkedHashMap<>();
        try {
            for (Map.Entry<String, org.apache.hadoop.fs.Path> schemaEntry :
                    metadataService.getSchemas(schemaFilterPattern).entrySet()) {
                Map<String, Schema> tablesForSchema = new java.util.LinkedHashMap<>();
                tables.put(schemaEntry.getKey(), tablesForSchema);
                for (Map.Entry<String, org.apache.hadoop.fs.Path> tableEntry :
                        metadataService.getTables(schemaEntry.getKey(), tableFilterPattern)
                                .entrySet()) {
                    Schema schema = command.getIncludeSchema()
                            ? metadataService.getTableSchema(
                                    schemaEntry.getKey(), tableEntry.getKey(), null)
                            : null;
                    tablesForSchema.put(tableEntry.getKey(), schema);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            return;
        }

        boolean errored = false;
        try (VectorSchemaRoot root = metadataService.getTablesRoot(
                tables, allocator, command.getIncludeSchema(),
                schemaFilterPattern, tableFilterPattern)) {
            listener.start(root);
            listener.putNext();
        } catch (Exception e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            errored = true;
        } finally {
            if (!errored) {
                listener.completed();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getTypeInfoRoot(request, allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public void close() throws Exception {
        // Dependencies and allocator are owned and closed by the server bootstrap.
    }

    /**
     * @param handle ticket handle as ByteString
     * @return truncated 8-character query identifier
     */
    private static String qid(ByteString handle) {
        String raw = handle.toStringUtf8();
        return raw.length() >= 8 ? raw.substring(0, 8) : raw;
    }

    /**
     * @param request protobuf request message
     * @param descriptor flight descriptor
     * @param schema Arrow schema for the response
     * @return FlightInfo with determined endpoints
     */
    private <T extends Message> FlightInfo getFlightInfoForSchema(
            T request, FlightDescriptor descriptor, Schema schema) {
        final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}
