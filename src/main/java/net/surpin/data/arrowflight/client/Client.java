package net.surpin.data.arrowflight.client;

import net.surpin.data.arrowflight.client.model.Field;
import net.surpin.data.arrowflight.client.model.FieldVector;
import net.surpin.data.arrowflight.client.model.RowSet;
import net.surpin.data.arrowflight.client.query.Endpoint;
import net.surpin.data.arrowflight.client.query.QueryEndpoints;
import com.google.protobuf.Any;
import net.surpin.data.arrowflight.server.LogUtil;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Describes the data-structure of Client for communicating with remote flight service
 */
public final class Client implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private static final String NODE;

    static {
        String n;
        try {
            n = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            n = "unknown";
        }
        NODE = n;
    }

    //the factory
    private static final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    //the existing objects of client
    private static final ConcurrentMap<String, Client> clients = new ConcurrentHashMap<>();

    //the flight client
    private final FlightClient client;
    //clients for locations advertised by FlightEndpoint (one connection per URI)
    private final ConcurrentMap<URI, FlightClient> endpointClients = new ConcurrentHashMap<>();
    //the flight-sql client
    private final FlightSqlClient sqlClient;
    //the token for calls
    private final CredentialCallOption bearerToken;

    //the configuration for retry/timeout
    private final Configuration config;

    //timeout call option (null if no timeout configured)
    private final CallOption timeoutOption;

    /**
     * Get the underlying FlightClient (for streaming reads)
     */
    public FlightClient getFlightClient() {
        return this.client;
    }

    /**
     * Get the bearer token credential (for streaming reads)
     */
    public CredentialCallOption getBearerToken() {
        return this.bearerToken;
    }

    /**
     * Get the combined call options (bearer token + timeout).
     */
    public CallOption[] callOptions() {
        if (timeoutOption != null) {
            return new CallOption[] { this.bearerToken, timeoutOption };
        }
        return new CallOption[] { this.bearerToken };
    }

    //the buffer
    private final BufferAllocator allocator;
    //the connection string to identify the client
    private final String connectionString;

    /**
     * Construct a Client object
     * @param client - the client object of the flight service
     * @param bearerToken - the credential token
     * @param connectionString - the connection string to identify the client
     * @param allocator - the buffer allocator
     * @param config - the client configuration
     */
    private Client(FlightClient client, CredentialCallOption bearerToken, String connectionString, BufferAllocator allocator, Configuration config) {
        this.client = client;
        this.sqlClient = new FlightSqlClient(this.client);
        this.bearerToken = bearerToken;
        this.config = config;

        this.connectionString = connectionString;
        this.allocator = allocator;

        long timeoutMs = config.getConnectTimeoutMs();
        this.timeoutOption = timeoutMs > 0 ? CallOptions.timeout(timeoutMs, TimeUnit.MILLISECONDS) : null;
    }

    /**
     * Fetch meta-data of all related end-points by a query
     * @param query - the query submitted to remote flight-service
     * @return - the schema and end-points for the query
     */
    public QueryEndpoints getQueryEndpoints(String query) {
        return retryWithBackoff(() -> {
            final FlightSql.CommandStatementQuery.Builder builder = FlightSql.CommandStatementQuery.newBuilder().setQuery(query);
            final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());

            FlightInfo fi = this.client.getInfo(descriptor, callOptions());
            LOGGER.debug("Client.getQueryEndpoints('{}'): got endpoints \n{}", query, fi.getEndpoints());
            Endpoint[] endpoints = fi.getEndpoints().stream().map(ep -> new Endpoint(ep.getLocations().stream().map(Location::getUri).toArray(URI[]::new), ep.getTicket().getBytes())).toArray(Endpoint[]::new);
            LOGGER.debug("Client.getQueryEndpoints('{}'): return endpoints \n{}", query, Arrays.asList(endpoints));
            return new QueryEndpoints(
                    fi.getSchema(), endpoints, fi.getBytes(), fi.getRecords());
        }, "getQueryEndpoints");
    }

    /**
     * Fetches only the result schema for a statement. Unlike getInfo/execute,
     * this does not create stream tickets or reserve per-node load.
     *
     * @param query SQL statement
     * @return Arrow result schema
     */
    public Schema getQuerySchema(String query) {
        return retryWithBackoff(
                () -> this.sqlClient.getExecuteSchema(query, callOptions()).getSchema(),
                "getQuerySchema");
    }

    /**
     * Fetch rows from the end-point
     * @param ep - the end-point
     * @param schema - the schema of rows from the end-point
     * @return - row set from the end-point
     */
    public RowSet fetch(Endpoint ep, Schema schema) {
        return retryWithBackoff(() -> {
            long startNanos = System.nanoTime();
            RowSet rs = new RowSet(schema);
            Field[] fields = Field.from(schema);
            FlightEndpoint fep = new FlightEndpoint(new Ticket(ep.getTicket()), Arrays.stream(ep.getURIs()).map(Location::new).toArray(Location[]::new));
            int batches = 0;
            long rows = 0;
            try (FlightStream stream = this.openStream(fep)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    batches++;
                    int rowCount = root.getRowCount();
                    rows += rowCount;
                    FieldVector[] fs = root.getFieldVectors().stream().map(fv -> FieldVector.fromArrow(fv, Field.find(fields, fv.getName()), rowCount)).toArray(FieldVector[]::new);
                    for (int i = 0; i < rowCount; i++) {
                        RowSet.Row row = new RowSet.Row();
                        for (FieldVector f : fs) {
                            row.add((f.getValues())[i]);
                        }
                        rs.add(row);
                    }
                }
            }
            LOGGER.debug("node={} client=fetchCompleted batches={} rows={} elapsed={}",
                    NODE, batches, rows, LogUtil.elapsedNanos(startNanos));
            return rs;
        }, "fetch");
    }

    /**
     * Fetch rows from all end-points
     * @param qEndpoints - the query end-points
     * @return - all rows from the end-points
     */
    public RowSet fetch(QueryEndpoints qEndpoints) {
        RowSet rs = new RowSet(qEndpoints.getSchema());
        Arrays.stream(qEndpoints.getEndpoints()).forEach(ep -> rs.add(fetch(ep, qEndpoints.getSchema())));
        return rs;
    }

    /**
     * Fetch rows from the end-point in a streaming fashion.
     * Each batch from the Flight stream is passed to the callback immediately,
     * without accumulating all rows in memory.
     */
    public void fetchStreaming(Endpoint ep, Schema schema, BatchCallback callback) {
        retryWithBackoff(() -> {
            long startNanos = System.nanoTime();
            Field[] fields = Field.from(schema);
            FlightEndpoint fep = new FlightEndpoint(new Ticket(ep.getTicket()), Arrays.stream(ep.getURIs()).map(Location::new).toArray(Location[]::new));
            int batches = 0;
            long rows = 0;
            try (FlightStream stream = this.openStream(fep)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    batches++;
                    rows += root.getRowCount();
                    if (batches == 1) {
                        LOGGER.debug("node={} client=ttfB batchRowCount={} elapsed={}",
                                NODE, root.getRowCount(), LogUtil.elapsedNanos(startNanos));
                    }
                    boolean shouldContinue = callback.onBatch(root, fields);
                    if (!shouldContinue) {
                        break;
                    }
                }
            }
            LOGGER.debug("node={} client=fetchStreamingCompleted batches={} rows={} elapsed={}",
                    NODE, batches, rows, LogUtil.elapsedNanos(startNanos));
            return null;
        }, "fetchStreaming");
    }

    /**
     * Fetch rows from all end-points in a streaming fashion.
     */
    public void fetchStreaming(QueryEndpoints qEndpoints, BatchCallback callback) {
        for (Endpoint ep : qEndpoints.getEndpoints()) {
            fetchStreaming(ep, qEndpoints.getSchema(), callback);
        }
    }

    /**
     * Callback interface for streaming batch processing.
     */
    @FunctionalInterface
    public interface BatchCallback {
        /**
         * Process a single batch of data.
         * @param root the current VectorSchemaRoot with data
         * @param fields the field definitions for the schema
         * @return true to continue, false to stop streaming
         */
        boolean onBatch(VectorSchemaRoot root, Field[] fields);
    }

    /**
     * Execute a literal SQL statement
     * @param stmt - the literal sql-statement
     */
    public long execute(String stmt) {
        return retryWithBackoff(() -> {
            long startNanos = System.nanoTime();
            FlightInfo fi = this.sqlClient.execute(stmt, callOptions());
            LOGGER.debug("node={} client=execute endpoints={} stmt='{}'",
                    NODE, fi.getEndpoints().size(), stmt);
            long count = 0;
            int endpointIdx = 0;
            for (FlightEndpoint endpoint: fi.getEndpoints()) {
                int batches = 0;
                try (FlightStream stream = this.openStream(endpoint)) {
                    while (stream.next()) {
                        batches++;
                        VectorSchemaRoot root = stream.getRoot();
                        count += root.getRowCount();
                    }
                }
                LOGGER.debug("node={} client=executeEndpoint idx={} batches={} rows={}",
                        NODE, endpointIdx++, batches, count);
            }
            LOGGER.debug("node={} client=executeCompleted totalRows={} endpoints={} elapsed={}",
                    NODE, count, fi.getEndpoints().size(), LogUtil.elapsedNanos(startNanos));
            return count;
        }, "execute");
    }

    /**
     * Truncate the target table
     * @param table - the name of the table
     */
    public void truncate(String table) {
        this.execute(String.format("truncate table %s", table));
    }

    /**
     * Get a prepared-statement for a query
     * @param query - the query being used for update
     * @return - the prepared sql-statement for down-stream operations
     */
    public FlightSqlClient.PreparedStatement getPreparedStatement(String query) {
        return this.sqlClient.prepare(query, this.bearerToken);
    }

    /**
     * Execute a prepared-statement
     *
     * @param preparedStmt - the prepared-statement being executed.
     */
    public void executeUpdate(FlightSqlClient.PreparedStatement preparedStmt) {
        preparedStmt.executeUpdate(this.bearerToken);
    }

    /**
     * Close the connection
     */
    @Override
    public void close() {
        clients.remove(this.connectionString);

        Set<FlightClient> routedClients = new HashSet<>(this.endpointClients.values());
        this.endpointClients.clear();
        for (FlightClient routedClient : routedClients) {
            try {
                routedClient.close();
            } catch (Exception ex) {
                LOGGER.warn("Error closing routed Flight client: {}", ex.getMessage());
            }
        }

        try {
            this.client.close();
            this.allocator.getChildAllocators().forEach(BufferAllocator::close);
            AutoCloseables.close(this.allocator);
        } catch (Exception ex) {
            LoggerFactory.getLogger(this.getClass()).warn(ex.getMessage() + Arrays.toString(ex.getStackTrace()));
        }
    }

    /**
     * Default max allocation per client: 2GB.
     * Can be overridden via {@link Configuration}.
     */
    private static final long DEFAULT_MAX_ALLOCATION = 2L * 1024 * 1024 * 1024; // 2GB

    public static Client getOrCreate(Configuration config) {
        String cs = config.getConnectionString();
        return clients.computeIfAbsent(cs, key -> {
            long maxAllocation = config.getAllocationLimit() > 0
                    ? config.getAllocationLimit()
                    : DEFAULT_MAX_ALLOCATION;

            final BufferAllocator allocator = new RootAllocator(maxAllocation);
            final FlightClient client = Client.create(config, allocator);

            final CallHeaders callHeaders = new FlightCallHeaders();
            if (config.getDefaultSchema() != null && !config.getDefaultSchema().isEmpty()) {
                callHeaders.insert("SCHEMA", config.getDefaultSchema());
            }
            if (config.getRoutingTag() != null && !config.getRoutingTag().isEmpty()) {
                callHeaders.insert("ROUTING_TAG", config.getRoutingTag());
            }
            if (config.getRoutingQueue() != null && !config.getRoutingQueue().isEmpty()) {
                callHeaders.insert("ROUTING_QUEUE", config.getRoutingQueue());
            }
            final HeaderCallOption clientProperties = (!callHeaders.keys().isEmpty()) ? new HeaderCallOption(callHeaders) : null;

            return new Client(client, authenticate(client, config.getUser(), config.getPassword(), config.getBearerToken(), clientProperties), cs, allocator, config);
        });
    }

    /**
     * Creates a FlightClient with optional TLS and interceptors.
     *
     * @param config    client configuration
     * @param allocator buffer allocator
     * @return FlightClient
     */
    private static FlightClient create(Configuration config, BufferAllocator allocator) {
        Location location = config.getTlsEnabled().booleanValue()
                ? Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())
                : Location.forGrpcInsecure(config.getFlightHost(), config.getFlightPort());
        return create(config, allocator, location,
                config.getPassword() != null && !config.getPassword().isEmpty());
    }

    /**
     * Builds a client for an endpoint-advertised location. Authentication is not
     * repeated: the credential obtained from the coordinator is attached to the
     * routed getStream call via {@link #callOptions()}.
     */
    private static FlightClient create(Configuration config, BufferAllocator allocator,
            Location location, boolean captureBearerToken) {
        URI uri = location.getUri();
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        FlightClient.Builder builder = FlightClient.builder().allocator(allocator).location(location);

        if ("grpc+tls".equals(scheme)) {
            builder.useTls();
            if (config.getTruststoreJks() == null || config.getTruststoreJks().isEmpty()) {
                builder.verifyServer(config.verifyServer());
            } else {
                builder.trustedCertificates(new ByteArrayInputStream(config.getCertificateBytes()));
            }
        } else if ("grpc+tcp".equals(scheme) || "grpc+unix".equals(scheme)) {
            if (config.getTlsEnabled().booleanValue()) {
                throw new IllegalArgumentException(
                        "Refusing to forward Flight credentials from TLS to plaintext endpoint " + uri);
            }
        } else {
            throw new IllegalArgumentException("Unsupported Flight endpoint location: " + uri);
        }

        if (captureBearerToken) {
            builder.intercept(Client.factory);
        }
        return builder.build();
    }

    /**
     * Authenticates with the Flight server using password or bearer token.
     *
     * @param client           FlightClient
     * @param user             username
     * @param password         password (null for bearer-only)
     * @param bearerToken      bearer token (null for password-only)
     * @param clientProperties optional client properties
     * @return credential call option
     */
    static CredentialCallOption authenticate(FlightClient client, String user, String password, String bearerToken, HeaderCallOption clientProperties) {
        final java.util.List<CallOption> callOptions = new java.util.ArrayList<>();

        LOGGER.info("Client.authenticate: clientProperties={}", clientProperties);
        if (clientProperties != null) {
            callOptions.add(clientProperties);
        }
        boolean usePassword = (password != null && !password.isEmpty());
        CredentialCallOption credentialOption = new CredentialCallOption(
                usePassword ? new BasicAuthCredentialWriter(user, password) : new BearerCredentialWriter(bearerToken));
        callOptions.add(credentialOption);
        LOGGER.info("Client.authenticate: callOptions={}", callOptions);
        client.handshake(callOptions.toArray(new CallOption[0]));
        LOGGER.info("Client.authenticate: handshake finished");
        return usePassword ? Client.factory.getCredentialCallOption() : credentialOption;
    }
    /**
     * Executes a call with exponential backoff retry on recoverable errors.
     *
     * @param callable  the call to execute
     * @param operation operation name for logging
     * @return result
     */
    public <T> T retryWithBackoff(Callable<T> callable, String operation) {
        int maxRetries = config.getMaxRetries();
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                long delay = calculateBackoff(attempt);
                sleepQuietly(delay);
            }
            try {
                return callable.call();
            } catch (Exception e) {
                lastException = e;
                logError(e, attempt, maxRetries, operation);
                if (shouldRetry(e, attempt, maxRetries)) {
                    continue;
                }
                break;
            }
        }

        throw wrapException(lastException, operation, maxRetries);
    }

    private long calculateBackoff(int attempt) {
        return config.getRetryBackoffMs() * (1L << (attempt - 1));
    }

    private void sleepQuietly(long delay) {
        try {
            LOGGER.info("Retry waiting {}ms", delay);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry", e);
        }
    }

    /**
     * Returns {@code true} if a retry is allowed: attempts remain and the exception is retryable
     * (internal FlightRuntimeException may still be retried, others always are).
     *
     * @param e          the exception that occurred during the operation
     * @param attempt    the current attempt index (0-based)
     * @param maxRetries the maximum number of retry attempts allowed (inclusive of the initial attempt)
     * @return {@code true} if the operation should be retried, {@code false} otherwise
     */
    private boolean shouldRetry(Exception e, int attempt, int maxRetries) {
        if (attempt >= maxRetries) {
            return false; // лимит исчерпан
        }
        if (e instanceof FlightRuntimeException) {
            return isInternalError((FlightRuntimeException) e);
        }

        return true;
    }

    private void logError(Exception e, int attempt, int maxRetries, String operation) {
        LOGGER.warn("Error in '{}' (attempt {} of {}): {}",
            operation, attempt, maxRetries + 1, e.getMessage());
    }

    private RuntimeException wrapException(Exception lastException, String operation, int maxRetries) {
        if (lastException instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException("Failed " + operation + " after " + (maxRetries + 1) + " attempts",
            lastException);
    }

    /**
     * Checks if a FlightRuntimeException is recoverable via retry.
     *
     * @param e flight exception
     * @return true if retryable
     */
    private static boolean isInternalError(FlightRuntimeException e) {
        Throwable cause = e.getCause();
        if (cause instanceof io.grpc.StatusRuntimeException) {
            io.grpc.Status.Code code = ((io.grpc.StatusRuntimeException) cause).getStatus().getCode();
            return code == io.grpc.Status.Code.INTERNAL
                    || code == io.grpc.Status.Code.UNAVAILABLE
                    || code == io.grpc.Status.Code.RESOURCE_EXHAUSTED
                    || code == io.grpc.Status.Code.DEADLINE_EXCEEDED
                    || code == io.grpc.Status.Code.ABORTED;
        }
        return false;
    }

    /**
     * Opens a FlightStream for the given endpoint.
     *
     * @param fep flight endpoint with ticket
     * @return FlightStream
     * @throws Exception on stream open failure
     */
    public FlightStream openStream(FlightEndpoint fep) throws Exception {
        String ticketHex = ticketHex(fep.getTicket());
        LOGGER.debug("node={} client=openStream ticket={} locations={}",
                NODE, ticketHex, fep.getLocations());

        if (fep.getLocations().isEmpty()) {
            LOGGER.debug("node={} client=openStreamPrimary ticket={}", NODE, ticketHex);
            return this.client.getStream(fep.getTicket(), callOptions());
        }

        IllegalArgumentException unsupported = null;
        for (Location location : fep.getLocations()) {
            if (Location.reuseConnection().equals(location) || isPrimaryLocation(location)) {
                LOGGER.debug("node={} client=openStreamPrimary ticket={} location={}",
                        NODE, ticketHex, location.getUri());
                return this.client.getStream(fep.getTicket(), callOptions());
            }
            try {
                LOGGER.debug("node={} client=routeStream ticket={} location={}",
                        NODE, ticketHex, location.getUri());
                FlightClient routedClient = this.endpointClients.computeIfAbsent(
                        location.getUri(), uri -> create(this.config, this.allocator, location, false));
                return routedClient.getStream(fep.getTicket(), callOptions());
            } catch (IllegalArgumentException ex) {
                unsupported = ex;
            }
        }

        if (unsupported != null) {
            throw unsupported;
        }
        throw new IllegalArgumentException("Flight endpoint has no usable location: " + fep);
    }

    private boolean isPrimaryLocation(Location location) {
        URI uri = location.getUri();
        String expectedScheme = this.config.getTlsEnabled().booleanValue()
                ? "grpc+tls" : "grpc+tcp";
        return expectedScheme.equalsIgnoreCase(uri.getScheme())
                && this.config.getFlightPort() == uri.getPort()
                && uri.getHost() != null
                && uri.getHost().equalsIgnoreCase(this.config.getFlightHost());
    }

    /**
     * Returns the client configuration.
     *
     * @return configuration
     */
    public Configuration getConfig() {
        return this.config;
    }

    /**
     * Converts a Flight ticket to hex string for logging.
     */
    private static String ticketHex(Ticket ticket) {
        if (ticket == null) {
            return "null";
        }
        byte[] bytes = ticket.getBytes();
        if (bytes == null || bytes.length == 0) {
            return "empty";
        }
        int len = Math.min(bytes.length, 8);
        StringBuilder sb = new StringBuilder(len * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
            if (sb.length() >= 16) {
                break;
            }
        }
        if (bytes.length > 8) {
            sb.append("..");
        }
        return sb.toString();
    }
}
