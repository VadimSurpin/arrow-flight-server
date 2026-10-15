package pro.surpin.data.arrowflight.client.read;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.ElapsedTime;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Engine-neutral reader of one scan partition: opens the Flight stream with retries
 * and yields non-empty Arrow batches one at a time.
 *
 * A partition is either a planned endpoint or a query that is planned on the server when opened.
 * Batches are owned by the stream and stay valid until the next call to nextBatch or close.
 */
public final class EndpointReader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointReader.class);
    private static final String NODE = hostName();

    private final Client client;
    private final Configuration configuration;
    private final Endpoint endpoint;
    private final String query;

    private Schema plannedSchema;
    private FlightStream stream;
    private VectorSchemaRoot root;
    private boolean opened;
    private boolean firstBatchPending;
    private long startNanos;
    private int batches;
    private long rows;

    /**
     * Creates a reader.
     * @param client Flight client
     * @param configuration retry settings
     * @param endpoint planned endpoint, or null for a query partition
     * @param schema planned schema for an endpoint partition
     * @param query query for a query partition, or null
     */
    private EndpointReader(Client client, Configuration configuration, Endpoint endpoint, Schema schema, String query) {
        this.client = client;
        this.configuration = configuration;
        this.endpoint = endpoint;
        this.plannedSchema = schema;
        this.query = query;
    }

    /**
     * Creates a reader for an endpoint returned by scan planning.
     * @param client Flight client
     * @param configuration retry settings
     * @param endpoint endpoint
     * @param schema schema advertised by the server for the planned query
     * @return reader; the stream is opened on the first nextBatch call
     */
    public static EndpointReader forEndpoint(Client client, Configuration configuration, Endpoint endpoint, Schema schema) {
        return new EndpointReader(client, configuration, endpoint, schema, null);
    }

    /**
     * Creates a reader for a partition query that is planned when the reader opens.
     * Only the first endpoint whose first batch has rows is read.
     * @param client Flight client
     * @param configuration retry settings
     * @param query partition query
     * @return reader; the query is planned on the first nextBatch call
     */
    public static EndpointReader forQuery(Client client, Configuration configuration, String query) {
        return new EndpointReader(client, configuration, null, null, query);
    }

    /**
     * Advances to the next non-empty batch, opening the stream on the first call.
     * A failure after rows were returned is not retried, because a new stream would replay them.
     * @return true when root() holds a non-empty batch, false at the end of data
     * @throws IOException if opening or reading the stream fails
     */
    public boolean nextBatch() throws IOException {
        try {
            if (!opened) {
                opened = true;
                startNanos = System.nanoTime();
                if (!openWithRetry()) {
                    LOGGER.debug("node={} client=readerNoData query={}", NODE, query);
                    close();
                    return false;
                }
            }
            if (stream == null) {
                return false;
            }
            if (firstBatchPending) {
                firstBatchPending = false;
                if (root.getRowCount() > 0) {
                    countBatch();
                    return true;
                }
            }
            while (stream.next()) {
                root = stream.getRoot();
                countBatch();
                if (root.getRowCount() > 0) {
                    return true;
                }
            }
            LOGGER.debug("node={} client=readerCompleted batches={} rows={} elapsed={}",
                    NODE, batches, rows, ElapsedTime.format(startNanos));
            close();
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close();
            throw new IOException("Interrupted while reading Arrow batch from Flight stream", e);
        } catch (Exception e) {
            LOGGER.error("node={} client=readerFailed batches={} rows={} error='{}'",
                    NODE, batches, rows, e.getMessage(), e);
            close();
            throw new IOException("Error reading Arrow batch from Flight stream", e);
        }
    }

    /**
     * Returns the current batch.
     * @return current batch, valid until the next nextBatch or close call
     * @throws IllegalStateException if there is no current batch
     */
    public VectorSchemaRoot root() {
        if (root == null) {
            throw new IllegalStateException("No current Arrow batch. Call nextBatch() first.");
        }
        return root;
    }

    /**
     * Returns the schema advertised by the server when the query was planned.
     * Engines compile against this schema; stream batches may use narrower integer types.
     * @return planned schema, or null before a query partition is opened
     */
    public Schema plannedSchema() {
        return plannedSchema;
    }

    /**
     * Returns the schema of the open stream.
     * @return stream schema
     * @throws IllegalStateException if the stream is not open
     */
    public Schema streamSchema() {
        if (stream == null) {
            throw new IllegalStateException("The Flight stream is not open.");
        }
        return stream.getSchema();
    }

    /**
     * Closes the stream; the reader returns no more batches afterwards.
     */
    @Override
    public void close() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Flight stream: {}", e.getMessage());
        } finally {
            stream = null;
            root = null;
            firstBatchPending = false;
            opened = true;
        }
    }

    /**
     * Counts a received batch for progress logging.
     */
    private void countBatch() {
        batches++;
        rows += root.getRowCount();
        if (batches == 1) {
            LOGGER.debug("node={} client=ttfB batchRowCount={} elapsed={}",
                    NODE, root.getRowCount(), ElapsedTime.format(startNanos));
        } else if (batches % 10 == 0) {
            LOGGER.debug("node={} client=readerProgress batches={} rows={} elapsed={}",
                    NODE, batches, rows, ElapsedTime.format(startNanos));
        }
    }

    /**
     * Opens the stream, retrying with exponential backoff.
     * @return true when a stream is open
     * @throws Exception if the last attempt fails
     */
    private boolean openWithRetry() throws Exception {
        int maxRetries = configuration.getMaxRetries();
        long backoffMs = configuration.getRetryBackoffMs();
        for (int attempt = 0; ; attempt++) {
            try {
                closeStreamOnly();
                return endpoint != null ? openEndpoint() : openQuery();
            } catch (Exception e) {
                LOGGER.warn("Failed to open Flight stream (attempt {} of {}): {}", attempt + 1, maxRetries + 1, e.getMessage());
                if (attempt >= maxRetries) {
                    throw e;
                }
                long delay = backoffMs * (1L << attempt);
                LOGGER.info("Retrying in {}ms", delay);
                Thread.sleep(delay);
            }
        }
    }

    /**
     * Opens the planned endpoint and loads its first batch.
     * @return always true
     * @throws Exception if the stream cannot be opened
     */
    private boolean openEndpoint() throws Exception {
        LOGGER.debug("node={} client=openEndpointStream uris={}", NODE, Arrays.toString(endpoint.getURIs()));
        openStream(endpoint);
        return true;
    }

    /**
     * Plans the query and opens the first endpoint whose first batch has rows.
     * @return true when such an endpoint was found
     * @throws Exception if every endpoint fails to open
     */
    private boolean openQuery() throws Exception {
        QueryEndpoints planned = client.getQueryEndpoints(query);
        LOGGER.debug("EndpointReader.openQuery(): endpoints: {}", planned);
        this.plannedSchema = planned.getSchema();
        Endpoint[] endpoints = planned.getEndpoints();
        Exception lastError = null;
        for (int i = 0; i < endpoints.length; i++) {
            try {
                openStream(endpoints[i]);
                if (root.getRowCount() > 0) {
                    return true;
                }
                closeStreamOnly();
            } catch (Exception e) {
                lastError = e;
                closeStreamOnly();
                LOGGER.warn("Failed to open query endpoint {} of {}: {}", i + 1, endpoints.length, e.getMessage());
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        return false;
    }

    /**
     * Opens a stream and reads its first batch.
     * @param target endpoint to open
     * @throws Exception if the stream cannot be opened
     */
    private void openStream(Endpoint target) throws Exception {
        FlightEndpoint fep = new FlightEndpoint(new Ticket(target.getTicket()),
                Arrays.stream(target.getURIs()).map(Location::new).toArray(Location[]::new));
        this.stream = client.openStream(fep);
        this.root = stream.getRoot();
        this.firstBatchPending = stream.next();
    }

    /**
     * Closes the stream without marking the reader finished.
     */
    private void closeStreamOnly() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Flight stream: {}", e.getMessage());
        } finally {
            stream = null;
            root = null;
            firstBatchPending = false;
        }
    }

    /**
     * Resolves the local host name for log lines.
     * @return host name, or "unknown"
     */
    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
