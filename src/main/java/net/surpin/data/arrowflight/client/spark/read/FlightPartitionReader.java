package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Client;
import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Field;
import net.surpin.data.arrowflight.client.model.FieldType;
import net.surpin.data.arrowflight.client.query.Endpoint;
import net.surpin.data.arrowflight.client.query.QueryEndpoints;
import net.surpin.data.arrowflight.server.LogUtil;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.*;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming partition reader that reads data from remote Flight service
 * without accumulating all rows in memory.
 *
 * Each batch from FlightStream is consumed row-by-row and converted to Spark InternalRow
 * on the fly. Only one batch is held in memory at a time.
 */
public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReader.class);

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

    private final Client client;
    private final Configuration configuration;
    private final InputPartition inputPartition;

    // current FlightStream and its VectorSchemaRoot
    private FlightStream stream;
    private VectorSchemaRoot root;
    private Field[] fields;

    // Schema from FlightInfo (what Spark's codegen was compiled against).
    // Kept separately because `fields` is overwritten per batch with the stream schema,
    // which may differ in integer width from the schema advertised by FlightInfo.
    // getValue() uses sparkFields to return the exact Java type Spark's getInt()/getByte() expects.
    private Field[] sparkFields;

    // current row position within the current batch
    private int rowIdx = 0;
    private int batchRowCount = 0;

    // whether we have a valid current row
    private boolean hasCurrent = false;
    // batch iteration is a separate PartitionReader mode and must not mix with rows
    private boolean columnarMode = false;
    private boolean firstColumnarBatch = true;

    // retry state
    private int openRetryCount = 0;
    private int nextRetryCount = 0;
    private static final int MAX_RETRIES = 3;
    private static final long BACKOFF_BASE_MS = 1000;
    private static final int MAX_NEXT_RETRIES = 2;

    // performance tracking
    private long readStartNanos;
    private int totalBatches;
    private long totalRows;

    /**
     * Construct a streaming partition reader
     * @param configuration - the configuration of remote flight service
     * @param inputPartition - the input partition to read
     */
    public FlightPartitionReader(Configuration configuration, InputPartition inputPartition) {
        this.configuration = configuration;
        this.inputPartition = inputPartition;
        this.client = Client.getOrCreate(configuration);
        LOGGER.debug("node={} spark=readerCreate partition={} class={}",
                NODE, inputPartition, inputPartition.getClass().getSimpleName());
    }

    FlightPartitionReader(FlightStream stream, VectorSchemaRoot root, Schema flightInfoSchema) {
        this.configuration = null;
        this.inputPartition = null;
        this.client = null;
        this.stream = stream;
        this.root = root;
        this.rowIdx = -1;
        this.batchRowCount = root.getRowCount();
        this.sparkFields = Field.from(flightInfoSchema);
        this.fields = this.sparkFields;
    }

    FlightPartitionReader(Client client, Configuration configuration, InputPartition inputPartition) {
        this.client = client;
        this.configuration = configuration;
        this.inputPartition = inputPartition;
    }

    @Override
    @SuppressWarnings("java:S3776") // Row iteration coordinates retry, stream, and batch state atomically.
    public boolean next() throws IOException {
        LOGGER.debug("node={} spark=readerNext", NODE);
        if (this.columnarMode) {
            throw new IllegalStateException("Cannot mix row and columnar iteration");
        }
        try {
            if (stream == null) {
                readStartNanos = System.nanoTime();
                totalBatches = 0;
                totalRows = 0;
                if (!openStream()) {
                    LOGGER.debug("node={} spark=readerNoData partition={}", NODE, inputPartition);
                    return false;
                }
                nextRetryCount = 0;
            }

            // move to next row in current batch
            rowIdx++;
            if (rowIdx < batchRowCount) {
                hasCurrent = true;
                return true;
            }

            LOGGER.debug("node={} spark=readerNextBatch", NODE);
            // current batch exhausted, try next batch
            while (true) {
                try {
                    if (!stream.next()) {
                        break;
                    }
                    root = stream.getRoot();
                    batchRowCount = root.getRowCount();
                    fields = Field.from(stream.getSchema());
                    rowIdx = 0;
                    totalBatches++;
                    totalRows += batchRowCount;
                    if (totalBatches == 1) {
                        LOGGER.debug("node={} spark=ttfB batchRowCount={} elapsed={}",
                                NODE, batchRowCount, elapsedNanos(readStartNanos));
                    }
                    if (totalBatches % 10 == 0) {
                        LOGGER.debug("node={} spark=readerProgress batches={} rows={} elapsed={}",
                                NODE, totalBatches, totalRows, elapsedNanos(readStartNanos));
                    }
                    nextRetryCount = 0;

                    if (batchRowCount > 0) {
                        hasCurrent = true;
                        return true;
                    }
                } catch (Exception midStreamError) {
                    // Restarting this stream here would replay rows already returned from
                    // the partition. Let Spark retry the whole task with the retained ticket.
                    LOGGER.warn("node={} spark=readerMidStreamError batches={} rows={} error='{}'",
                            NODE, totalBatches, totalRows, midStreamError.getMessage());
                    throw midStreamError;
                }
            }

            LOGGER.debug("node={} spark=readerCompleted batches={} rows={} elapsed={}",
                    NODE, totalBatches, totalRows, elapsedNanos(readStartNanos));
            // no more batches
            hasCurrent = false;
            closeStream();
            return false;

        } catch (Exception e) {
            LOGGER.error("node={} spark=readerFailed batches={} rows={} error='{}'",
                    NODE, totalBatches, totalRows, e.getMessage(), e);
            closeStream();
            throw new IOException(e);
        }
    }

    /**
     * Advances by one non-empty Arrow record batch without materializing rows.
     * Used by the DataSource V2 columnar reader.
     *
     * @return true when {@link #currentBatch()} contains a batch
     * @throws IOException on Flight stream failure
     */
    public boolean nextBatch() throws IOException {
        this.columnarMode = true;
        try {
            if (this.stream == null) {
                readStartNanos = System.nanoTime();
                totalBatches = 0;
                totalRows = 0;
                if (!openStream()) {
                    return false;
                }
            }

            if (this.firstColumnarBatch) {
                this.firstColumnarBatch = false;
                if (this.batchRowCount > 0) {
                    totalBatches++;
                    totalRows += this.batchRowCount;
                    LOGGER.debug("node={} spark=ttfB columnarBatchRowCount={} elapsed={}",
                            NODE, this.batchRowCount, elapsedNanos(readStartNanos));
                    return true;
                }
            }

            while (this.stream.next()) {
                this.root = this.stream.getRoot();
                this.batchRowCount = this.root.getRowCount();
                totalBatches++;
                totalRows += this.batchRowCount;
                if (totalBatches % 10 == 0) {
                    LOGGER.debug("node={} spark=columnarProgress batches={} rows={} elapsed={}",
                            NODE, totalBatches, totalRows, elapsedNanos(readStartNanos));
                }
                if (this.batchRowCount > 0) {
                    return true;
                }
            }

            LOGGER.debug("node={} spark=columnarCompleted batches={} rows={} elapsed={}",
                    NODE, totalBatches, totalRows, elapsedNanos(readStartNanos));
            closeStream();
            return false;
        } catch (Exception e) {
            LOGGER.error("node={} spark=columnarFailed batches={} rows={} error='{}'",
                    NODE, totalBatches, totalRows, e.getMessage(), e);
            closeStream();
            throw new IOException("Error reading Arrow batch from Flight stream", e);
        }
    }

    /**
     * Returns the current Flight-owned batch. It remains valid until the next call
     * to {@link #nextBatch()} or {@link #close()}.
     */
    public VectorSchemaRoot currentBatch() {
        if (!this.columnarMode || this.root == null || this.batchRowCount <= 0) {
            throw new IllegalStateException("No current Arrow batch. Call nextBatch() first.");
        }
        return this.root;
    }

    @Override
    public InternalRow get() {
        LOGGER.debug("FlightPartitionReader.get():");

        if (!hasCurrent || root == null) {
            throw new IllegalStateException("No current row. Call next() first.");
        }

        List<org.apache.arrow.vector.FieldVector> fieldVectors = root.getFieldVectors();
        int numCols = fieldVectors.size();
        Object[] values = new Object[numCols];

        Field[] expectedFields = (sparkFields != null && sparkFields.length == numCols) ? sparkFields : fields;
        for (int col = 0; col < numCols; col++) {
            values[col] = getValue(fieldVectors.get(col), rowIdx, expectedFields[col]);
        }

        return new GenericInternalRow(values);
    }

    @Override
    public void close() {
        closeStream();
    }

    /**
     * Open the appropriate stream based on input partition type
     * @return - true if stream opened successfully with data available
     * @throws Exception - if stream opening fails
     */
    private boolean openStream() throws Exception {
        if (this.inputPartition instanceof FlightInputPartition.FlightEndpointInputPartition endpointPartition) {
            return openEndpointStreamWithRetry(endpointPartition);
        } else if (this.inputPartition instanceof FlightInputPartition.FlightQueryInputPartition queryPartition) {
            return openQueryStreamWithRetry(queryPartition);
        } else {
            LOGGER.error("FlightPartitionReader.openStream(): Unsupported partition, no data read: {}", this.inputPartition);
            return false;
        }
    }

    /**
     * Open endpoint stream with exponential backoff retry
     * @param dePartition - the endpoint input partition
     * @return - true if stream opened successfully
     * @throws Exception - if all retries fail
     */
    private boolean openEndpointStreamWithRetry(FlightInputPartition.FlightEndpointInputPartition dePartition) throws Exception {
        int maxRetries = this.configuration.getMaxRetries();
        long backoffMs = this.configuration.getRetryBackoffMs();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                closeStream();
                return openEndpointStream(dePartition);
            } catch (Exception e) {
                LOGGER.warn("Failed to open endpoint stream (attempt {} of {}): {}", attempt, maxRetries + 1, e.getMessage());
                if (attempt >= maxRetries) {
                    throw e;
                }
                long delay = backoffMs * (1L << attempt);
                LOGGER.info("Retrying in {}ms", delay);
                Thread.sleep(delay);
            }
        }
        return false;
    }

    /**
     * Open query stream with endpoint fallback and retry logic
     * @param dqPartition - the query input partition
     * @return - true if stream opened successfully
     * @throws Exception - if all retries and endpoints fail
     */
    @SuppressWarnings("java:S3776") // Retry decisions intentionally remain beside endpoint failover state.
    private boolean openQueryStreamWithRetry(FlightInputPartition.FlightQueryInputPartition dqPartition) throws Exception {
        int maxRetries = this.configuration.getMaxRetries();
        long backoffMs = this.configuration.getRetryBackoffMs();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                closeStream();
                QueryEndpoints qeps = this.client.getQueryEndpoints(dqPartition.getQuery());
                LOGGER.debug("FlightPartitionReader.openQueryStream(): endpoints: {}", qeps);
                if (qeps.getEndpoints().length == 0) {
                    return false;
                }

                // try each endpoint in sequence
                Exception lastEpError = null;
                for (int epIdx = 0; epIdx < qeps.getEndpoints().length; epIdx++) {
                    Endpoint endpoint = qeps.getEndpoints()[epIdx];
                    try {
                        if (openSingleEndpoint(endpoint, qeps.getSchema())) {
                            return true;
                        }
                    } catch (Exception epErr) {
                        lastEpError = epErr;
                        LOGGER.warn("Failed to open query endpoint {} of {}: {}", epIdx + 1, qeps.getEndpoints().length, epErr.getMessage());
                    }
                }

                if (lastEpError instanceof RuntimeException) {
                    throw lastEpError;
                }
                if (lastEpError != null) {
                    throw lastEpError;
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to open query stream (attempt {} of {}): {}", attempt, maxRetries + 1, e.getMessage());
                if (attempt >= maxRetries) {
                    throw e;
                }
                long delay = backoffMs * (1L << attempt);
                LOGGER.info("Retrying in {}ms", delay);
                Thread.sleep(delay);
            }
        }
        return false;
    }

    /**
     * Open a single Flight endpoint and populate the stream
     * @param endpoint - the endpoint to connect to
     * @param schema - the expected schema
     * @return - true if data is available from the endpoint
     * @throws Exception - if connection fails
     */
    private boolean openSingleEndpoint(Endpoint endpoint, Schema schema) throws Exception {
        org.apache.arrow.flight.FlightEndpoint fep = new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(endpoint.getTicket()),
                Arrays.stream(endpoint.getURIs())
                        .map(org.apache.arrow.flight.Location::new)
                        .toArray(org.apache.arrow.flight.Location[]::new)
        );

        this.stream = this.client.openStream(fep);
        this.root = stream.getRoot();
        this.fields = Field.from(schema);
        this.sparkFields = this.fields;
        this.rowIdx = -1;
        this.batchRowCount = 0;

        if (stream.next()) {
            batchRowCount = root.getRowCount();
        }
        return batchRowCount > 0;
    }

    /**
     * Attempt to reopen the stream after a mid-stream failure.
     * Reconnects from the beginning of the partition.
     */
    private boolean reopenStream() {
        if (nextRetryCount >= MAX_NEXT_RETRIES) {
            LOGGER.error("Max mid-stream retries ({}) exceeded", MAX_NEXT_RETRIES);
            return false;
        }
        nextRetryCount++;
        long delay = BACKOFF_BASE_MS * (1L << (nextRetryCount - 1));
        LOGGER.info("Reopening stream after mid-stream failure, retry {}/{}, waiting {}ms",
                nextRetryCount, MAX_NEXT_RETRIES, delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        closeStream();
        try {
            return openStream();
        } catch (Exception e) {
            LOGGER.error("Reopen stream failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Open a stream for a direct endpoint partition
     * @param dePartition - the endpoint input partition
     * @return - true if stream opened successfully
     * @throws Exception - if stream opening fails
     */
    private boolean openEndpointStream(FlightInputPartition.FlightEndpointInputPartition dePartition) throws Exception {
        Endpoint endpoint = dePartition.getEndpoint();
        Schema schema = dePartition.getSchema();
        String ticketHex = bytesToHex(endpoint.getTicket());
        String uris = Arrays.toString(endpoint.getURIs());
        LOGGER.debug("node={} spark=openEndpointStream ticket={} uris={}",
                NODE, ticketHex, uris);

        org.apache.arrow.flight.FlightEndpoint fep = new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(endpoint.getTicket()),
                Arrays.stream(endpoint.getURIs())
                        .map(org.apache.arrow.flight.Location::new)
                        .toArray(org.apache.arrow.flight.Location[]::new)
        );

        this.stream = this.client.openStream(fep);
        this.root = stream.getRoot();
        this.fields = Field.from(schema);
        this.sparkFields = this.fields;  // FlightInfo schema = what Spark's codegen expects
        this.rowIdx = -1;
        this.batchRowCount = 0;

        // try first batch
        if (stream.next()) {
            batchRowCount = root.getRowCount();
        }

        LOGGER.debug("node={} spark=endpointStreamOpened ticket={} firstBatchRowCount={}",
                NODE, ticketHex, batchRowCount);
        return true;
    }

    /**
     * Close the current stream and reset reader state
     */
    private void closeStream() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).warn("Error closing Flight stream: " + e.getMessage());
        } finally {
            stream = null;
            root = null;
            fields = null;
            sparkFields = null;
            batchRowCount = 0;
            rowIdx = 0;
            hasCurrent = false;
        }
    }

    /**
     * Extract a single value from a FieldVector at the given row index,
     * converting it to a Spark-compatible object.
     * @param vector - the field vector to read from
     * @param rowIndex - the row index to extract
     * @param field - the expected field metadata for type coercion
     * @return - the Spark-compatible value, or null if the field is null
     */
    private static Object getValue(org.apache.arrow.vector.FieldVector vector, int rowIndex, Field field) {
        if (vector.isNull(rowIndex)) {
            return null;
        }

        Object result;
        result = extractInt(vector, rowIndex, field);
        if (result != null) {
            return result;
        }
        result = extractFloat(vector, rowIndex);
        if (result != null) {
            return result;
        }
        result = extractDecimal(vector, rowIndex, field);
        if (result != null) {
            return result;
        }
        result = extractString(vector, rowIndex);
        if (result != null) {
            return result;
        }
        if (vector instanceof BitVector bitVector) {
            return bitVector.get(rowIndex) != 0;
        }
        result = extractDate(vector, rowIndex);
        if (result != null) {
            return result;
        }
        result = extractTime(vector, rowIndex);
        if (result != null) {
            return result;
        }
        result = extractTimestamp(vector, rowIndex);
        if (result != null) {
            return result;
        }
        result = extractBinary(vector, rowIndex);
        if (result != null) {
            return result;
        }
        result = extractUInt(vector, rowIndex);
        if (result != null) {
            return result;
        }
        result = extractInterval(vector, rowIndex);
        if (result != null) {
            return result;
        }
        if (vector instanceof NullVector) {
            return null;
        }
        return vector.getObject(rowIndex);
    }

    @SuppressWarnings("java:S3776") // Exact-width Arrow integer vectors require explicit conversions.
    private static Object extractInt(
            org.apache.arrow.vector.FieldVector vector, int rowIndex, Field field) {
        if (vector instanceof TinyIntVector vec) {
            byte b = vec.get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case INT:   return (int) b;
                    case SHORT: return (short) b;
                    case LONG, BIGINT: return (long) b;
                    default: break;
                }
            }
            return b;
        }
        if (vector instanceof SmallIntVector vec) {
            short s = vec.get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case INT:   return (int) s;
                    case LONG, BIGINT: return (long) s;
                    default: break;
                }
            }
            return s;
        }
        if (vector instanceof IntVector vec) {
            int i = vec.get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case LONG, BIGINT: return (long) i;
                    case SHORT: return (short) i;
                    case BYTE:  return (byte) i;
                    default: break;
                }
            }
            return i;
        }
        if (vector instanceof BigIntVector vec) {
            return vec.get(rowIndex);
        }
        return null;
    }

    private static Object extractFloat(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof Float4Vector vec) {
            return vec.get(rowIndex);
        }
        if (vector instanceof Float8Vector vec) {
            return vec.get(rowIndex);
        }
        return null;
    }

    private static Object extractDecimal(
            org.apache.arrow.vector.FieldVector vector, int rowIndex, Field field) {
        if (vector instanceof DecimalVector vec) {
            return toSparkDecimal(vec.getObject(rowIndex), field);
        }
        if (vector instanceof Decimal256Vector vec) {
            return toSparkDecimal(vec.getObject(rowIndex), field);
        }
        return null;
    }

    private static Object extractString(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof VarCharVector vec) {
            return UTF8String.fromString(vec.getObject(rowIndex).toString());
        }
        if (vector instanceof LargeVarCharVector vec) {
            return UTF8String.fromString(vec.getObject(rowIndex).toString());
        }
        return null;
    }

    private static Object extractDate(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof DateDayVector vec) {
            return vec.get(rowIndex);
        }
        if (vector instanceof DateMilliVector vec) {
            return Math.toIntExact(Math.floorDiv(vec.get(rowIndex), 86_400_000L));
        }
        return null;
    }

    private static Object extractTime(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof TimeSecVector vec) {
            return vec.getObject(rowIndex).toString();
        }
        if (vector instanceof TimeMilliVector vec) {
            return vec.getObject(rowIndex).toString();
        }
        if (vector instanceof TimeMicroVector vec) {
            return vec.getObject(rowIndex).toString();
        }
        if (vector instanceof TimeNanoVector vec) {
            return vec.getObject(rowIndex).toString();
        }
        return null;
    }

    private static Object extractTimestamp(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof TimeStampSecVector vec) {
            return Math.multiplyExact(vec.get(rowIndex), 1_000_000L);
        }
        if (vector instanceof TimeStampSecTZVector vec) {
            return Math.multiplyExact(vec.get(rowIndex), 1_000_000L);
        }
        if (vector instanceof TimeStampMilliVector vec) {
            return Math.multiplyExact(vec.get(rowIndex), 1_000L);
        }
        if (vector instanceof TimeStampMilliTZVector vec) {
            return Math.multiplyExact(vec.get(rowIndex), 1_000L);
        }
        if (vector instanceof TimeStampMicroVector vec) {
            return vec.get(rowIndex);
        }
        if (vector instanceof TimeStampMicroTZVector vec) {
            return vec.get(rowIndex);
        }
        if (vector instanceof TimeStampNanoVector vec) {
            return Math.floorDiv(vec.get(rowIndex), 1_000L);
        }
        if (vector instanceof TimeStampNanoTZVector vec) {
            return Math.floorDiv(vec.get(rowIndex), 1_000L);
        }
        return null;
    }

    private static Object extractBinary(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof VarBinaryVector vec) {
            return vec.getObject(rowIndex);
        }
        if (vector instanceof LargeVarBinaryVector vec) {
            return vec.getObject(rowIndex);
        }
        if (vector instanceof FixedSizeBinaryVector vec) {
            return vec.getObject(rowIndex);
        }
        return null;
    }

    private static Object extractUInt(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof UInt1Vector vec) {
            return (int) vec.get(rowIndex);
        }
        if (vector instanceof UInt2Vector vec) {
            return (int) vec.get(rowIndex);
        }
        if (vector instanceof UInt4Vector vec) {
            return (int) vec.get(rowIndex);
        }
        if (vector instanceof UInt8Vector vec) {
            return vec.get(rowIndex);
        }
        return null;
    }

    private static Object extractInterval(
            org.apache.arrow.vector.FieldVector vector, int rowIndex) {
        if (vector instanceof DurationVector vec) {
            return vec.getObject(rowIndex);
        }
        if (vector instanceof IntervalYearVector vec) {
            return vec.get(rowIndex);
        }
        if (vector instanceof IntervalDayVector vec) {
            return vec.getObject(rowIndex);
        }
        return null;
    }

    private static Decimal toSparkDecimal(BigDecimal value, Field field) {
        if (value == null) {
            return null;
        }
        if (field != null && field.getType() instanceof FieldType.DecimalType decimalType) {
            return Decimal.apply(value, decimalType.getPrecision(), decimalType.getScale());
        }
        return Decimal.apply(value);
    }

    /**
     * Formats elapsed nanoseconds as a human-readable duration.
     */
    private static String elapsedNanos(long startNanos) {
        return LogUtil.elapsedNanos(startNanos);
    }

    /**
     * Converts byte array to hex string for ticket logging.
     */
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "empty";
        }
        int len = Math.min(bytes.length, 8);
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }
        if (bytes.length > 8) {
            sb.append("..");
        }
        return sb.toString();
    }
}
