package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.model.FieldType;
import pro.surpin.data.arrowflight.client.read.EndpointReader;
import org.apache.arrow.vector.*;
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
import java.time.*;
import java.util.List;

/**
 * Streaming partition reader that reads data from remote Flight service
 * without accumulating all rows in memory.
 *
 * Stream handling lives in EndpointReader; this class converts the current
 * Arrow batch to Spark InternalRow on the fly. Only one batch is held in memory at a time.
 */
public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReader.class);

    private final Client client;
    private final Configuration configuration;
    private final InputPartition inputPartition;
    private EndpointReader reader;
    private boolean readerCreated = false;

    private VectorSchemaRoot root;
    private Field[] fields;

    // Schema from FlightInfo (what Spark's codegen was compiled against).
    // Kept separately because `fields` follows the stream schema, which may
    // differ in integer width from the schema advertised by FlightInfo.
    // getValue() uses sparkFields to return the exact Java type Spark's getInt()/getByte() expects.
    private Field[] sparkFields;

    // current row position within the current batch
    private int rowIdx = 0;
    private int batchRowCount = 0;

    // whether we have a valid current row
    private boolean hasCurrent = false;
    // batch iteration is a separate PartitionReader mode and must not mix with rows
    private boolean columnarMode = false;
    private boolean rowMode = false;

    /**
     * Construct a streaming partition reader
     * @param configuration - the configuration of remote flight service
     * @param inputPartition - the input partition to read
     */
    public FlightPartitionReader(Configuration configuration, InputPartition inputPartition) {
        this(Client.getOrCreate(configuration), configuration, inputPartition);
    }

    /**
     * Construct a reader with an explicit client.
     * @param client - the Flight client
     * @param configuration - the configuration of remote flight service
     * @param inputPartition - the input partition to read
     */
    FlightPartitionReader(Client client, Configuration configuration, InputPartition inputPartition) {
        this.client = client;
        this.configuration = configuration;
        this.inputPartition = inputPartition;
        LOGGER.debug("spark=readerCreate partition={} class={}",
                inputPartition, inputPartition.getClass().getSimpleName());
    }

    /**
     * Creates the stream reader for a partition.
     * @param client - the Flight client
     * @param configuration - retry settings
     * @param partition - the input partition
     * @return reader, or null for an unsupported partition
     * @throws IOException if the partition schema cannot be parsed
     */
    private static EndpointReader createReader(Client client, Configuration configuration, InputPartition partition)
            throws IOException {
        if (partition instanceof FlightInputPartition.FlightEndpointInputPartition endpointPartition) {
            return EndpointReader.forEndpoint(client, configuration,
                    endpointPartition.getEndpoint(), endpointPartition.getSchema());
        }
        if (partition instanceof FlightInputPartition.FlightQueryInputPartition queryPartition) {
            return EndpointReader.forQuery(client, configuration, queryPartition.getQuery());
        }
        LOGGER.error("FlightPartitionReader: Unsupported partition, no data read: {}", partition);
        return null;
    }

    @Override
    public boolean next() throws IOException {
        if (this.columnarMode) {
            throw new IllegalStateException("Cannot mix row and columnar iteration");
        }
        this.rowMode = true;
        if (this.hasCurrent && this.rowIdx + 1 < this.batchRowCount) {
            this.rowIdx++;
            return true;
        }
        if (!advance()) {
            this.hasCurrent = false;
            return false;
        }
        this.fields = Field.from(this.reader.streamSchema());
        this.sparkFields = this.reader.plannedSchema() == null
                ? this.fields : Field.from(this.reader.plannedSchema());
        this.rowIdx = 0;
        this.hasCurrent = true;
        return true;
    }

    /**
     * Advances by one non-empty Arrow record batch without materializing rows.
     * Used by the DataSource V2 columnar reader.
     *
     * @return true when {@link #currentBatch()} contains a batch
     * @throws IOException on Flight stream failure
     */
    public boolean nextBatch() throws IOException {
        if (this.rowMode) {
            throw new IllegalStateException("Cannot mix row and columnar iteration");
        }
        this.columnarMode = true;
        return advance();
    }

    /**
     * Moves the stream reader to the next non-empty batch.
     * @return true when a batch is available
     * @throws IOException on Flight stream failure
     */
    private boolean advance() throws IOException {
        if (!this.readerCreated) {
            this.readerCreated = true;
            this.reader = createReader(this.client, this.configuration, this.inputPartition);
        }
        if (this.reader == null || !this.reader.nextBatch()) {
            this.root = null;
            this.batchRowCount = 0;
            return false;
        }
        this.root = this.reader.root();
        this.batchRowCount = this.root.getRowCount();
        return true;
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
        if (this.reader != null) {
            this.reader.close();
        }
        this.root = null;
        this.hasCurrent = false;
        this.batchRowCount = 0;
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
}
