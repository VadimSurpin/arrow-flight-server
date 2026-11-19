package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import pro.surpin.data.arrowflight.server.TableProvider.EmptyScan;
import pro.surpin.data.arrowflight.server.TableProvider.IcebergSnapshot;
import pro.surpin.data.arrowflight.server.model.HandleState.TableScanState;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/** Streams the visible records of a pinned Iceberg table as Arrow batches. */
final class IcebergArrowReader extends ArrowReader {
    private final TableScanState scan;
    private final FileIO io;
    private final int batchSize;
    private final BooleanSupplier cancelled;
    private CloseableIterable<Record> records;
    private CloseableIterator<Record> rows;
    private boolean closed;

    /**
     * Creates a reader for a pinned scan.
     *
     * @param allocator Arrow allocator
     * @param scan pinned scan
     * @param io Iceberg file access
     * @param batchSize maximum rows per batch
     * @param cancelled client cancellation check
     */
    IcebergArrowReader(BufferAllocator allocator, TableScanState scan, FileIO io,
            int batchSize, BooleanSupplier cancelled) {
        super(allocator);
        this.scan = scan;
        this.io = io;
        this.batchSize = batchSize;
        this.cancelled = cancelled;
    }

    @Override
    protected Schema readSchema() throws IOException {
        return Schema.fromJSON(scan.schemaJson());
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        checkCancelled();
        if (scan.source() instanceof EmptyScan) {
            return false;
        }
        if (records == null) {
            openRows((IcebergSnapshot) scan.source());
        }
        if (!rows.hasNext()) {
            return false;
        }
        prepareLoadNextBatch();
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.allocateNew();
        List<FieldVector> columns = root.getFieldVectors();
        int count = 0;
        do {
            checkCancelled();
            writeRecord(columns, count++, rows.next());
        } while (count < batchSize && rows.hasNext());
        root.setRowCount(count);
        return true;
    }

    /**
     * Opens the records in the metadata file selected during planning.
     *
     * @param snapshot pinned metadata and snapshot
     * @throws IOException if the metadata no longer identifies the selected snapshot
     */
    private void openRows(IcebergSnapshot snapshot) throws IOException {
        TableMetadata metadata = TableMetadataParser.read(io, snapshot.metadataFile().toString());
        if (metadata.formatVersion() != 2 || metadata.currentSnapshot() == null
                || metadata.currentSnapshot().snapshotId() != snapshot.snapshotId()) {
            throw new IOException("Iceberg metadata does not contain the pinned current snapshot "
                    + snapshot.snapshotId());
        }
        BaseTable table = new BaseTable(new StaticTableOperations(metadata, io), snapshot.metadataFile().toString());
        records = IcebergGenerics.read(table).build();
        rows = records.iterator();
    }

    /** Checks whether the client still wants the scan. */
    private void checkCancelled() {
        if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            throw CallStatus.CANCELLED.withDescription("Iceberg scan cancelled").toRuntimeException();
        }
    }

    /**
     * Writes an Iceberg record into its Arrow column vectors.
     *
     * @param columns destination vectors in schema order
     * @param index destination row
     * @param record source record
     */
    private static void writeRecord(List<FieldVector> columns, int index, Record record) {
        for (int column = 0; column < columns.size(); column++) {
            writeValue(columns.get(column), index, record.get(column));
        }
    }

    /**
     * Writes an Iceberg value into its Arrow vector.
     *
     * @param vector destination vector
     * @param index destination index
     * @param value source value
     */
    private static void writeValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            vector.setNull(index);
            return;
        }
        switch (vector) {
            case BitVector v -> v.setSafe(index, (Boolean) value ? 1 : 0);
            case IntVector v -> v.setSafe(index, (Integer) value);
            case BigIntVector v -> v.setSafe(index, (Long) value);
            case Float4Vector v -> v.setSafe(index, (Float) value);
            case Float8Vector v -> v.setSafe(index, (Double) value);
            case DateDayVector v -> v.setSafe(index, Math.toIntExact(((LocalDate) value).toEpochDay()));
            case TimeMicroVector v -> v.setSafe(index, ((LocalTime) value).toNanoOfDay() / 1_000);
            case TimeStampVector v -> v.setSafe(index, timestampMicros(value));
            case VarCharVector v -> v.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
            case VarBinaryVector v -> v.setSafe(index, binary(value));
            case FixedSizeBinaryVector v -> v.setSafe(index, binary(value));
            case DecimalVector v -> v.setSafe(index, (BigDecimal) value);
            case MapVector v -> writeMap(v, index, (Map<?, ?>) value);
            case ListVector v -> writeList(v, index, (List<?>) value);
            case StructVector v -> {
                v.setIndexDefined(index);
                writeRecord(v.getChildrenFromFields(), index, (Record) value);
            }
            default -> throw new UnsupportedOperationException("Unsupported Iceberg vector " + vector.getClass());
        }
    }

    /**
     * Writes a list and its elements.
     *
     * @param vector list vector
     * @param index destination index
     * @param values list elements
     */
    private static void writeList(ListVector vector, int index, List<?> values) {
        int offset = vector.startNewValue(index);
        FieldVector elements = vector.getDataVector();
        for (int i = 0; i < values.size(); i++) {
            writeValue(elements, offset + i, values.get(i));
        }
        vector.endValue(index, values.size());
    }

    /**
     * Writes a map and its entries.
     *
     * @param vector map vector
     * @param index destination index
     * @param values map entries
     */
    private static void writeMap(MapVector vector, int index, Map<?, ?> values) {
        int offset = vector.startNewValue(index);
        StructVector entries = (StructVector) vector.getDataVector();
        List<FieldVector> children = entries.getChildrenFromFields();
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            entries.setIndexDefined(offset);
            writeValue(children.get(0), offset, entry.getKey());
            writeValue(children.get(1), offset, entry.getValue());
            offset++;
        }
        vector.endValue(index, values.size());
    }

    /**
     * Converts an Iceberg timestamp to Arrow microseconds.
     *
     * @param value timestamp value
     * @return microseconds since the epoch
     */
    private static long timestampMicros(Object value) {
        Instant instant = value instanceof OffsetDateTime zoned ? zoned.toInstant()
                : ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }

    /**
     * Converts an Iceberg binary value to bytes.
     *
     * @param value binary or UUID value
     * @return bytes in Arrow representation
     */
    private static byte[] binary(Object value) {
        return switch (value) {
            case byte[] bytes -> bytes;
            case UUID uuid -> ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
                    .putLong(uuid.getLeastSignificantBits()).array();
            case ByteBuffer buffer -> {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                yield bytes;
            }
            default -> throw new IllegalArgumentException("Unsupported binary value " + value.getClass());
        };
    }

    @Override
    public long bytesRead() {
        return 0L;
    }

    @Override
    public void close(boolean closeReadSource) throws IOException {
        if (!closed) {
            closed = true;
            super.close(closeReadSource);
        }
    }

    @Override
    protected void closeReadSource() throws IOException {
        try (io; CloseableIterable<Record> openRecords = records) {
            // Closing the iterable also closes its iterators.
        }
    }
}
