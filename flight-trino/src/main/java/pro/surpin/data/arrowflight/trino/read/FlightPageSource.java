package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.type.ArrowToTrinoBlockConverter;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalLong;

/** Streams Arrow batches of one Flight endpoint as Trino pages. */
final class FlightPageSource implements ConnectorPageSource {
    private final FlightService.BatchReader reader;
    private final List<FlightColumnHandle> columns;
    private final List<String> countStates;

    private boolean finished;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private long memoryUsage;

    /**
     * Creates a page source.
     *
     * @param reader endpoint reader
     * @param columns output columns, possibly empty for row counting
     */
    FlightPageSource(FlightService.BatchReader reader, List<FlightColumnHandle> columns) {
        this(reader, columns, List.of());
    }

    /**
     * Creates a page source with partial count validation.
     *
     * @param reader endpoint reader
     * @param columns output columns
     * @param countStates count state column names
     */
    FlightPageSource(FlightService.BatchReader reader, List<FlightColumnHandle> columns, List<String> countStates) {
        this.reader = reader;
        this.columns = List.copyOf(columns);
        this.countStates = List.copyOf(countStates);
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getNextPage() {
        if (finished) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (!reader.next()) {
                close();
                return null;
            }
            VectorSchemaRoot root = reader.root();
            validateCountStates(root);
            memoryUsage = root.getFieldVectors().stream()
                    .mapToLong(FlightPageSource::retainedBytes).sum();
            int rows = root.getRowCount();
            Block[] blocks = new Block[columns.size()];
            for (int i = 0; i < blocks.length; i++) {
                FlightColumnHandle column = columns.get(i);
                FieldVector vector = root.getVector(column.remoteName());
                if (vector == null) {
                    throw new IllegalStateException("Flight stream has no column " + column.remoteName());
                }
                blocks[i] = ArrowToTrinoBlockConverter.toBlock(column.type(), vector, rows);
            }
            completedPositions += rows;
            for (FieldVector vector : root.getFieldVectors()) {
                completedBytes += vector.getBufferSize();
            }
            return new Page(rows, blocks);
        } catch (IOException e) {
            closeAfterFailure(e);
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            closeAfterFailure(e);
            throw e;
        } finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    /**
     * Checks count states before passing them to Trino.
     *
     * @param batch current Arrow batch
     * @throws IllegalStateException if a state is missing or invalid
     */
    private void validateCountStates(VectorSchemaRoot batch) {
        for (String name : countStates) {
            if (!(batch.getVector(name) instanceof BigIntVector vector)) {
                throw new IllegalStateException("Flight count state must be a signed BIGINT: " + name);
            }
            for (int row = 0; row < batch.getRowCount(); row++) {
                if (vector.isNull(row) || vector.get(row) < 0) {
                    throw new IllegalStateException("Flight count state must be non-null and nonnegative: " + name);
                }
            }
        }
    }

    @Override
    public long getMemoryUsage() {
        return memoryUsage;
    }

    /**
     * Closes the reader while preserving the original read or conversion failure.
     *
     * @param failure original failure
     */
    private void closeAfterFailure(Exception failure) {
        try {
            close();
        } catch (RuntimeException closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    /**
     * Counts retained buffers including nested values.
     * @param vector Arrow vector
     * @return retained bytes
     */
    private static long retainedBytes(FieldVector vector) {
        return vector.getFieldBuffers().stream().mapToLong(buffer -> buffer.capacity()).sum()
                + vector.getChildrenFromFields().stream().mapToLong(FlightPageSource::retainedBytes).sum();
    }

    @Override
    public void close() {
        if (!finished) {
            finished = true;
            memoryUsage = 0;
            reader.close();
        }
    }
}
