package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.type.ArrowBlocks;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalLong;

/** Streams Arrow batches of one Flight endpoint as Trino pages. */
final class FlightPageSource implements ConnectorPageSource {
    private final FlightService.BatchReader reader;
    private final List<FlightColumnHandle> columns;

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
        this.reader = reader;
        this.columns = List.copyOf(columns);
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
                blocks[i] = ArrowBlocks.toBlock(column.type(), vector, rows);
            }
            completedPositions += rows;
            for (FieldVector vector : root.getFieldVectors()) {
                completedBytes += vector.getBufferSize();
            }
            return new Page(rows, blocks);
        } catch (IOException e) {
            close();
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            close();
            throw e;
        } finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    @Override
    public long getMemoryUsage() {
        return memoryUsage;
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
