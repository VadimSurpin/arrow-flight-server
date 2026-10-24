package pro.surpin.data.arrowflight.server.adapters;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;
import pro.surpin.data.arrowflight.server.services.RuntimeFilters;
import pro.surpin.data.arrowflight.server.services.SplitBlockBloom;

/**
 * Drops the rows of a stream that a join's bloom filters would discard, so they never cross the network.
 *
 * <p>The stage reads each batch of a source root and copies the rows that pass every filter into a root of its own,
 * which is what the stream sends. A row passes a filter when its key is not null and some partition of the
 * filter accepts it. Filters on columns the source does not have, or whose type is not an integer type, are
 * ignored, which only costs speed.
 */
final class BloomRowFilter implements AutoCloseable {

    /** Reads a key out of a vector. */
    private interface Keys {
        boolean isNull(int row);

        long get(int row);
    }

    private record Test(Keys keys, SplitBlockBloom[] partitions) {
    }

    private final VectorSchemaRoot source;
    private final VectorSchemaRoot output;
    private final List<Test> tests;
    private final TransferPair[] copies;
    private int[] kept = new int[0];

    private BloomRowFilter(VectorSchemaRoot source, VectorSchemaRoot output, List<Test> tests) {
        this.source = source;
        this.output = output;
        this.tests = tests;
        this.copies = new TransferPair[source.getFieldVectors().size()];
        for (int i = 0; i < copies.length; i++) {
            copies[i] = source.getFieldVectors().get(i).makeTransferPair(output.getFieldVectors().get(i));
        }
    }

    /**
     * Builds the stage for a stream.
     *
     * @param allocator allocator for the output root
     * @param source the root the query fills batch by batch
     * @param blooms the filters to apply
     * @return the stage, or null when no filter applies to the source
     */
    static BloomRowFilter create(BufferAllocator allocator, VectorSchemaRoot source,
            List<RuntimeFilters.Bloom> blooms) {
        List<Test> tests = new ArrayList<>();
        for (RuntimeFilters.Bloom bloom : blooms) {
            Keys keys = keysOf(source, bloom.column());
            if (keys != null) {
                tests.add(new Test(keys, bloom.partitions().toArray(new SplitBlockBloom[0])));
            }
        }
        if (tests.isEmpty()) {
            return null;
        }
        VectorSchemaRoot output = VectorSchemaRoot.create(source.getSchema(), allocator);
        return new BloomRowFilter(source, output, tests);
    }

    private static Keys keysOf(VectorSchemaRoot source, String column) {
        String wanted = column.toLowerCase(Locale.ROOT);
        for (FieldVector vector : source.getFieldVectors()) {
            if (!vector.getName().toLowerCase(Locale.ROOT).equals(wanted)) {
                continue;
            }
            if (vector instanceof BigIntVector v) {
                return new Keys() {
                    public boolean isNull(int row) {
                        return v.isNull(row);
                    }

                    public long get(int row) {
                        return v.get(row);
                    }
                };
            }
            if (vector instanceof IntVector v) {
                return new Keys() {
                    public boolean isNull(int row) {
                        return v.isNull(row);
                    }

                    public long get(int row) {
                        return v.get(row);
                    }
                };
            }
            if (vector instanceof SmallIntVector v) {
                return new Keys() {
                    public boolean isNull(int row) {
                        return v.isNull(row);
                    }

                    public long get(int row) {
                        return v.get(row);
                    }
                };
            }
            if (vector instanceof TinyIntVector v) {
                return new Keys() {
                    public boolean isNull(int row) {
                        return v.isNull(row);
                    }

                    public long get(int row) {
                        return v.get(row);
                    }
                };
            }
            return null;
        }
        return null;
    }

    /** @return the root holding the rows that passed, to hand to the stream */
    VectorSchemaRoot root() {
        return output;
    }

    /**
     * Filters the source's current batch into the output root.
     *
     * @return the number of rows that passed
     */
    int next() {
        int rows = source.getRowCount();
        if (kept.length < rows) {
            kept = new int[rows];
        }
        int count = 0;
        for (int row = 0; row < rows; row++) {
            if (passes(row)) {
                kept[count++] = row;
            }
        }
        for (FieldVector vector : output.getFieldVectors()) {
            vector.setInitialCapacity(Math.max(count, 1));
        }
        output.allocateNew();
        for (int out = 0; out < count; out++) {
            for (TransferPair copy : copies) {
                copy.copyValueSafe(kept[out], out);
            }
        }
        output.setRowCount(count);
        return count;
    }

    private boolean passes(int row) {
        for (Test test : tests) {
            if (test.keys().isNull(row)) {
                return false;
            }
            long key = test.keys().get(row);
            boolean accepted = false;
            for (SplitBlockBloom partition : test.partitions()) {
                if (partition.mightContain(key)) {
                    accepted = true;
                    break;
                }
            }
            if (!accepted) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        output.close();
    }
}
