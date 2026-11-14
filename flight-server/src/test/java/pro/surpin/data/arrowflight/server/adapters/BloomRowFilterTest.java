package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.services.BloomTestSupport;
import pro.surpin.data.arrowflight.server.services.RuntimeFilters;
import pro.surpin.data.arrowflight.server.services.SplitBlockBloom;

class BloomRowFilterTest {

    private BufferAllocator allocator;

    @BeforeEach
    void open() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void close() {
        allocator.close();
    }

    private static RuntimeFilters.Bloom bloom(String column, long... keys) {
        return new RuntimeFilters.Bloom(column, List.of(BloomTestSupport.bloom(8, keys)));
    }

    private static VectorSchemaRoot source(BufferAllocator allocator, long[] keys, Integer[] others) {
        BigIntVector key = new BigIntVector("L_ORDERKEY", allocator);
        IntVector other = new IntVector("qty", allocator);
        VarCharVector text = new VarCharVector("note", allocator);
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] == Long.MIN_VALUE) {
                key.setNull(i);
            } else {
                key.setSafe(i, keys[i]);
            }
            other.setSafe(i, others[i]);
            text.setSafe(i, ("row" + i).getBytes(StandardCharsets.UTF_8));
        }
        key.setValueCount(keys.length);
        other.setValueCount(keys.length);
        text.setValueCount(keys.length);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(key, other, text));
        root.setRowCount(keys.length);
        return root;
    }

    @Test
    void keepsTheRowsOfAcceptedKeysWithAllTheirColumns() {
        try (VectorSchemaRoot source = source(allocator, new long[] {1, 2, 3, 4, 5}, new Integer[] {10, 20, 30, 40, 50});
                BloomRowFilter filter = BloomRowFilter.create(allocator, source, List.of(bloom("l_orderkey", 2, 4)))) {
            int kept = filter.next();

            // a bloom filter may let a few extra keys through, but never drops an inserted one
            VectorSchemaRoot out = filter.root();
            assertEquals(kept, out.getRowCount());
            List<Long> keys = new java.util.ArrayList<>();
            for (int i = 0; i < kept; i++) {
                long key = ((BigIntVector) out.getVector("L_ORDERKEY")).get(i);
                keys.add(key);
                assertEquals((int) key * 10, ((IntVector) out.getVector("qty")).get(i));
                assertEquals("row" + (key - 1), new String(((VarCharVector) out.getVector("note")).get(i),
                        StandardCharsets.UTF_8));
            }
            org.junit.jupiter.api.Assertions.assertTrue(keys.contains(2L) && keys.contains(4L), keys.toString());
            org.junit.jupiter.api.Assertions.assertTrue(kept <= 5);
        }
    }

    @Test
    void dropsRowsWithANullKey() {
        try (VectorSchemaRoot source = source(allocator, new long[] {Long.MIN_VALUE, 7}, new Integer[] {1, 2});
                BloomRowFilter filter = BloomRowFilter.create(allocator, source, List.of(bloom("L_ORDERKEY", 7)))) {
            assertEquals(1, filter.next());
            assertEquals(7L, ((BigIntVector) filter.root().getVector("L_ORDERKEY")).get(0));
        }
    }

    @Test
    void anEmptyFilterPassesNothingAndTheStageStaysUsableForTheNextBatch() {
        try (VectorSchemaRoot source = source(allocator, new long[] {1, 2, 3}, new Integer[] {1, 2, 3});
                BloomRowFilter filter = BloomRowFilter.create(allocator, source, List.of(bloom("l_orderkey")))) {
            assertEquals(0, filter.next());
            assertEquals(0, filter.root().getRowCount());

            // the same source root now holds a different batch
            ((BigIntVector) source.getVector("L_ORDERKEY")).setSafe(0, 9);
            RuntimeFilters.Bloom nine = bloom("l_orderkey", 9);
            try (BloomRowFilter again = BloomRowFilter.create(allocator, source, List.of(nine))) {
                assertEquals(1, again.next());
            }
        }
    }

    @Test
    void everyFilterMustAcceptARow() {
        SplitBlockBloom pass = BloomTestSupport.bloom(8, 1, 2, 3);
        RuntimeFilters.Bloom onKey = new RuntimeFilters.Bloom("l_orderkey", List.of(pass));
        RuntimeFilters.Bloom onQty = new RuntimeFilters.Bloom("qty", List.of(BloomTestSupport.bloom(8, 20)));
        try (VectorSchemaRoot source = source(allocator, new long[] {1, 2, 3}, new Integer[] {10, 20, 30});
                BloomRowFilter filter = BloomRowFilter.create(allocator, source, List.of(onKey, onQty))) {
            int kept = filter.next();
            org.junit.jupiter.api.Assertions.assertTrue(kept >= 1);
            boolean found = false;
            for (int i = 0; i < kept; i++) {
                found |= ((IntVector) filter.root().getVector("qty")).get(i) == 20;
            }
            org.junit.jupiter.api.Assertions.assertTrue(found);
        }
    }

    @Test
    void aKeyAcceptedByAnyPartitionPasses() {
        RuntimeFilters.Bloom partitioned = new RuntimeFilters.Bloom("l_orderkey",
                List.of(BloomTestSupport.bloom(8, 1), BloomTestSupport.bloom(8, 3)));
        try (VectorSchemaRoot source = source(allocator, new long[] {1, 3}, new Integer[] {1, 3});
                BloomRowFilter filter = BloomRowFilter.create(allocator, source, List.of(partitioned))) {
            assertEquals(2, filter.next());
        }
    }

    @Test
    void ignoresFiltersOnMissingOrNonIntegerColumns() {
        try (VectorSchemaRoot source = source(allocator, new long[] {1}, new Integer[] {1})) {
            assertNull(BloomRowFilter.create(allocator, source,
                    List.of(bloom("missing", 1), bloom("note", 1))));
        }
    }
}
