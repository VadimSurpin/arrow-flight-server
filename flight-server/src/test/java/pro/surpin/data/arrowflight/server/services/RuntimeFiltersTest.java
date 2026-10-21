package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RuntimeFiltersTest {

    private static final long[] INSERTED = {5, 17, -3, 1_000_000_007L};
    private static final long[] PROBES = {5, 17, -3, 1_000_000_007L, 6, 18, 99, -4, 12345};

    @Test
    void readsAnInFilterOnlyBlock() {
        byte[] block = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("a", new long[] {1, 2})));

        RuntimeFilters filters = RuntimeFilters.decode(block);

        assertEquals(1, filters.in().size());
        assertTrue(filters.blooms().isEmpty());
    }

    @Test
    void readsBloomFiltersAndInFiltersTogether() {
        byte[] block = BloomTestSupport.block(
                List.of(new RuntimeInFilters.Filter("a", new long[] {1, 2})),
                List.of(BloomTestSupport.sent("l_orderkey", 4, INSERTED, PROBES)));

        RuntimeFilters filters = RuntimeFilters.decode(block);

        assertEquals(1, filters.in().size());
        assertEquals(1, filters.blooms().size());
        RuntimeFilters.Bloom bloom = filters.blooms().get(0);
        assertEquals("l_orderkey", bloom.column());
        assertEquals(1, bloom.partitions().size());
        for (long key : INSERTED) {
            assertTrue(bloom.partitions().get(0).mightContain(key));
        }
    }

    @Test
    void readsEveryPartitionOfAPartitionedFilter() {
        byte[] first = BloomTestSupport.directory(3, 1, 2);
        byte[] second = BloomTestSupport.directory(3, 3, 4);
        long[] probes = {1, 2, 3, 4, 5};
        boolean[] answers = new boolean[probes.length];
        for (int i = 0; i < probes.length; i++) {
            answers[i] = new SplitBlockBloom(3, first, 0).mightContain(probes[i])
                    || new SplitBlockBloom(3, second, 0).mightContain(probes[i]);
        }
        BloomTestSupport.Sent sent = new BloomTestSupport.Sent("k", List.of(first, second), List.of(3, 3),
                probes, answers);

        RuntimeFilters filters = RuntimeFilters.decode(BloomTestSupport.block(List.of(), List.of(sent)));

        assertEquals(2, filters.blooms().get(0).partitions().size());
    }

    @Test
    void leavesOutABloomFilterWhoseWitnessesThisServerDoesNotReproduce() {
        BloomTestSupport.Sent good = BloomTestSupport.sent("good", 4, INSERTED, PROBES);
        boolean[] flipped = good.witnessAnswers().clone();
        flipped[PROBES.length - 1] = !flipped[PROBES.length - 1];
        BloomTestSupport.Sent bad = new BloomTestSupport.Sent("bad", good.directories(), good.logBuckets(),
                good.witnessKeys(), flipped);

        RuntimeFilters filters = RuntimeFilters.decode(BloomTestSupport.block(List.of(), List.of(bad, good)));

        assertEquals(1, filters.blooms().size());
        assertEquals("good", filters.blooms().get(0).column());
    }

    @Test
    void leavesOutABloomFilterWithoutAnAcceptedWitness() {
        BloomTestSupport.Sent none = BloomTestSupport.sent("k", 4, INSERTED, new long[] {6, 18, 99});

        RuntimeFilters filters = RuntimeFilters.decode(BloomTestSupport.block(List.of(), List.of(none)));

        assertTrue(filters.blooms().isEmpty());
    }

    @Test
    void rejectsMalformedBlocks() {
        byte[] good = BloomTestSupport.block(List.of(),
                List.of(BloomTestSupport.sent("k", 4, INSERTED, PROBES)));

        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(Arrays.copyOf(good, good.length - 2)));
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(Arrays.copyOf(good, good.length + 1)));
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(new byte[] {1, 2}));
    }

    @Test
    void rejectsADirectoryLengthThatDisagreesWithItsBucketCount() {
        ByteBuffer block = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(RuntimeFilters.BLOOM_MAGIC).putInt(0).putInt(1);
        block.putInt(1).put((byte) 'k').putInt(1).putInt(3).putLong(64);
        byte[] bytes = block.array();

        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(bytes));
    }

    @Test
    void rejectsMoreDirectoryBytesThanTheLimit() {
        int log = 22; // 128 MiB per directory
        ByteBuffer block = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(RuntimeFilters.BLOOM_MAGIC).putInt(0).putInt(1);
        block.putInt(1).put((byte) 'k').putInt(1).putInt(log).putLong((long) SplitBlockBloom.BUCKET_BYTES << log);

        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(block.array()));
    }

    @Test
    void noFiltersIsEmpty() {
        assertTrue(RuntimeFilters.NONE.isEmpty());
        assertFalse(RuntimeFilters.decode(RuntimeInFilters.encode(
                List.of(new RuntimeInFilters.Filter("a", new long[] {1})))).isEmpty());
    }
}
