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

    /** A block built by the Spark connector's {@code RuntimeBloom} for the keys 3, 10, 17, ..., 696 (7i + 3). */
    private static final String SPARK_BLOCK = "324652410000000001000000090000006c5f706172746b657901000000020000008000000000000000703c8ec332cbdd77a8"
            + "3255a7fcc18f3f9bd9edbfedab6d6d52d34d4a55d501da1c87e3384e3ebbcff7aa5a557ce10f7fd9cdfc9f5b5bc94bbb494a"
            + "76bf2a00d5c771388ef9f966fe556daa4ef8e30fffbbbab7b9dad252d249c96e2bfb07e0aa791c86e327b7dddd6ed5bdaaf8"
            + "c31ffe667b5f77f5a6b6b6ed2e2d695501ee5f200000000300000000000000012d0000000000000001570000000000000001"
            + "810000000000000001b20000000000000001dc00000000000000010601000000000000013001000000000000016101000000"
            + "000000018b0100000000000001b50100000000000001df01000000000000011002000000000000013a020000000000000164"
            + "02000000000000018e0200000000000001b90200000000000000ba0200000000000000bb0200000000000000bc0200000000"
            + "000000bd0200000000000000be0200000000000000bf0200000000000000c00200000000000000c10200000000000000c202"
            + "00000000000000c30200000000000000c40200000000000000c50200000000000000c60200000000000000c7020000000000"
            + "0000c80200000000000000";

    @Test
    void acceptsTheBloomBlockTheSparkConnectorBuilds() {
        RuntimeFilters filters = RuntimeFilters.decode(java.util.HexFormat.of().parseHex(SPARK_BLOCK));

        assertEquals(1, filters.blooms().size(), "the witnesses must be reproduced by the server's reader");
        RuntimeFilters.Bloom bloom = filters.blooms().get(0);
        assertEquals("l_partkey", bloom.column());
        for (int i = 0; i < 100; i++) {
            assertTrue(bloom.partitions().get(0).mightContain(i * 7L + 3));
        }
    }
}
