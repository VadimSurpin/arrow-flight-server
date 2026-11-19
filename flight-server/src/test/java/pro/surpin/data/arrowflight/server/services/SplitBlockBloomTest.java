package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.Test;

class SplitBlockBloomTest {

    @Test
    void hashMatchesTheReferenceForEdgeAndRandomKeys() {
        long[] keys = {0, 1, -1, 42, -42, Long.MAX_VALUE, Long.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE};
        for (long key : keys) {
            assertEquals(BloomTestSupport.referenceHash(key), SplitBlockBloom.hash(key), "key " + key);
        }
        Random random = new Random(7);
        for (int i = 0; i < 10_000; i++) {
            long key = random.nextLong();
            assertEquals(BloomTestSupport.referenceHash(key), SplitBlockBloom.hash(key), "key " + key);
        }
    }

    @Test
    void neverRejectsAnInsertedKey() {
        Random random = new Random(11);
        long[] keys = random.longs(5_000).toArray();
        SplitBlockBloom bloom = BloomTestSupport.bloom(10, keys);
        for (long key : keys) {
            assertTrue(bloom.mightContain(key), "key " + key);
        }
    }

    @Test
    void sign_extendedNegativeIntegersAreInsertedAndFound() {
        long[] keys = {-1, -2, -100_000, Integer.MIN_VALUE};
        SplitBlockBloom bloom = BloomTestSupport.bloom(6, keys);
        for (long key : keys) {
            assertTrue(bloom.mightContain(key));
        }
    }

    @Test
    void rejectsMostKeysThatWereNotInserted() {
        long[] inserted = new Random(3).longs(10_000).toArray();
        // sized the way StarRocks sizes it: the next power of two of the key count, in bytes
        SplitBlockBloom bloom = BloomTestSupport.bloom(9, inserted);
        Random random = new Random(5);
        int falsePositives = 0;
        int probes = 100_000;
        for (int i = 0; i < probes; i++) {
            if (bloom.mightContain(random.nextLong())) {
                falsePositives++;
            }
        }
        assertTrue(falsePositives < probes / 20, "false positive rate too high: " + falsePositives);
    }

    @Test
    void anEmptyFilterRejectsEverything() {
        SplitBlockBloom bloom = BloomTestSupport.bloom(3);
        for (long key = -50; key < 50; key++) {
            assertFalse(bloom.mightContain(key));
        }
    }

    @Test
    void agreementRequiresEveryAnswerToMatchAcrossPartitions() {
        SplitBlockBloom first = BloomTestSupport.bloom(4, 10);
        SplitBlockBloom second = BloomTestSupport.bloom(4, 20);
        SplitBlockBloom[] both = {first, second};
        assertTrue(SplitBlockBloom.agreesWith(both, new long[] {10, 20}, new boolean[] {true, true}));
        assertFalse(SplitBlockBloom.agreesWith(both, new long[] {10, 20}, new boolean[] {true, false}));
        assertFalse(SplitBlockBloom.agreesWith(both, new long[] {10}, new boolean[] {false}));
    }

    @Test
    void rejectsDirectoriesThatDoNotFit() {
        byte[] directory = new byte[SplitBlockBloom.BUCKET_BYTES * 4];
        assertThrows(IllegalArgumentException.class, () -> new SplitBlockBloom(3, directory, 0));
        assertThrows(IllegalArgumentException.class, () -> new SplitBlockBloom(2, directory, 1));
        assertThrows(IllegalArgumentException.class, () -> new SplitBlockBloom(0, directory, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SplitBlockBloom(SplitBlockBloom.MAX_LOG_BUCKETS + 1, directory, 0));
    }
}
