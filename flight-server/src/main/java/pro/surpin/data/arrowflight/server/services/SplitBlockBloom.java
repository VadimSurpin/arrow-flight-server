package pro.surpin.data.arrowflight.server.services;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A reader for StarRocks' split-block bloom filter ({@code SimdBlockFilter}), the filter its hash joins build, so a
 * scan can drop rows the join would discard.
 *
 * <p>The filter is a directory of 32-byte buckets. A key is hashed to 64 bits; the low bits pick the bucket and
 * the rest of the bits pick one bit in each of the bucket's eight 32-bit words. The key matches when all eight
 * bits are set. Both the hash and the layout must be reproduced exactly, so {@link #agreesWith} lets a caller
 * check an instance against answers StarRocks gave for sample keys.
 */
public final class SplitBlockBloom {

    /** Smallest filter StarRocks builds: two buckets. */
    static final int MIN_LOG_BUCKETS = 1;
    /** Largest filter accepted: 2^26 buckets, 2 GiB. */
    static final int MAX_LOG_BUCKETS = 26;
    /** Bytes in one bucket. */
    static final int BUCKET_BYTES = 32;

    private static final int WORDS_PER_BUCKET = 8;
    private static final int[] SALT = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
        0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};
    private static final long MIX = 0xde5fb9d2630458e9L;
    private static final VarHandle WORDS = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private final int logBuckets;
    private final byte[] directory;
    private final int offset;

    /**
     * Wraps a directory without copying it.
     *
     * @param logBuckets log2 of the bucket count
     * @param directory array holding the buckets
     * @param offset where the directory starts in the array
     * @throws IllegalArgumentException when the size does not fit the bucket count
     */
    public SplitBlockBloom(int logBuckets, byte[] directory, int offset) {
        if (logBuckets < MIN_LOG_BUCKETS || logBuckets > MAX_LOG_BUCKETS) {
            throw new IllegalArgumentException("Invalid bloom filter size: 2^" + logBuckets + " buckets");
        }
        long bytes = (long) BUCKET_BYTES << logBuckets;
        if (offset < 0 || offset + bytes > directory.length) {
            throw new IllegalArgumentException("Truncated bloom filter directory");
        }
        this.logBuckets = logBuckets;
        this.directory = directory;
        this.offset = offset;
    }

    /**
     * Hashes a key the way StarRocks does for integer columns: the value, sign-extended to 64 bits, goes through
     * phmap's 64-bit mix, the sum of the high and low halves of a 128-bit multiplication.
     *
     * @param key the key value
     * @return the 64-bit hash
     */
    public static long hash(long key) {
        return Math.unsignedMultiplyHigh(key, MIX) + key * MIX;
    }

    /**
     * Tests a key.
     *
     * @param key the key value
     * @return false when the key was certainly not inserted, true when it may have been
     */
    public boolean mightContain(long key) {
        long hash = hash(key);
        int bucket = (int) (hash & ((1L << logBuckets) - 1));
        int mask = (int) (hash >>> logBuckets);
        int base = offset + bucket * BUCKET_BYTES;
        for (int i = 0; i < WORDS_PER_BUCKET; i++) {
            int bit = 1 << ((mask * SALT[i]) >>> 27);
            int word = (int) WORDS.get(directory, base + i * Integer.BYTES);
            if ((word & bit) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a filter, made of these partitions and accepting a key that any of them accepts, answers like
     * StarRocks' filter did for sample keys.
     *
     * @param partitions the partition filters, one for a join that was not partitioned
     * @param keys sample keys
     * @param accepted what StarRocks answered for each key
     * @return true when every answer matches
     */
    public static boolean agreesWith(SplitBlockBloom[] partitions, long[] keys, boolean[] accepted) {
        for (int i = 0; i < keys.length; i++) {
            boolean answer = false;
            for (SplitBlockBloom partition : partitions) {
                answer = answer || partition.mightContain(keys[i]);
            }
            if (answer != accepted[i]) {
                return false;
            }
        }
        return true;
    }
}
