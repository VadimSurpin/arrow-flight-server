package pro.surpin.data.arrowflight.server.services;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A second, independent implementation of StarRocks' split-block bloom filter, written from its C++ ({@code
 * SimdBlockFilter::insert_hash} with the scalar {@code make_mask}, {@code phmap_mix<8>}) with BigInteger instead of
 * the production code's 64-bit tricks, and an encoder for the block a StarRocks BE sends.
 */
public final class BloomTestSupport {

    private static final long[] SALT = {0x47b6137bL, 0x44974d91L, 0x8824ad5bL, 0xa2b7289dL,
        0x705495c7L, 0x2df1424bL, 0x9efc4947L, 0x5c6bfb31L};
    private static final BigInteger MIX = new BigInteger("de5fb9d2630458e9", 16);
    private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);

    private BloomTestSupport() {
    }

    /** What StarRocks would hash for a key: std::hash of the integer, which sign-extends, then phmap_mix. */
    public static long referenceHash(long key) {
        BigInteger a = BigInteger.valueOf(key).mod(TWO_64);
        BigInteger product = a.multiply(MIX);
        BigInteger low = product.mod(TWO_64);
        BigInteger high = product.shiftRight(64);
        return high.add(low).mod(TWO_64).longValue();
    }

    /** A filter directory of 2^logBuckets buckets holding the keys. */
    public static byte[] directory(int logBuckets, long... keys) {
        byte[] directory = new byte[SplitBlockBloom.BUCKET_BYTES << logBuckets];
        ByteBuffer words = ByteBuffer.wrap(directory).order(ByteOrder.LITTLE_ENDIAN);
        for (long key : keys) {
            long hash = referenceHash(key);
            int bucket = (int) (hash & ((1L << logBuckets) - 1));
            long mask = (hash >>> logBuckets) & 0xffffffffL;
            for (int i = 0; i < 8; i++) {
                long shifted = ((mask * SALT[i]) & 0xffffffffL) >>> 27;
                int at = bucket * SplitBlockBloom.BUCKET_BYTES + i * Integer.BYTES;
                words.putInt(at, words.getInt(at) | (1 << shifted));
            }
        }
        return directory;
    }

    public static SplitBlockBloom bloom(int logBuckets, long... keys) {
        return new SplitBlockBloom(logBuckets, directory(logBuckets, keys), 0);
    }

    /** A bloom filter as the BE sends it. */
    public record Sent(String column, List<byte[]> directories, List<Integer> logBuckets, long[] witnessKeys,
            boolean[] witnessAnswers) {
    }

    /** Witnesses answered by a correct filter over the given partitions, from keys 0..count. */
    public static Sent sent(String column, int logBuckets, long[] inserted, long[] probes) {
        byte[] directory = directory(logBuckets, inserted);
        SplitBlockBloom filter = new SplitBlockBloom(logBuckets, directory, 0);
        boolean[] answers = new boolean[probes.length];
        for (int i = 0; i < probes.length; i++) {
            answers[i] = filter.mightContain(probes[i]);
        }
        return new Sent(column, List.of(directory), List.of(logBuckets), probes, answers);
    }

    /** An "ARF2" block: the IN-filters, then the bloom filters. */
    public static byte[] block(List<RuntimeInFilters.Filter> in, List<Sent> blooms) {
        byte[] inBlock = RuntimeInFilters.encode(in);
        int size = inBlock.length - Integer.BYTES + Integer.BYTES + Integer.BYTES;
        for (Sent bloom : blooms) {
            size += Integer.BYTES + bloom.column().getBytes(StandardCharsets.UTF_8).length + Integer.BYTES;
            for (byte[] directory : bloom.directories()) {
                size += Integer.BYTES + Long.BYTES + directory.length;
            }
            size += Integer.BYTES + bloom.witnessKeys().length * (Long.BYTES + 1);
        }
        ByteBuffer block = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(RuntimeFilters.BLOOM_MAGIC);
        block.put(inBlock, Integer.BYTES, inBlock.length - Integer.BYTES);
        block.putInt(blooms.size());
        for (Sent bloom : blooms) {
            byte[] name = bloom.column().getBytes(StandardCharsets.UTF_8);
            block.putInt(name.length).put(name).putInt(bloom.directories().size());
            for (int p = 0; p < bloom.directories().size(); p++) {
                block.putInt(bloom.logBuckets().get(p)).putLong(bloom.directories().get(p).length)
                        .put(bloom.directories().get(p));
            }
            block.putInt(bloom.witnessKeys().length);
            for (int w = 0; w < bloom.witnessKeys().length; w++) {
                block.putLong(bloom.witnessKeys()[w]).put((byte) (bloom.witnessAnswers()[w] ? 1 : 0));
            }
        }
        return block.array();
    }
}
