package pro.surpin.data.arrowflight.client.spark.read;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds the bloom filter block a scan sends to the Flight server with each ticket, so the server drops rows whose
 * join key cannot match before they cross the network.
 *
 * <p>The filter is the split-block bloom filter StarRocks uses and the server reads: a directory of 32-byte
 * buckets, a 64-bit hash that picks the bucket and one bit in each of the bucket's eight 32-bit words. The block
 * layout is the server's {@code RuntimeFilters} "ARF2" block without IN-filters. The server only trusts a filter
 * whose sample answers it reproduces, so each filter carries witnesses.
 */
final class RuntimeBloom {

    /** Largest directory a filter can have, 2^17 buckets. */
    static final int MAX_LOG_BUCKETS = 17;
    /** Largest directory in bytes. */
    static final int MAX_BYTES = 32 << MAX_LOG_BUCKETS;
    private static final int BUCKET_BITS = 256;
    private static final int WITNESSES = 16;
    private static final int BLOCK_MAGIC = 0x41524632;
    /** Protobuf tag of the length-delimited ticket field 15 the server reads its filters from. */
    private static final byte TICKET_FIELD_TAG = (15 << 3) | 2;
    private static final int[] SALT = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
        0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};
    private static final long MIX = 0xde5fb9d2630458e9L;

    /**
     * One built filter.
     *
     * @param column the scan's output column
     * @param logBuckets log2 of the bucket count
     * @param directory the buckets
     * @param witnessKeys sample keys
     * @param accepted what the filter answers for each sample key
     */
    record Filter(String column, int logBuckets, byte[] directory, long[] witnessKeys, boolean[] accepted) {
    }

    private RuntimeBloom() {
    }

    private static long hash(long key) {
        return Math.unsignedMultiplyHigh(key, MIX) + key * MIX;
    }

    private static boolean mightContain(int logBuckets, ByteBuffer words, long key) {
        long hash = hash(key);
        int base = (int) (hash & ((1L << logBuckets) - 1)) * 32;
        int mask = (int) (hash >>> logBuckets);
        for (int i = 0; i < 8; i++) {
            if ((words.getInt(base + i * 4) & (1 << ((mask * SALT[i]) >>> 27))) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds a filter that accepts every given key.
     *
     * @param column the scan's output column
     * @param keys the join keys
     * @param bitsPerKey directory bits per key before rounding up to a power of two buckets; the size is capped
     * @return the filter
     */
    static Filter of(String column, long[] keys, int bitsPerKey) {
        long wanted = Math.max(1L, (long) keys.length * bitsPerKey / BUCKET_BITS);
        int logBuckets = Math.min(MAX_LOG_BUCKETS, Math.max(1, 64 - Long.numberOfLeadingZeros(wanted - 1)));
        byte[] directory = new byte[32 << logBuckets];
        ByteBuffer words = ByteBuffer.wrap(directory).order(ByteOrder.LITTLE_ENDIAN);
        for (long key : keys) {
            long hash = hash(key);
            int base = (int) (hash & ((1L << logBuckets) - 1)) * 32;
            int mask = (int) (hash >>> logBuckets);
            for (int i = 0; i < 8; i++) {
                int at = base + i * 4;
                words.putInt(at, words.getInt(at) | (1 << ((mask * SALT[i]) >>> 27)));
            }
        }
        int accepted = Math.min(WITNESSES, keys.length);
        long[] witnessKeys = new long[accepted + WITNESSES];
        boolean[] answers = new boolean[witnessKeys.length];
        long max = Long.MIN_VALUE;
        for (long key : keys) {
            max = Math.max(max, key);
        }
        for (int i = 0; i < witnessKeys.length; i++) {
            witnessKeys[i] = i < accepted ? keys[(int) ((long) i * keys.length / accepted)] : max + 1 + (i - accepted);
            answers[i] = mightContain(logBuckets, words, witnessKeys[i]);
        }
        return new Filter(column, logBuckets, directory, witnessKeys, answers);
    }

    /**
     * Serialises filters into a runtime filter block.
     *
     * @param filters the filters, at most the server's limit of 16
     * @return the block bytes
     */
    static byte[] encode(List<Filter> filters) {
        int size = 12;
        for (Filter f : filters) {
            size += 4 + f.column().getBytes(StandardCharsets.UTF_8).length + 4 + 4 + 8 + f.directory().length + 4
                    + f.witnessKeys().length * 9;
        }
        ByteBuffer block = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(BLOCK_MAGIC).putInt(0).putInt(filters.size());
        for (Filter f : filters) {
            byte[] name = f.column().getBytes(StandardCharsets.UTF_8);
            block.putInt(name.length).put(name).putInt(1).putInt(f.logBuckets()).putLong(f.directory().length)
                    .put(f.directory()).putInt(f.witnessKeys().length);
            for (int i = 0; i < f.witnessKeys().length; i++) {
                block.putLong(f.witnessKeys()[i]).put((byte) (f.accepted()[i] ? 1 : 0));
            }
        }
        return block.array();
    }

    /**
     * Adds a block to a ticket as the field the server reads runtime filters from. Protobuf messages concatenate,
     * so the server's own ticket bytes stay untouched.
     *
     * @param ticket the ticket the server issued
     * @param block a block made by {@link #encode}
     * @return the ticket carrying the block
     */
    static byte[] appendTo(byte[] ticket, byte[] block) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(ticket.length + block.length + 6);
        out.writeBytes(ticket);
        out.write(TICKET_FIELD_TAG);
        int length = block.length;
        while (length > 0x7f) {
            out.write((length & 0x7f) | 0x80);
            length >>>= 7;
        }
        out.write(length);
        out.writeBytes(block);
        return out.toByteArray();
    }
}
