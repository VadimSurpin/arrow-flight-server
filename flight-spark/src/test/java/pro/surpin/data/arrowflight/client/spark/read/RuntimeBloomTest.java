package pro.surpin.data.arrowflight.client.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/**
 * Tests the bloom filter block a scan sends to the Flight server. The reader below is an independent copy of the
 * server's {@code SplitBlockBloom}/{@code RuntimeFilters} decoding, so the two cannot drift apart unnoticed.
 */
class RuntimeBloomTest {

    private static final int[] SALT = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
        0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};
    private static final long MIX = 0xde5fb9d2630458e9L;

    private record Decoded(String column, int logBuckets, byte[] directory, long[] witnessKeys, boolean[] accepted) {
        boolean mightContain(long key) {
            long hash = Math.unsignedMultiplyHigh(key, MIX) + key * MIX;
            int bucket = (int) (hash & ((1L << logBuckets) - 1));
            int mask = (int) (hash >>> logBuckets);
            ByteBuffer words = ByteBuffer.wrap(directory).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < 8; i++) {
                if ((words.getInt(bucket * 32 + i * 4) & (1 << ((mask * SALT[i]) >>> 27))) == 0) {
                    return false;
                }
            }
            return true;
        }
    }

    private static Decoded decode(byte[] bytes) {
        ByteBuffer block = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x41524632, block.getInt(), "ARF2 marker");
        assertEquals(0, block.getInt(), "no IN filters");
        assertEquals(1, block.getInt(), "one bloom filter");
        byte[] name = new byte[block.getInt()];
        block.get(name);
        assertEquals(1, block.getInt(), "one partition");
        int logBuckets = block.getInt();
        long length = block.getLong();
        assertEquals(32L << logBuckets, length);
        byte[] directory = new byte[(int) length];
        block.get(directory);
        int witnesses = block.getInt();
        long[] keys = new long[witnesses];
        boolean[] accepted = new boolean[witnesses];
        for (int i = 0; i < witnesses; i++) {
            keys[i] = block.getLong();
            accepted[i] = block.get() != 0;
        }
        assertFalse(block.hasRemaining());
        return new Decoded(new String(name, StandardCharsets.UTF_8), logBuckets, directory, keys, accepted);
    }

    @Test
    void neverRejectsAnInsertedKeyAndRarelyAcceptsOthers() {
        long[] keys = new Random(7).longs(200_000, 0, 1L << 40).toArray();
        Decoded bloom = decode(RuntimeBloom.encode(List.of(RuntimeBloom.of("l_partkey", keys, 10))));

        assertEquals("l_partkey", bloom.column());
        for (long key : keys) {
            assertTrue(bloom.mightContain(key));
        }
        java.util.Set<Long> inserted = java.util.Arrays.stream(keys).boxed().collect(java.util.stream.Collectors.toSet());
        long falsePositives = new Random(11).longs(200_000, 1L << 41, 1L << 42)
                .filter(k -> !inserted.contains(k) && bloom.mightContain(k)).count();
        assertTrue(falsePositives < 200_000 * 0.03, "false positive rate too high: " + falsePositives);
    }

    @Test
    void witnessesAgreeWithTheFilterAndIncludeAcceptedKeys() {
        long[] keys = LongStream.range(0, 5_000).map(i -> i * 3).toArray();
        Decoded bloom = decode(RuntimeBloom.encode(List.of(RuntimeBloom.of("k", keys, 10))));

        boolean anyAccepted = false;
        for (int i = 0; i < bloom.witnessKeys().length; i++) {
            assertEquals(bloom.mightContain(bloom.witnessKeys()[i]), bloom.accepted()[i]);
            anyAccepted |= bloom.accepted()[i];
        }
        assertTrue(anyAccepted, "the server drops a filter without an accepted witness");
        assertTrue(bloom.witnessKeys().length <= 256);
    }

    @Test
    void sizeIsCappedForHugeKeySets() {
        long[] keys = LongStream.range(0, 3_000_000).toArray();
        Decoded bloom = decode(RuntimeBloom.encode(List.of(RuntimeBloom.of("k", keys, 10))));

        assertTrue(bloom.directory().length <= RuntimeBloom.MAX_BYTES);
        for (int i = 0; i < 1_000; i++) {
            assertTrue(bloom.mightContain(keys[i * 2_999]));
        }
    }

    @Test
    void appendedBlockSurvivesAsAnUnknownTicketField() throws Exception {
        byte[] ticket = Any.newBuilder().setTypeUrl("type.googleapis.com/x").setValue(ByteString.copyFromUtf8("h"))
                .build().toByteArray();
        byte[] block = RuntimeBloom.encode(List.of(RuntimeBloom.of("k", new long[] {1, 2, 3}, 10)));

        Any parsed = Any.parseFrom(RuntimeBloom.appendTo(ticket, block));

        assertEquals("h", parsed.getValue().toStringUtf8());
        List<ByteString> blocks = parsed.getUnknownFields().getField(15).getLengthDelimitedList();
        assertEquals(1, blocks.size());
        assertEquals(ByteString.copyFrom(block), blocks.get(0));
    }
}
