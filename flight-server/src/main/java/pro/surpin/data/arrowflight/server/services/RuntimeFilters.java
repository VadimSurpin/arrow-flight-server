package pro.surpin.data.arrowflight.server.services;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything a Flight client sends with a scan to cut its rows: IN-filters ({@link RuntimeInFilters}) and bloom
 * filters ({@link SplitBlockBloom}).
 *
 * <p>The block is little-endian. "ARF1" holds IN-filters only. "ARF2" holds the IN-filters, then the bloom filter
 * count and per filter the column name, the partition count and per partition the log2 of its bucket count
 * (int32), the directory length (uint64) and the directory, then the witness count and per witness a key (int64)
 * and StarRocks' answer for it (uint8). A bloom filter whose witnesses this server does not reproduce is left
 * out, because a filter that disagrees with the client's own would drop rows the client needs.
 *
 * @param in the IN-filters
 * @param blooms the bloom filters that were checked against their witnesses
 */
public record RuntimeFilters(List<RuntimeInFilters.Filter> in, List<Bloom> blooms) {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeFilters.class);

    /** Block marker of a block that carries bloom filters, "ARF2". */
    static final int BLOOM_MAGIC = 0x41524632;
    /** Most partitions one bloom filter can have. */
    static final int MAX_PARTITIONS = 1024;
    /** Most directory bytes accepted across all bloom filters of one block. */
    static final long MAX_BLOOM_BYTES = 64L << 20;
    /** Most witnesses one bloom filter can carry. */
    static final int MAX_WITNESSES = 256;

    /** No filters. */
    public static final RuntimeFilters NONE = new RuntimeFilters(List.of(), List.of());

    /**
     * A join-built bloom filter on one column.
     *
     * @param column name of the scan's output column
     * @param partitions the filters of each hash partition of the join, one for an unpartitioned join; a key
     *        passes when any partition accepts it
     */
    public record Bloom(String column, List<SplitBlockBloom> partitions) {
    }

    /** @return whether there is nothing to apply */
    public boolean isEmpty() {
        return in.isEmpty() && blooms.isEmpty();
    }

    /**
     * Parses a block, rejecting anything malformed or larger than the limits.
     *
     * @param bytes block bytes
     * @return the filters, without the bloom filters that failed their witnesses
     * @throws IllegalArgumentException when the block is not valid
     */
    public static RuntimeFilters decode(byte[] bytes) {
        ByteBuffer block = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        try {
            int magic = block.getInt();
            if (magic == RuntimeInFilters.MAGIC) {
                return new RuntimeFilters(RuntimeInFilters.decode(bytes), List.of());
            }
            if (magic != BLOOM_MAGIC) {
                throw new IllegalArgumentException("Unsupported runtime filter block");
            }
            List<RuntimeInFilters.Filter> in = RuntimeInFilters.readFilters(block);
            List<Bloom> blooms = readBlooms(block);
            if (block.hasRemaining()) {
                throw new IllegalArgumentException("Unexpected runtime filter data");
            }
            return new RuntimeFilters(in, blooms);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("Truncated runtime filter block", e);
        }
    }

    private static List<Bloom> readBlooms(ByteBuffer block) {
        int count = block.getInt();
        if (count < 0 || count > RuntimeInFilters.MAX_FILTERS) {
            throw new IllegalArgumentException("Invalid bloom filter count");
        }
        List<Bloom> blooms = new ArrayList<>(count);
        long totalBytes = 0;
        for (int i = 0; i < count; i++) {
            int nameLength = block.getInt();
            if (nameLength < 0 || nameLength > RuntimeInFilters.MAX_NAME_BYTES || nameLength > block.remaining()) {
                throw new IllegalArgumentException("Invalid bloom filter column name");
            }
            byte[] name = new byte[nameLength];
            block.get(name);
            int partitionCount = block.getInt();
            if (partitionCount < 1 || partitionCount > MAX_PARTITIONS) {
                throw new IllegalArgumentException("Invalid bloom filter partition count");
            }
            List<SplitBlockBloom> partitions = new ArrayList<>(partitionCount);
            for (int p = 0; p < partitionCount; p++) {
                int logBuckets = block.getInt();
                long length = block.getLong();
                totalBytes += length;
                if (logBuckets < SplitBlockBloom.MIN_LOG_BUCKETS || logBuckets > SplitBlockBloom.MAX_LOG_BUCKETS
                        || length != (long) SplitBlockBloom.BUCKET_BYTES << logBuckets
                        || totalBytes > MAX_BLOOM_BYTES || length > block.remaining()) {
                    throw new IllegalArgumentException("Invalid bloom filter directory");
                }
                partitions.add(new SplitBlockBloom(logBuckets, block.array(), block.arrayOffset() + block.position()));
                block.position(block.position() + (int) length);
            }
            int witnessCount = block.getInt();
            if (witnessCount < 0 || witnessCount > MAX_WITNESSES
                    || (long) witnessCount * (Long.BYTES + 1) > block.remaining()) {
                throw new IllegalArgumentException("Invalid bloom filter witness count");
            }
            long[] keys = new long[witnessCount];
            boolean[] accepted = new boolean[witnessCount];
            boolean anyAccepted = false;
            for (int w = 0; w < witnessCount; w++) {
                keys[w] = block.getLong();
                accepted[w] = block.get() != 0;
                anyAccepted |= accepted[w];
            }
            String column = new String(name, StandardCharsets.UTF_8);
            if (anyAccepted && SplitBlockBloom.agreesWith(partitions.toArray(new SplitBlockBloom[0]), keys, accepted)) {
                blooms.add(new Bloom(column, List.copyOf(partitions)));
            } else {
                LOGGER.warn("Ignoring the bloom filter on {}: its answers for {} sample keys differ from the client's",
                        column, witnessCount);
            }
        }
        return blooms;
    }
}
