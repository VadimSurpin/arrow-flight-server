package pro.surpin.data.arrowflight.client.query;

import java.io.Serializable;
import java.nio.ByteBuffer;

/** Carries side-effect-free server estimates for one query. */
public record QueryStatistics(long totalBytes, long totalRecords) implements Serializable {
    /** Flight action used to request query statistics without allocating endpoints. */
    public static final String ACTION_TYPE = "arrow-flight-query-statistics";

    /** Number of bytes in the binary action response. */
    public static final int ENCODED_SIZE = Long.BYTES * 2;

    /**
     * Encodes statistics for a Flight action result.
     *
     * @return fixed-width binary representation
     */
    public byte[] encode() {
        return ByteBuffer.allocate(ENCODED_SIZE)
                .putLong(totalBytes)
                .putLong(totalRecords)
                .array();
    }

    /**
     * Decodes statistics from a Flight action result.
     *
     * @param bytes fixed-width binary representation
     * @return decoded statistics
     * @throws IllegalArgumentException if the representation has an invalid size
     */
    public static QueryStatistics decode(byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_SIZE) {
            throw new IllegalArgumentException("Invalid query statistics response size");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new QueryStatistics(buffer.getLong(), buffer.getLong());
    }
}
