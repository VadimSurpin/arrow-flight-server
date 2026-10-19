package pro.surpin.data.arrowflight.server.model;

import java.nio.ByteBuffer;

/** Describes Parquet input statistics without execution endpoints. */
public record QueryStatistics(long totalBytes, long totalRecords) {
    /** Number of bytes in the Flight action representation. */
    public static final int ENCODED_SIZE = Long.BYTES * 2;

    /**
     * Encodes statistics for the Flight action response.
     *
     * @return fixed-width binary representation
     */
    public byte[] encode() {
        return ByteBuffer.allocate(ENCODED_SIZE)
                .putLong(totalBytes)
                .putLong(totalRecords)
                .array();
    }
}
