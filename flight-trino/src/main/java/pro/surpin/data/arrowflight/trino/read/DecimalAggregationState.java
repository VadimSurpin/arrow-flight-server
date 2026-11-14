package pro.surpin.data.arrowflight.trino.read;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigInteger;

/** Encodes exact decimal accumulators for the pinned Trino 446 serializers. */
final class DecimalAggregationState {
    private static final BigInteger LOW_MASK = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    /** Prevents utility construction. */
    private DecimalAggregationState() { }

    /**
     * Serializes an unscaled sum and optional average count without narrowing its range.
     *
     * @param total exact unscaled sum
     * @param count non-null input count for an average
     * @param average whether the state is an average
     * @return native serialized state
     * @throws ArithmeticException if the overflow counter cannot be represented
     * @throws IllegalArgumentException if the average count is not positive
     */
    static Slice encode(BigInteger total, long count, boolean average) {
        if (average && count <= 0) {
            throw new IllegalArgumentException("Decimal average count must be positive");
        }
        long low = total.longValue();
        long high = total.shiftRight(64).longValue();
        BigInteger wrapped = BigInteger.valueOf(high).shiftLeft(64)
                .add(BigInteger.valueOf(low).and(LOW_MASK));
        long overflow = total.subtract(wrapped).shiftRight(128).longValueExact();
        Slice buffer = Slices.allocate(Long.BYTES * (average ? 4 : 3));
        buffer.setLong(0, low);
        buffer.setLong(Long.BYTES, high);
        int words = high == 0 ? 1 : 2;
        if (average) {
            buffer.setLong(Long.BYTES * words, count);
            buffer.setLong(Long.BYTES * (words + 1), overflow);
            if (overflow != 0 || count != 1) {
                words += 2;
            }
        } else if (overflow != 0) {
            buffer.setLong(Long.BYTES * 2, overflow);
            words = 3;
        }
        return buffer.slice(0, words * Long.BYTES);
    }
}
