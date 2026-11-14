package pro.surpin.data.arrowflight.trino.read;

import io.trino.operator.aggregation.DecimalAverageAggregation;
import io.trino.operator.aggregation.DecimalSumAggregation;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateSerializer;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies decimal wire compatibility against the actual pinned Trino accumulators. */
@Tag("unit")
class TrinoPartialAggregationDecimalStateTest {
    /** Checks the native AVG overflow carry retains the decimal scale for both signs. */
    @Test
    void preservesOverflowScaleInNativeAverage() {
        BigInteger bound = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
        for (int scale = 0; scale <= 38; scale++) {
            var type = DecimalType.createDecimalType(38, scale);
            for (BigInteger value : List.of(bound, bound.negate())) {
                var state = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();
                var input = type.createBlockBuilder(null, 1);
                type.writeObject(input, Int128.valueOf(value));
                var block = (Int128ArrayBlock) input.build();
                for (int row = 0; row < 3; row++) {
                    DecimalAverageAggregation.inputLongDecimal(state, block, 0);
                }
                assertEquals(value, DecimalAverageAggregation.average(state, type).toBigInteger(), type.toString());
            }
        }
    }

    /** Checks every native encoding length and signed overflow against real Trino states. */
    @Test
    void matchesNativeSerializersThroughOverflowAndCancellation() {
        var sum = new LongDecimalWithOverflowStateFactory().createSingleState();
        var average = new LongDecimalWithOverflowAndLongStateFactory().createSingleState();
        var sumSerializer = new LongDecimalWithOverflowStateSerializer();
        var averageSerializer = new LongDecimalWithOverflowAndLongStateSerializer();
        var type = DecimalType.createDecimalType(38, 0);
        BigInteger bound = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
        List<BigInteger> inputs = new ArrayList<>(List.of(BigInteger.ZERO, BigInteger.ONE,
                BigInteger.valueOf(-2), BigInteger.ONE.shiftLeft(63), BigInteger.ONE.shiftLeft(63).negate()));
        inputs.addAll(java.util.Collections.nCopies(5, bound));
        inputs.addAll(java.util.Collections.nCopies(5, bound.negate()));
        BigInteger total = BigInteger.ZERO;
        long count = 0;
        for (BigInteger value : inputs) {
            var input = type.createBlockBuilder(null, 1);
            type.writeObject(input, Int128.valueOf(value));
            var block = (Int128ArrayBlock) input.build();
            DecimalSumAggregation.inputLongDecimal(sum, block, 0);
            DecimalAverageAggregation.inputLongDecimal(average, block, 0);
            total = total.add(value);
            count++;
            var serializedSum = VARBINARY.createBlockBuilder(null, 1);
            sumSerializer.serialize(sum, serializedSum);
            var serializedAverage = VARBINARY.createBlockBuilder(null, 1);
            averageSerializer.serialize(average, serializedAverage);
            assertArrayEquals(VARBINARY.getSlice(serializedSum.build(), 0).getBytes(),
                    DecimalAggregationState.encode(total, count, false).getBytes());
            assertArrayEquals(VARBINARY.getSlice(serializedAverage.build(), 0).getBytes(),
                    DecimalAggregationState.encode(total, count, true).getBytes());
        }
    }
}
