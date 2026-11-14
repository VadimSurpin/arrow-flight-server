package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.metadata.AggregateSpec;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks native REAL and exact decimal state construction and validation. */
@Tag("unit")
class ExactAggregationStatesTest {
    /** Checks REAL averages put count before sum, including the empty state. */
    @Test
    void assemblesRealAverageInNativeOrder() {
        var type = RowType.anonymous(List.of(BIGINT, DOUBLE));
        var specification = new AggregateSpec(AggregateSpec.Function.AVG,
                Optional.of(new FlightColumnHandle("v", "v", REAL)), new FlightColumnHandle("state", "state", type));
        try (var allocator = new RootAllocator(); var sum = new Float8Vector("state_sum", allocator);
             var count = new BigIntVector("state_count", allocator)) {
            sum.allocateNew(1);
            count.allocateNew(1);
            sum.set(0, 30);
            count.set(0, 2);
            try (var root = VectorSchemaRoot.of(sum, count)) {
                root.setRowCount(1);
                var row = type.getObject(FlightAggregationStates.read(specification, root), 0);
                assertEquals(2, BIGINT.getLong(row.getRawFieldBlock(0), row.getRawIndex()));
                assertEquals(30, DOUBLE.getDouble(row.getRawFieldBlock(1), row.getRawIndex()));
            }
        }
        var empty = type.getObject(FlightAggregationStates.empty(specification), 0);
        assertEquals(0, BIGINT.getLong(empty.getRawFieldBlock(0), empty.getRawIndex()));
        assertEquals(0, DOUBLE.getDouble(empty.getRawFieldBlock(1), empty.getRawIndex()));
    }

    /** Checks decimal states retain overflow and reject inconsistent or noninteger input. */
    @Test
    void assemblesAndValidatesDecimalStates() {
        var input = new FlightColumnHandle("v", "v", DecimalType.createDecimalType(38, 0));
        try (var allocator = new RootAllocator(); var sum = new VarCharVector("state_sum", allocator);
             var count = new BigIntVector("state_count", allocator)) {
            sum.allocateNew();
            count.allocateNew(2);
            BigInteger total = BigInteger.ONE.shiftLeft(128).add(BigInteger.valueOf(5));
            sum.setSafe(0, total.toString().getBytes(StandardCharsets.UTF_8));
            sum.setNull(1);
            count.set(0, 4);
            count.set(1, 0);
            try (var root = VectorSchemaRoot.of(sum, count)) {
                root.setRowCount(2);
                for (var function : List.of(AggregateSpec.Function.SUM, AggregateSpec.Function.AVG)) {
                    var spec = new AggregateSpec(function, Optional.of(input),
                            new FlightColumnHandle("state", "state", VARBINARY));
                    var block = FlightAggregationStates.read(spec, root);
                    assertEquals(DecimalAggregationState.encode(total, 4, function == AggregateSpec.Function.AVG),
                            VARBINARY.getSlice(block, 0));
                    assertTrue(block.isNull(1));
                    assertTrue(FlightAggregationStates.empty(spec).isNull(0));
                }
                var average = new AggregateSpec(AggregateSpec.Function.AVG, Optional.of(input),
                        new FlightColumnHandle("state", "state", VARBINARY));
                count.set(1, 1);
                assertThrows(IllegalStateException.class, () -> FlightAggregationStates.read(average, root));
                count.set(1, 0);
                sum.setSafe(0, "1.5".getBytes(StandardCharsets.UTF_8));
                assertThrows(IllegalStateException.class, () -> FlightAggregationStates.read(average, root));
            }
        }
        assertThrows(IllegalArgumentException.class, () -> DecimalAggregationState.encode(BigInteger.ZERO, 0, true));
        assertThrows(ArithmeticException.class, () -> DecimalAggregationState.encode(BigInteger.ONE.shiftLeft(256), 1, false));
    }
}
