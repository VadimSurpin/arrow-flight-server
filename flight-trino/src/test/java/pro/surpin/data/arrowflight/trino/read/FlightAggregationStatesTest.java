package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.metadata.AggregateSpec;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;

import io.trino.spi.type.RowType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks native state assembly, empty defaults and malformed remote results. */
@Tag("unit")
class FlightAggregationStatesTest {
    private static final FlightColumnHandle LONG_INPUT = new FlightColumnHandle("v", "v", BIGINT);
    private static final FlightColumnHandle DOUBLE_INPUT = new FlightColumnHandle("d", "d", DOUBLE);
    private static final RowType AVERAGE_TYPE = RowType.anonymous(List.of(DOUBLE, BIGINT));

    /** Checks empty states preserve COUNT zero and SQL NULL for other final results. */
    @Test
    void buildsEmptyNativeStates() {
        for (AggregateSpec.Function function : AggregateSpec.Function.values()) {
            var specification = specification(function);
            var block = FlightAggregationStates.empty(specification);
            assertEquals(1, block.getPositionCount());
            switch (function) {
                case COUNT -> assertEquals(0L, BIGINT.getLong(block, 0));
                case AVG -> {
                    var row = AVERAGE_TYPE.getObject(block, 0);
                    assertFalse(block.isNull(0));
                    assertEquals(0.0, DOUBLE.getDouble(row.getRawFieldBlock(0), row.getRawIndex()));
                    assertEquals(0L, BIGINT.getLong(row.getRawFieldBlock(1), row.getRawIndex()));
                }
                default -> assertTrue(block.isNull(0));
            }
        }
    }

    /** Checks AVG uses sum/count states and normalizes empty sums to zero. */
    @Test
    void assemblesAverageStates() {
        var specification = specification(AggregateSpec.Function.AVG);
        try (var allocator = new RootAllocator();
             var sums = new Float8Vector("state_sum", allocator);
             var counts = new BigIntVector("state_count", allocator)) {
            sums.allocateNew(2);
            counts.allocateNew(2);
            sums.set(0, 30.0);
            sums.setNull(1);
            counts.set(0, 2L);
            counts.set(1, 0L);
            sums.setValueCount(2);
            counts.setValueCount(2);
            try (var root = VectorSchemaRoot.of(sums, counts)) {
                root.setRowCount(2);
                var block = FlightAggregationStates.read(specification, root);
                var row = AVERAGE_TYPE.getObject(block, 0);
                assertEquals(30.0, DOUBLE.getDouble(row.getRawFieldBlock(0), row.getRawIndex()));
                assertEquals(2L, BIGINT.getLong(row.getRawFieldBlock(1), row.getRawIndex()));
                var empty = AVERAGE_TYPE.getObject(block, 1);
                assertEquals(0.0, DOUBLE.getDouble(empty.getRawFieldBlock(0), empty.getRawIndex()));
                counts.set(1, 1L);
                assertThrows(IllegalStateException.class, () -> FlightAggregationStates.read(specification, root));
                counts.set(1, -1L);
                assertThrows(IllegalStateException.class, () -> FlightAggregationStates.read(specification, root));
                counts.setNull(1);
                assertThrows(IllegalStateException.class, () -> FlightAggregationStates.read(specification, root));
                counts.set(1, 0L);
                sums.set(1, 10.0);
                assertThrows(IllegalStateException.class, () -> FlightAggregationStates.read(specification, root));
            }
        }
    }

    /** Checks integer sums remain exact beyond DOUBLE precision and reject BIGINT overflow. */
    @Test
    void convertsExactIntegerSums() {
        try (var allocator = new RootAllocator(); var sums = new DecimalVector("state", allocator, 29, 0)) {
            sums.allocateNew(2);
            sums.set(0, new BigDecimal("9007199254740993"));
            sums.setNull(1);
            sums.setValueCount(2);
            try (var root = VectorSchemaRoot.of(sums)) {
                root.setRowCount(2);
                var specification = specification(AggregateSpec.Function.SUM);
                var block = FlightAggregationStates.read(specification, root);
                assertEquals(9007199254740993L, BIGINT.getLong(block, 0));
                assertTrue(block.isNull(1));
                sums.set(0, new BigDecimal("9223372036854775808"));
                assertThrows(ArithmeticException.class, () -> FlightAggregationStates.read(specification, root));
            }
        }
    }

    /** Checks nullable DOUBLE sums and integral extrema preserve their scalar states. */
    @Test
    void readsScalarStates() {
        try (var allocator = new RootAllocator(); var sums = new Float8Vector("state", allocator)) {
            sums.allocateNew(2);
            sums.set(0, 12.5);
            sums.setNull(1);
            sums.setValueCount(2);
            try (var root = VectorSchemaRoot.of(sums)) {
                root.setRowCount(2);
                var sum = new AggregateSpec(AggregateSpec.Function.SUM, Optional.of(DOUBLE_INPUT),
                        new FlightColumnHandle("state", "state", DOUBLE));
                var block = FlightAggregationStates.read(sum, root);
                assertEquals(12.5, DOUBLE.getDouble(block, 0));
                assertTrue(block.isNull(1));
                assertThrows(IllegalStateException.class,
                        () -> FlightAggregationStates.read(specification(AggregateSpec.Function.MIN), root));
            }
        }
    }

    /**
     * Creates a supported integral-input contract.
     *
     * @param function logical aggregate
     * @return aggregate specification
     */
    private static AggregateSpec specification(AggregateSpec.Function function) {
        return new AggregateSpec(function, Optional.of(LONG_INPUT),
                new FlightColumnHandle("state", "state", function == AggregateSpec.Function.AVG ? AVERAGE_TYPE : BIGINT));
    }
}
