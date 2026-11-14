package pro.surpin.data.arrowflight.trino.metadata;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Checks independent input, final result and native state contracts. */
@Tag("unit")
class NumericAggregateContractTest {
    /** Checks every valid decimal precision and scale uses exact native states. */
    @Test
    void coversAllDecimalPrecisionsAndScales() {
        for (int precision = 1; precision <= 38; precision++) {
            for (int scale = 0; scale <= precision; scale++) {
                var type = DecimalType.createDecimalType(precision, scale);
                var input = new FlightColumnHandle("v", "v", type);
                for (var function : List.of(AggregateSpec.Function.SUM, AggregateSpec.Function.AVG)) {
                    var specification = new AggregateSpec(function, Optional.of(input),
                            new FlightColumnHandle("state", "state", VARBINARY));
                    assertEquals(VARBINARY, AggregateSpec.intermediateType(function, Optional.of(input)).orElseThrow());
                    assertEquals(function == AggregateSpec.Function.SUM
                            ? DecimalType.createDecimalType(38, scale) : type, specification.resultType());
                    assertEquals(AggregateSpec.RemoteFunction.FLIGHT_EXACT_SUM,
                            specification.remoteAggregates().getFirst().function());
                    assertEquals(AggregateSpec.EmptyState.NULL, specification.emptyState());
                }
            }
        }
    }

    /** Checks REAL final results differ from their DOUBLE and row intermediate states. */
    @Test
    void preservesRealResultAndStateTypes() {
        var input = new FlightColumnHandle("v", "v", REAL);
        for (var function : List.of(AggregateSpec.Function.SUM, AggregateSpec.Function.AVG)) {
            var state = function == AggregateSpec.Function.SUM ? DOUBLE : RowType.anonymous(List.of(BIGINT, DOUBLE));
            var specification = new AggregateSpec(function, Optional.of(input),
                    new FlightColumnHandle("state", "state", state));
            assertEquals(state, AggregateSpec.intermediateType(function, Optional.of(input)).orElseThrow());
            assertEquals(REAL, specification.resultType());
            assertEquals(AggregateSpec.InputCast.DOUBLE, specification.remoteAggregates().getFirst().inputCast());
        }
    }
}
