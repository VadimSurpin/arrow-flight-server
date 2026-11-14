package pro.surpin.data.arrowflight.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.DecimalType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Describes remote aggregates and their native Trino intermediate state.
 *
 * @param function logical aggregate
 * @param input input column, or empty for COUNT(*)
 * @param output native state column
 */
public record AggregateSpec(@JsonProperty("function") Function function,
                            @JsonProperty("input") Optional<FlightColumnHandle> input,
                            @JsonProperty("output") FlightColumnHandle output) {
    /** Supported logical and remote aggregate functions. */
    public enum Function { COUNT, MIN, MAX, SUM, AVG }

    /** Native state assembly strategies. */
    public enum StateFormat { COUNT, SCALAR, LONG_SUM, AVERAGE, NEGATED_EXTREMUM }

    /** Explicit conversions applied to remote aggregate inputs. */
    public enum InputCast { NONE, DOUBLE, DECIMAL_19, NEGATE }

    /** Native states supplied when a global scan has no endpoints. */
    public enum EmptyState { ZERO, NULL, AVERAGE_ZERO }

    /**
     * Validates the native state contract.
     *
     * @param function logical aggregate
     * @param input input column, or empty for COUNT(*)
     * @param output native state column
     * @throws IllegalArgumentException if the input or state type is unsupported
     */
    @JsonCreator
    public AggregateSpec {
        Objects.requireNonNull(function, "function");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(output, "output");
        if (!intermediateType(function, input).filter(output.type()::equals).isPresent()) {
            throw new IllegalArgumentException("Unsupported partial aggregate state: " + function + " " + output.type());
        }
    }

    /**
     * Returns a supported native intermediate type.
     *
     * @param function logical aggregate
     * @param input input column, or empty for COUNT(*)
     * @return native type, or empty for unsupported inputs
     */
    public static Optional<Type> intermediateType(Function function, Optional<FlightColumnHandle> input) {
        Type type = input.map(FlightColumnHandle::type).orElse(null);
        return switch (function) {
            case COUNT -> type == null || integral(type) || type.equals(DOUBLE) || type.equals(BOOLEAN)
                    || type instanceof io.trino.spi.type.VarcharType ? Optional.of(BIGINT) : Optional.empty();
            case MIN, MAX -> integral(type) || floating(type) || type instanceof DecimalType
                    ? Optional.ofNullable(type) : Optional.empty();
            case SUM -> integral(type) ? Optional.of(BIGINT)
                    : DOUBLE.equals(type) ? Optional.of(DOUBLE) : Optional.empty();
            case AVG -> integral(type) || DOUBLE.equals(type)
                    ? Optional.of(RowType.anonymous(List.of(DOUBLE, BIGINT))) : Optional.empty();
        };
    }

    /**
     * Returns the final SQL result type.
     *
     * @return final result type
     */
    public Type resultType() {
        return function == Function.AVG ? DOUBLE : output.type();
    }

    /**
     * Returns the state assembly strategy.
     *
     * @return native state format
     */
    public StateFormat stateFormat() {
        return switch (function) {
            case COUNT -> StateFormat.COUNT;
            case AVG -> StateFormat.AVERAGE;
            case SUM -> output.type().equals(BIGINT) ? StateFormat.LONG_SUM : StateFormat.SCALAR;
            case MAX -> floating(input.orElseThrow().type()) ? StateFormat.NEGATED_EXTREMUM : StateFormat.SCALAR;
            default -> StateFormat.SCALAR;
        };
    }

    /**
     * Returns the empty global state strategy.
     *
     * @return empty state
     */
    public EmptyState emptyState() {
        return switch (stateFormat()) {
            case COUNT -> EmptyState.ZERO;
            case SCALAR, LONG_SUM, NEGATED_EXTREMUM -> EmptyState.NULL;
            case AVERAGE -> EmptyState.AVERAGE_ZERO;
        };
    }

    /**
     * Returns the scalar aggregates computed by Flight.
     *
     * @return ordered remote aggregate expressions
     */
    public List<RemoteAggregate> remoteAggregates() {
        if (stateFormat() == StateFormat.NEGATED_EXTREMUM) {
            return List.of(new RemoteAggregate(Function.MIN, input, output, InputCast.NEGATE));
        }
        if (function == Function.AVG) {
            String name = output.remoteName();
            return List.of(
                    new RemoteAggregate(Function.SUM, input,
                            new FlightColumnHandle(name + "_sum", name + "_sum", DOUBLE),
                            integral(input.orElseThrow().type()) ? InputCast.DOUBLE : InputCast.NONE),
                    new RemoteAggregate(Function.COUNT, input,
                            new FlightColumnHandle(name + "_count", name + "_count", BIGINT), InputCast.NONE));
        }
        if (stateFormat() == StateFormat.LONG_SUM) {
            return List.of(new RemoteAggregate(function, input,
                    new FlightColumnHandle(output.name(), output.remoteName(), DecimalType.createDecimalType(29, 0)),
                    InputCast.DECIMAL_19));
        }
        return List.of(new RemoteAggregate(function, input, output, InputCast.NONE));
    }

    /**
     * Checks exact integral types supported by the contract.
     *
     * @param type input type, or null for no input
     * @return whether the type is a supported integer
     */
    private static boolean integral(Type type) {
        return BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type);
    }

    /**
     * Checks supported floating-point extrema types.
     *
     * @param type input type, or null for no input
     * @return whether the type is REAL or DOUBLE
     */
    private static boolean floating(Type type) {
        return REAL.equals(type) || DOUBLE.equals(type);
    }

    /**
     * Describes one scalar SQL aggregate returned in Arrow.
     *
     * @param function remote aggregate function
     * @param input input column, or empty for COUNT(*)
     * @param output remote Arrow column
     * @param inputCast conversion applied before aggregation
     */
    public record RemoteAggregate(Function function, Optional<FlightColumnHandle> input,
                                  FlightColumnHandle output, InputCast inputCast) { }
}
