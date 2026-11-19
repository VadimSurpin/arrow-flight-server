package pro.surpin.data.arrowflight.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Describes remote aggregates and their native Trino intermediate state.
 *
 * @param function logical aggregate
 * @param input input column, or empty for COUNT(*)
 * @param output native state column
 * @param filter optional Boolean aggregate mask
 */
public record AggregateSpec(@JsonProperty("function") Function function,
                            @JsonProperty("input") Optional<FlightColumnHandle> input,
                            @JsonProperty("output") FlightColumnHandle output,
                            @JsonProperty("filter") Optional<FlightColumnHandle> filter) {
    /**
     * Creates an aggregate without a filter.
     *
     * @param function aggregate operation
     * @param input input column
     * @param output state output
     */
    public AggregateSpec(Function function, Optional<FlightColumnHandle> input, FlightColumnHandle output) {
        this(function, input, output, Optional.empty());
    }
    /** Supported logical and remote aggregate functions. */
    public enum Function { COUNT, MIN, MAX, SUM, AVG }

    /** Aggregate operations requested from the Flight server. */
    public enum RemoteFunction { COUNT, MIN, MAX, SUM, FLIGHT_EXACT_SUM, FLIGHT_CHAR_MIN, FLIGHT_CHAR_MAX, FLIGHT_UUID_MIN, FLIGHT_UUID_MAX }

    /** Native state assembly strategies. */
    public enum StateFormat { COUNT, SCALAR, LONG_SUM, AVERAGE, REAL_AVERAGE, DECIMAL_SUM, DECIMAL_AVERAGE, NEGATED_EXTREMUM }

    /** Explicit conversions applied to remote aggregate inputs. */
    public enum InputCast { NONE, DOUBLE, DECIMAL_19, NEGATE, CHARACTER_LENGTH }

    /** Native states supplied when a global scan has no endpoints. */
    public enum EmptyState { ZERO, NULL, AVERAGE_ZERO }

    /**
     * Validates the native state contract.
     *
     * @param function logical aggregate
     * @param input input column, or empty for COUNT(*)
     * @param output native state column
     * @param filter optional Boolean aggregate mask
     * @throws IllegalArgumentException if the input or state type is unsupported
     */
    @JsonCreator
    public AggregateSpec {
        Objects.requireNonNull(function, "function");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(output, "output");
        filter = filter == null ? Optional.empty() : filter;
        if (filter.isPresent() && (!BOOLEAN.equals(filter.get().type())
                || function == Function.MIN || function == Function.MAX)) {
            throw new IllegalArgumentException("Unsupported partial aggregate filter");
        }
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
            case COUNT -> type == null || integral(type) || floating(type) || BOOLEAN.equals(type)
                    || type instanceof io.trino.spi.type.VarcharType || type instanceof DecimalType
                    || DATE.equals(type) || type instanceof TimestampType || VARBINARY.equals(type)
                    || type instanceof io.trino.spi.type.UuidType
                    || type instanceof io.trino.spi.type.CharType || isInterval(type) || isIpAddress(type)
                    ? Optional.of(BIGINT) : Optional.empty();
            case MIN, MAX -> integral(type) || floating(type) || type instanceof DecimalType || orderableScalar(type)
                    || input.filter(FlightColumnHandle::nullFreeOrder).isPresent() && type != null && type.isOrderable()
                    ? Optional.ofNullable(type) : Optional.empty();
            case SUM -> isInterval(type) ? Optional.of(BIGINT) : type instanceof DecimalType ? Optional.of(VARBINARY)
                    : integral(type) ? Optional.of(BIGINT) : floating(type) ? Optional.of(DOUBLE) : Optional.empty();
            case AVG -> type instanceof DecimalType ? Optional.of(VARBINARY)
                    : REAL.equals(type) ? Optional.of(RowType.anonymous(List.of(BIGINT, DOUBLE)))
                    : integral(type) || DOUBLE.equals(type) || isInterval(type)
                            ? Optional.of(RowType.anonymous(List.of(DOUBLE, BIGINT))) : Optional.empty();
        };
    }

    /**
     * Checks whether the unchanged Flight server can produce this aggregate state.
     *
     * @return whether the server supports the required SQL and exact value transport
     */
    public boolean supportedByServer() {
        if (filter.isPresent() || input.filter(column -> column.expression() != null).isPresent()) {
            return false;
        }
        Type type = input.map(FlightColumnHandle::type).orElse(null);
        return switch (function) {
            case COUNT -> type == null || integral(type) || floating(type) || BOOLEAN.equals(type)
                    || type instanceof io.trino.spi.type.VarcharType || type instanceof DecimalType
                    || DATE.equals(type) || type instanceof TimestampType || VARBINARY.equals(type);
            case MIN -> integral(type) || floating(type);
            case MAX -> integral(type);
            case SUM, AVG -> integral(type) || floating(type);
        };
    }

    /**
     * Returns the final SQL result type.
     *
     * @return final result type
     */
    public Type resultType() {
        if (function == Function.COUNT) {
            return BIGINT;
        }
        Type type = input.orElseThrow().type();
        return switch (function) {
            case SUM -> type instanceof DecimalType decimal
                    ? DecimalType.createDecimalType(38, decimal.getScale()) : integral(type) ? BIGINT : type;
            case AVG -> integral(type) ? DOUBLE : type;
            default -> type;
        };
    }

    /**
     * Returns the state assembly strategy.
     *
     * @return native state format
     */
    public StateFormat stateFormat() {
        return switch (function) {
            case COUNT -> StateFormat.COUNT;
            case AVG -> input.orElseThrow().type() instanceof DecimalType ? StateFormat.DECIMAL_AVERAGE
                    : input.orElseThrow().type().equals(REAL) ? StateFormat.REAL_AVERAGE : StateFormat.AVERAGE;
            case SUM -> input.orElseThrow().type() instanceof DecimalType ? StateFormat.DECIMAL_SUM
                    : output.type().equals(BIGINT) ? StateFormat.LONG_SUM : StateFormat.SCALAR;
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
            case SCALAR, LONG_SUM, NEGATED_EXTREMUM, DECIMAL_SUM, DECIMAL_AVERAGE -> EmptyState.NULL;
            case AVERAGE, REAL_AVERAGE -> EmptyState.AVERAGE_ZERO;
        };
    }

    /**
     * Returns the scalar aggregates computed by Flight.
     *
     * @return ordered remote aggregate expressions
     */
    public List<RemoteAggregate> remoteAggregates() {
        if ((function == Function.MIN || function == Function.MAX)
                && input.orElseThrow().type() instanceof io.trino.spi.type.CharType) {
            var operation = function == Function.MIN ? RemoteFunction.FLIGHT_CHAR_MIN : RemoteFunction.FLIGHT_CHAR_MAX;
            return List.of(new RemoteAggregate(operation, input,
                    new FlightColumnHandle(output.name(), output.remoteName(), VARCHAR), InputCast.CHARACTER_LENGTH));
        }
        if ((function == Function.MIN || function == Function.MAX) && (isInterval(input.orElseThrow().type())
                || isIpAddress(input.orElseThrow().type()))) {
            Type physical = isInterval(input.orElseThrow().type()) ? BIGINT : VARBINARY;
            if (input.orElseThrow().type().getTypeSignature().getBase()
                    .equals(io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH)) {
                physical = INTEGER;
            }
            return List.of(new RemoteAggregate(RemoteFunction.valueOf(function.name()), input,
                    new FlightColumnHandle(output.name(), output.remoteName(), physical), InputCast.NONE));
        }
        if ((function == Function.MIN || function == Function.MAX)
                && input.orElseThrow().type() instanceof io.trino.spi.type.UuidType) {
            return List.of(new RemoteAggregate(function == Function.MIN ? RemoteFunction.FLIGHT_UUID_MIN : RemoteFunction.FLIGHT_UUID_MAX, input,
                    new FlightColumnHandle(output.name(), output.remoteName(), VARCHAR), InputCast.NONE));
        }
        if (stateFormat() == StateFormat.NEGATED_EXTREMUM) {
            return List.of(new RemoteAggregate(RemoteFunction.MIN, input, output, InputCast.NEGATE));
        }
        if (stateFormat() == StateFormat.DECIMAL_SUM || stateFormat() == StateFormat.DECIMAL_AVERAGE) {
            String name = output.remoteName();
            var sum = new RemoteAggregate(RemoteFunction.FLIGHT_EXACT_SUM, input,
                    new FlightColumnHandle(name + "_sum", name + "_sum", VARCHAR), InputCast.NONE);
            return function == Function.SUM ? List.of(sum) : List.of(sum,
                    new RemoteAggregate(RemoteFunction.COUNT, input,
                            new FlightColumnHandle(name + "_count", name + "_count", BIGINT), InputCast.NONE));
        }
        if (function == Function.AVG) {
            String name = output.remoteName();
            return List.of(
                    new RemoteAggregate(RemoteFunction.SUM, input,
                            new FlightColumnHandle(name + "_sum", name + "_sum", DOUBLE),
                            DOUBLE.equals(input.orElseThrow().type()) ? InputCast.NONE : InputCast.DOUBLE),
                    new RemoteAggregate(RemoteFunction.COUNT, input,
                            new FlightColumnHandle(name + "_count", name + "_count", BIGINT), InputCast.NONE));
        }
        if (stateFormat() == StateFormat.LONG_SUM) {
            return List.of(new RemoteAggregate(RemoteFunction.SUM, input,
                    new FlightColumnHandle(output.name(), output.remoteName(), DecimalType.createDecimalType(29, 0)),
                    InputCast.DECIMAL_19));
        }
        return List.of(new RemoteAggregate(RemoteFunction.valueOf(function.name()), input, output,
                function == Function.SUM && REAL.equals(input.orElseThrow().type()) ? InputCast.DOUBLE : InputCast.NONE));
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
     * Checks scalar values whose DuckDB and Trino orderings are compatible.
     *
     * @param type input type, or null for no input
     * @return whether the scalar type can use native extrema
     */
    public static boolean orderableScalar(Type type) {
        return BOOLEAN.equals(type) || type instanceof io.trino.spi.type.VarcharType
                || type instanceof io.trino.spi.type.UuidType
                || type instanceof io.trino.spi.type.CharType
                || isInterval(type) || isIpAddress(type)
                || VARBINARY.equals(type) || DATE.equals(type) || type instanceof TimeType
                || type instanceof TimestampType
                || type instanceof TimestampWithTimeZoneType timestamp && timestamp.getPrecision() <= 6;
    }

    /**
     * Checks the two native interval families.
     *
     * @param type input type, or null
     * @return whether the type is a native month or millisecond interval
     */
    public static boolean isInterval(Type type) {
        return type != null && (type.getTypeSignature().getBase().equals(io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH)
                || type.getTypeSignature().getBase().equals(io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND));
    }

    /**
     * Checks the native IP address type.
     *
     * @param type input type, or null
     * @return whether the type stores a canonical 16-byte IP address
     */
    public static boolean isIpAddress(Type type) {
        return type != null && type.getTypeSignature().getBase().equals(io.trino.spi.type.StandardTypes.IPADDRESS);
    }

    /**
     * Describes one scalar SQL aggregate returned in Arrow.
     *
     * @param function remote aggregate function
     * @param input input column, or empty for COUNT(*)
     * @param output remote Arrow column
     * @param inputCast conversion applied before aggregation
     */
    public record RemoteAggregate(RemoteFunction function, Optional<FlightColumnHandle> input,
                                  FlightColumnHandle output, InputCast inputCast) { }
}
