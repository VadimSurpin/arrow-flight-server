package pro.surpin.data.arrowflight.trino.pushdown;

import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/** Renders scalar expressions only when declared bounds preserve native Trino semantics. */
public final class TypedExpressions {
    private static final Map<Type, Integer> INTEGER_BITS = Map.of(TINYINT, 8, SMALLINT, 16, INTEGER, 32, BIGINT, 64);
    private static final Map<Type, String> SQL_TYPES = Map.of(TINYINT, "TINYINT", SMALLINT, "SMALLINT", INTEGER, "INTEGER",
            BIGINT, "BIGINT", REAL, "REAL", DOUBLE, "DOUBLE", BOOLEAN, "BOOLEAN");
    private static final Map<FunctionName, String> ARITHMETIC = Map.of(StandardFunctions.ADD_FUNCTION_NAME, "+",
            StandardFunctions.SUBTRACT_FUNCTION_NAME, "-", StandardFunctions.MULTIPLY_FUNCTION_NAME, "*");
    private static final Map<FunctionName, String> COMPARISONS = Map.of(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, "=",
            StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, "<>", StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, "<",
            StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, "<=", StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, ">",
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ">=",
            StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, "IS DISTINCT FROM");

    /** Prevents utility construction. */
    private TypedExpressions() { }

    /**
     * Renders a supported expression with explicit intermediate types.
     *
     * @param expression typed connector expression
     * @param assignments source variables and columns
     * @param quote configured identifier quote
     * @return validated SQL, or empty when native execution is required
     */
    public static Optional<String> translate(ConnectorExpression expression, Map<String, ColumnHandle> assignments, String quote) {
        SqlRenderer renderer = new SqlRenderer(quote == null || quote.isEmpty() ? "\"" : quote);
        if (expression instanceof Constant constant) {
            return literal(constant);
        }
        if (expression instanceof Variable variable) {
            if (!(assignments.get(variable.getName()) instanceof FlightColumnHandle column)
                    || !column.type().equals(variable.getType()) || sqlType(variable.getType()).isEmpty()) {
                return Optional.empty();
            }
            String value = column.expression() == null ? renderer.quoteIdentifier(column.remoteName()) : column.expression();
            return Optional.of(cast(value, variable.getType()));
        }
        if (!(expression instanceof Call call) || sqlType(call.getType()).isEmpty()) {
            return Optional.empty();
        }
        List<String> values = new ArrayList<>();
        for (var argument : call.getArguments()) {
            Optional<String> translated = translate(argument, assignments, quote);
            if (translated.isEmpty()) {
                return Optional.empty();
            }
            values.add(translated.get());
        }
        var name = call.getFunctionName();
        var args = call.getArguments();
        if (name.equals(new FunctionName("$trino_coalesce")) && args.size() >= 2
                && args.stream().allMatch(argument -> argument.getType().equals(call.getType()))) {
            return Optional.of(cast("COALESCE(" + String.join(", ", values) + ")", call.getType()));
        }
        if (name.equals(new FunctionName("$trino_case")) && args.size() >= 3 && args.size() % 2 == 1) {
            StringBuilder sql = new StringBuilder("CASE");
            for (int i = 0; i < args.size() - 1; i += 2) {
                if (!BOOLEAN.equals(args.get(i).getType()) || !call.getType().equals(args.get(i + 1).getType())) {
                    return Optional.empty();
                }
                sql.append(" WHEN ").append(values.get(i)).append(" THEN ").append(values.get(i + 1));
            }
            return call.getType().equals(args.getLast().getType())
                    ? Optional.of(cast(sql + " ELSE " + values.getLast() + " END", call.getType())) : Optional.empty();
        }
        if (name.equals(StandardFunctions.CAST_FUNCTION_NAME) && args.size() == 1
                && (args.getFirst() instanceof Constant constant && constant.getValue() == null
                        || safeCast(args.getFirst().getType(), call.getType()))) {
            return Optional.of(cast(values.getFirst(), call.getType()));
        }
        String arithmetic = ARITHMETIC.get(name);
        if (arithmetic != null && args.size() == 2 && safeArithmetic(call.getType(), args, arithmetic)) {
            if (call.getType() instanceof DecimalType) {
                values = arithmetic.equals("*")
                        ? List.of(cast(values.get(0), DecimalType.createDecimalType(38, scale(args.get(0).getType()))),
                                cast(values.get(1), DecimalType.createDecimalType(38, scale(args.get(1).getType()))))
                        : values.stream().map(value -> cast(value, call.getType())).toList();
            }
            return Optional.of(cast("(" + values.get(0) + " " + arithmetic + " " + values.get(1) + ")", call.getType()));
        }
        String comparison = COMPARISONS.get(name);
        if (comparison != null && args.size() == 2 && BOOLEAN.equals(call.getType())
                && !floating(args.getFirst().getType())
                && (args.get(0).getType().equals(args.get(1).getType())
                        || args.get(0).getType() instanceof VarcharType && args.get(1).getType() instanceof VarcharType)) {
            return Optional.of(cast("(" + values.get(0) + " " + comparison + " " + values.get(1) + ")", BOOLEAN));
        }
        if ((name.equals(StandardFunctions.AND_FUNCTION_NAME) || name.equals(StandardFunctions.OR_FUNCTION_NAME))
                && values.size() >= 2 && BOOLEAN.equals(call.getType())
                && args.stream().allMatch(argument -> BOOLEAN.equals(argument.getType()))) {
            return Optional.of(cast("(" + String.join(name.equals(StandardFunctions.AND_FUNCTION_NAME) ? " AND " : " OR ", values) + ")", BOOLEAN));
        }
        if (values.size() == 1 && name.equals(StandardFunctions.IS_NULL_FUNCTION_NAME) && BOOLEAN.equals(call.getType())) {
            return Optional.of(cast("(" + values.getFirst() + " IS NULL)", BOOLEAN));
        }
        if (values.size() == 1 && name.equals(StandardFunctions.NOT_FUNCTION_NAME) && BOOLEAN.equals(call.getType())
                && BOOLEAN.equals(args.getFirst().getType())) {
            return Optional.of(cast("(NOT " + values.getFirst() + ")", BOOLEAN));
        }
        return Optional.empty();
    }

    /**
     * Checks casts that preserve values or use compatible nonthrowing numeric rounding.
     *
     * @param source source type
     * @param target target type
     * @return whether all source values fit the target representation
     */
    private static boolean safeCast(Type source, Type target) {
        if (source.equals(target)) {
            return true;
        }
        if (source instanceof VarcharType from && target instanceof VarcharType to) {
            return to.isUnbounded() || !from.isUnbounded() && from.getBoundedLength() <= to.getBoundedLength();
        }
        if (INTEGER_BITS.containsKey(source) && INTEGER_BITS.containsKey(target)) {
            return INTEGER_BITS.get(source) <= INTEGER_BITS.get(target);
        }
        if (target instanceof DecimalType decimal && exact(source)) {
            return decimal.getScale() >= scale(source)
                    && decimal.getPrecision() - decimal.getScale() >= precision(source) - scale(source);
        }
        return DOUBLE.equals(target) && REAL.equals(source);
    }

    /**
     * Checks that arithmetic fits the declared result without implicit narrowing.
     *
     * @param output declared result type
     * @param arguments typed operands
     * @param operation arithmetic operator
     * @return whether every value in the operand domains is representable
     */
    private static boolean safeArithmetic(Type output, List<ConnectorExpression> arguments, String operation) {
        Type left = arguments.get(0).getType();
        Type right = arguments.get(1).getType();
        if (output instanceof DecimalType result && exact(left) && exact(right)) {
            int resultScale = operation.equals("*") ? scale(left) + scale(right) : Math.max(scale(left), scale(right));
            int required = operation.equals("*") ? precision(left) + precision(right)
                    : Math.max(precision(left) - scale(left), precision(right) - scale(right)) + resultScale + 1;
            return required <= 38 && result.getPrecision() >= required && result.getScale() == resultScale
                    && (operation.equals("*") || safeCast(left, output) && safeCast(right, output));
        }
        if (INTEGER_BITS.containsKey(output) && INTEGER_BITS.containsKey(left) && INTEGER_BITS.containsKey(right)) {
            int required = operation.equals("*") ? INTEGER_BITS.get(left) + INTEGER_BITS.get(right)
                    : Math.max(INTEGER_BITS.get(left), INTEGER_BITS.get(right)) + 1;
            return INTEGER_BITS.get(output) >= required;
        }
        return false;
    }

    /**
     * Renders a lossless constant representation.
     *
     * @param constant typed constant
     * @return SQL literal, or empty for unsupported values
     */
    private static Optional<String> literal(Constant constant) {
        Object value = constant.getValue();
        Type type = constant.getType();
        if (value == null) {
            return Optional.of("NULL");
        }
        if (type instanceof DecimalType decimal) {
            var unscaled = decimal.isShort() ? java.math.BigInteger.valueOf(((Number) value).longValue())
                    : ((Int128) value).toBigInteger();
            return Optional.of(cast(new BigDecimal(unscaled, decimal.getScale()).toPlainString(), type));
        }
        if (INTEGER_BITS.containsKey(type)) {
            return Optional.of(cast(Long.toString(((Number) value).longValue()), type));
        }
        if (BOOLEAN.equals(type)) {
            return Optional.of(Boolean.TRUE.equals(value) ? "TRUE" : "FALSE");
        }
        if (floating(type)) {
            String text = REAL.equals(type) ? Float.toString(Float.intBitsToFloat(((Number) value).intValue()))
                    : Double.toString(((Number) value).doubleValue());
            return Optional.of(cast("'" + text + "'", type));
        }
        if (type instanceof VarcharType && value instanceof Slice slice) {
            String text = slice.toStringUtf8();
            return text.indexOf('\0') < 0 && slice.equals(Slices.utf8Slice(text))
                    ? Optional.of("'" + text.replace("'", "''") + "'") : Optional.empty();
        }
        return Optional.empty();
    }

    /**
     * Renders an explicit result cast.
     *
     * @param sql scalar expression
     * @param type declared type
     * @return typed SQL expression
     */
    private static String cast(String sql, Type type) {
        return "CAST(" + sql + " AS " + sqlType(type).orElseThrow() + ")";
    }

    /**
     * Maps admitted scalar types to DuckDB declarations.
     *
     * @param type native type
     * @return SQL type, or empty when unsupported
     */
    private static Optional<String> sqlType(Type type) {
        if (type instanceof DecimalType decimal) {
            return Optional.of("DECIMAL(" + decimal.getPrecision() + "," + decimal.getScale() + ")");
        }
        return type instanceof VarcharType ? Optional.of("VARCHAR") : Optional.ofNullable(SQL_TYPES.get(type));
    }

    /**
     * Checks exact numeric types.
     *
     * @param type native type
     * @return whether the type represents exact decimal or integer values
     */
    private static boolean exact(Type type) {
        return type instanceof DecimalType || INTEGER_BITS.containsKey(type);
    }

    /**
     * Checks floating numeric types.
     *
     * @param type native type
     * @return whether the type uses IEEE floating arithmetic
     */
    private static boolean floating(Type type) {
        return REAL.equals(type) || DOUBLE.equals(type);
    }

    /**
     * Returns a conservative decimal precision for an exact type.
     *
     * @param type exact numeric type
     * @return decimal digits required for the entire input domain
     */
    private static int precision(Type type) {
        return type instanceof DecimalType decimal ? decimal.getPrecision() : switch (INTEGER_BITS.getOrDefault(type, 0)) {
            case 8 -> 3;
            case 16 -> 5;
            case 32 -> 10;
            case 64 -> 19;
            default -> 0;
        };
    }

    /**
     * Returns an exact type's decimal scale.
     *
     * @param type exact numeric type
     * @return scale, or zero for integral values
     */
    private static int scale(Type type) {
        return type instanceof DecimalType decimal ? decimal.getScale() : 0;
    }
}
