package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.expr.Aggregation;
import pro.surpin.data.arrowflight.client.expr.Expr;
import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;
import org.apache.spark.sql.connector.expressions.aggregate.Sum;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Translates Spark pushdown filters, predicates and aggregations into engine-neutral expressions.
 */
public final class SparkPushdown {

    /**
     * Prevents instantiation.
     */
    private SparkPushdown() {
    }

    // ── V1 filters ───────────────────────────────────────────────────────

    /**
     * Translates a Spark V1 filter; composite filters are translated only when every child is supported.
     * @param filter Spark filter
     * @return expression, or empty when the filter cannot be evaluated exactly by the server
     */
    public static Optional<Expr> translate(Filter filter) {
        return switch (filter) {
            case null -> Optional.empty();
            case EqualTo f -> compare(Expr.CompareOp.EQ, f.attribute(), f.value());
            case EqualNullSafe f -> equalNullSafe(f);
            case LessThan f -> compare(Expr.CompareOp.LT, f.attribute(), f.value());
            case LessThanOrEqual f -> compare(Expr.CompareOp.LE, f.attribute(), f.value());
            case GreaterThan f -> compare(Expr.CompareOp.GT, f.attribute(), f.value());
            case GreaterThanOrEqual f -> compare(Expr.CompareOp.GE, f.attribute(), f.value());
            case And f -> both(translate(f.left()), translate(f.right())).map(p -> new Expr.And(p[0], p[1]));
            case Or f -> both(translate(f.left()), translate(f.right())).map(p -> new Expr.Or(p[0], p[1]));
            case Not f -> translate(f.child()).map(Expr.Not::new);
            case IsNull f -> Optional.of(new Expr.IsNull(new Expr.Column(f.attribute()), false));
            case IsNotNull f -> Optional.of(new Expr.IsNull(new Expr.Column(f.attribute()), true));
            case StringStartsWith f -> Optional.of(like(f.attribute(), Expr.LikeKind.STARTS_WITH, f.value()));
            case StringEndsWith f -> Optional.of(like(f.attribute(), Expr.LikeKind.ENDS_WITH, f.value()));
            case StringContains f -> Optional.of(like(f.attribute(), Expr.LikeKind.CONTAINS, f.value()));
            case In f -> in(f);
            default -> Optional.empty();
        };
    }

    /**
     * Translates a V1 comparison; a null value is rejected because SQL comparison with NULL is never true.
     * @param op comparison operator
     * @param attribute column name
     * @param value literal value
     * @return comparison expression
     */
    private static Optional<Expr> compare(Expr.CompareOp op, String attribute, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        return Expr.Literal.of(value).map(lit -> new Expr.Compare(op, new Expr.Column(attribute), lit));
    }

    /**
     * Translates a V1 null-safe equality.
     * @param filter Spark filter
     * @return expression
     */
    private static Optional<Expr> equalNullSafe(EqualNullSafe filter) {
        Expr.Column column = new Expr.Column(filter.attribute());
        if (filter.value() == null) {
            return Optional.of(new Expr.IsNull(column, false));
        }
        return compare(Expr.CompareOp.EQ, filter.attribute(), filter.value())
                .map(eq -> new Expr.And(new Expr.IsNull(column, true), eq));
    }

    /**
     * Translates a V1 IN filter; an empty list is always false.
     * @param filter Spark filter
     * @return expression
     */
    private static Optional<Expr> in(In filter) {
        Object[] values = filter.values();
        if (values == null || values.length == 0) {
            return Optional.of(new Expr.Constant(false));
        }
        List<Expr.Literal> literals = new ArrayList<>(values.length);
        for (Object value : values) {
            Optional<Expr.Literal> literal = Expr.Literal.of(value);
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            literals.add(literal.get());
        }
        return Optional.of(new Expr.In(new Expr.Column(filter.attribute()), literals));
    }

    /**
     * Builds a LIKE expression.
     * @param attribute column name
     * @param kind match kind
     * @param value plain string
     * @return expression
     */
    private static Expr like(String attribute, Expr.LikeKind kind, String value) {
        return new Expr.Like(new Expr.Column(attribute), kind, value);
    }

    // ── V2 predicates ────────────────────────────────────────────────────

    /**
     * Translates a Spark V2 predicate; composite predicates are translated only when every child is supported.
     * @param predicate Spark V2 predicate
     * @return expression, or empty when the predicate cannot be evaluated exactly by the server
     */
    public static Optional<Expr> translate(Predicate predicate) {
        if (predicate == null) {
            return Optional.empty();
        }
        String name = predicate.name().toUpperCase(Locale.ROOT);
        Expression[] children = predicate.children();
        Optional<Expr.CompareOp> op = Expr.CompareOp.fromSymbol(name);
        if (op.isPresent()) {
            return children.length == 2
                    ? both(operand(children[0]), operand(children[1]))
                            .map(p -> new Expr.Compare(op.get(), p[0], p[1]))
                    : Optional.empty();
        }
        return switch (name) {
            case "ALWAYS_TRUE" -> constant(children, true);
            case "ALWAYS_FALSE" -> constant(children, false);
            case "AND", "OR" -> logical(name, children);
            case "NOT" -> children.length == 1 && children[0] instanceof Predicate child
                    ? translate(child).map(Expr.Not::new) : Optional.empty();
            case "IS_NULL", "IS_NOT_NULL" -> children.length == 1
                    ? operand(children[0]).map(e -> new Expr.IsNull(e, "IS_NOT_NULL".equals(name)))
                    : Optional.empty();
            case "IN" -> in(children);
            case "STARTS_WITH" -> like(children, Expr.LikeKind.STARTS_WITH);
            case "ENDS_WITH" -> like(children, Expr.LikeKind.ENDS_WITH);
            case "CONTAINS" -> like(children, Expr.LikeKind.CONTAINS);
            default -> Optional.empty();
        };
    }

    /**
     * Translates a constant predicate, which must have no children.
     * @param children predicate children
     * @param value constant value
     * @return expression
     */
    private static Optional<Expr> constant(Expression[] children, boolean value) {
        return children.length == 0 ? Optional.of(new Expr.Constant(value)) : Optional.empty();
    }

    /**
     * Translates AND/OR with two predicate children.
     * @param name AND or OR
     * @param children predicate children
     * @return expression
     */
    private static Optional<Expr> logical(String name, Expression[] children) {
        if (children.length != 2 || !(children[0] instanceof Predicate left)
                || !(children[1] instanceof Predicate right)) {
            return Optional.empty();
        }
        return both(translate(left), translate(right))
                .map(p -> "AND".equals(name) ? new Expr.And(p[0], p[1]) : new Expr.Or(p[0], p[1]));
    }

    /**
     * Translates IN whose list contains only literals.
     * @param children value followed by list items
     * @return expression
     */
    private static Optional<Expr> in(Expression[] children) {
        if (children.length <= 1) {
            return Optional.empty();
        }
        Optional<Expr> value = operand(children[0]);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        List<Expr.Literal> literals = new ArrayList<>(children.length - 1);
        for (int i = 1; i < children.length; i++) {
            if (!(children[i] instanceof Literal<?> literal)) {
                return Optional.empty();
            }
            Optional<Expr.Literal> converted = literal(literal);
            if (converted.isEmpty()) {
                return Optional.empty();
            }
            literals.add(converted.get());
        }
        return Optional.of(new Expr.In(value.get(), literals));
    }

    /**
     * Translates a string match on a column with a non-null string literal.
     * @param children column and literal
     * @param kind match kind
     * @return expression
     */
    private static Optional<Expr> like(Expression[] children, Expr.LikeKind kind) {
        if (children.length != 2 || !(children[0] instanceof NamedReference reference)
                || !(children[1] instanceof Literal<?> literal)
                || literal.value() == null || !literal.dataType().equals(DataTypes.StringType)) {
            return Optional.empty();
        }
        return column(reference).map(c -> new Expr.Like(c, kind, literal.value().toString()));
    }

    /**
     * Translates a predicate operand, which may be a column or a literal.
     * @param expression Spark expression
     * @return expression
     */
    private static Optional<Expr> operand(Expression expression) {
        if (expression instanceof NamedReference reference) {
            return column(reference).map(Expr.class::cast);
        }
        if (expression instanceof Literal<?> literal) {
            return literal(literal).map(Expr.class::cast);
        }
        return Optional.empty();
    }

    /**
     * Translates a single-part column reference.
     * @param reference Spark reference
     * @return column, or empty for nested or blank references
     */
    private static Optional<Expr.Column> column(NamedReference reference) {
        String[] fields = reference.fieldNames();
        if (fields == null || fields.length != 1 || fields[0] == null || fields[0].isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new Expr.Column(fields[0]));
    }

    /**
     * Converts a typed Spark literal to a neutral Java value.
     * @param literal Spark literal
     * @return literal
     */
    private static Optional<Expr.Literal> literal(Literal<?> literal) {
        Object value = literal.value();
        DataType type = literal.dataType();
        if (value == null) {
            return Expr.Literal.of(null);
        }
        if (type.equals(DataTypes.DateType) && value instanceof Number days) {
            return Expr.Literal.of(LocalDate.ofEpochDay(days.longValue()));
        }
        if ((type.equals(DataTypes.TimestampType) || type.equals(DataTypes.TimestampNTZType))
                && value instanceof Number micros) {
            long epochMicros = micros.longValue();
            long seconds = Math.floorDiv(epochMicros, 1_000_000L);
            int nanos = Math.toIntExact(Math.floorMod(epochMicros, 1_000_000L) * 1_000L);
            return Expr.Literal.of(LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC));
        }
        if (type.equals(DataTypes.StringType)) {
            return Expr.Literal.of(value.toString());
        }
        if (value instanceof org.apache.spark.sql.types.Decimal decimal) {
            return Expr.Literal.of(decimal.toJavaBigDecimal());
        }
        return Expr.Literal.of(value);
    }

    // ── Aggregation ──────────────────────────────────────────────────────

    /**
     * Translates a Spark aggregation; SUM is pushed only for decimal and floating-point
     * inputs whose result type the server reproduces exactly.
     * @param aggregation Spark aggregation
     * @param schema table schema used to resolve columns and types
     * @return aggregation, or empty when any part cannot be pushed
     */
    public static Optional<Aggregation> translate(
            org.apache.spark.sql.connector.expressions.aggregate.Aggregation aggregation, StructType schema) {
        if (aggregation.aggregateExpressions().length == 0 && aggregation.groupByExpressions().length == 0) {
            return Optional.empty();
        }
        List<Aggregation.Call> calls = new ArrayList<>();
        for (AggregateFunc agg : aggregation.aggregateExpressions()) {
            Optional<Aggregation.Call> call = aggregate(agg, schema);
            if (call.isEmpty()) {
                return Optional.empty();
            }
            calls.add(call.get());
        }
        List<Expr.Column> groupBy = new ArrayList<>();
        for (Expression expression : aggregation.groupByExpressions()) {
            Optional<Expr.Column> column = schemaColumn(expression, schema);
            if (column.isEmpty()) {
                return Optional.empty();
            }
            groupBy.add(column.get());
        }
        return Optional.of(new Aggregation(groupBy, calls));
    }

    /** Integer digits from which a decimal column is wide enough for partial sums to stay in range. */
    public static final int WIDE_DECIMAL_DIGITS = 12;

    /**
     * Whether the aggregation sums a decimal expression that has fewer than {@code minIntegerDigits} integer digits.
     * Spark casts a partial SUM back to the input type, so such a column overflows once the total outgrows it.
     * With a minimum of 0 every decimal sum qualifies in the "never push" sense, so callers pass a large value.
     * @param aggregation Spark aggregation
     * @param schema table schema
     * @param minIntegerDigits integer digits a decimal needs to be considered safe; 0 means no decimal is safe
     * @return true when some SUM must not be pushed as a partial aggregate
     */
    public static boolean sumsDecimal(
            org.apache.spark.sql.connector.expressions.aggregate.Aggregation aggregation, StructType schema,
            int minIntegerDigits) {
        for (AggregateFunc agg : aggregation.aggregateExpressions()) {
            if (agg instanceof Sum sum
                    && numericType(sum.column(), schema).orElse(null) instanceof DecimalType decimal
                    && (minIntegerDigits == 0 || decimal.precision() - decimal.scale() < minIntegerDigits)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Translates one aggregate function.
     * @param agg Spark aggregate function
     * @param schema table schema
     * @return aggregate call
     */
    private static Optional<Aggregation.Call> aggregate(AggregateFunc agg, StructType schema) {
        return switch (agg) {
            case CountStar ignored -> Optional.of(new Aggregation.Call(Aggregation.Function.COUNT_STAR, null));
            case Count c -> c.isDistinct() ? Optional.empty()
                    : schemaColumn(c.column(), schema).map(e -> new Aggregation.Call(Aggregation.Function.COUNT, e));
            case Min m -> schemaColumn(m.column(), schema)
                    .map(e -> new Aggregation.Call(Aggregation.Function.MIN, e));
            case Max m -> schemaColumn(m.column(), schema)
                    .map(e -> new Aggregation.Call(Aggregation.Function.MAX, e));
            case Sum s -> sum(s, schema);
            default -> Optional.empty();
        };
    }

    /**
     * Translates SUM, casting decimal inputs to the precision Spark expects.
     * @param sum Spark SUM
     * @param schema table schema
     * @return aggregate call
     */
    private static Optional<Aggregation.Call> sum(Sum sum, StructType schema) {
        Optional<Expr> input = numeric(sum.column(), schema);
        Optional<DataType> type = numericType(sum.column(), schema);
        if (sum.isDistinct() || input.isEmpty() || type.isEmpty() || !safeSumType(type.get())) {
            return Optional.empty();
        }
        Expr argument = type.get() instanceof DecimalType decimal
                ? Expr.castDecimal(input.get(), decimal.precision(), decimal.scale())
                : input.get();
        return Optional.of(new Aggregation.Call(Aggregation.Function.SUM, argument));
    }

    /**
     * Resolves a single column reference against the schema, case-insensitively.
     * @param expression Spark expression
     * @param schema table schema
     * @return column with its canonical schema name
     */
    private static Optional<Expr.Column> schemaColumn(Expression expression, StructType schema) {
        if (!(expression instanceof NamedReference reference) || reference.fieldNames().length != 1) {
            return Optional.empty();
        }
        String name = reference.fieldNames()[0];
        return Arrays.stream(schema.fields())
                .map(StructField::name)
                .filter(field -> field.equalsIgnoreCase(name))
                .findFirst()
                .map(Expr.Column::new);
    }

    /**
     * Translates the numeric expression subset supported in pushed aggregates.
     * @param expression Spark expression
     * @param schema table schema
     * @return expression when every node is supported
     */
    private static Optional<Expr> numeric(Expression expression, StructType schema) {
        Optional<Expr.Column> column = schemaColumn(expression, schema);
        if (column.isPresent()) {
            return Optional.of(column.get());
        }
        if (expression instanceof Literal<?> literal) {
            return numericLiteral(literal.value());
        }
        if (expression instanceof Cast cast && isNumericType(cast.dataType())) {
            return numeric(cast.expression(), schema).map(e -> new Expr.Cast(e, cast.dataType().sql()));
        }
        if (expression instanceof GeneralScalarExpression scalar && isArithmeticOperator(scalar.name())
                && scalar.children().length == 2) {
            return both(numeric(scalar.children()[0], schema), numeric(scalar.children()[1], schema))
                    .map(p -> new Expr.Arithmetic(scalar.name(), p[0], p[1]));
        }
        return Optional.empty();
    }

    /**
     * Converts a numeric literal; other values are rejected.
     * @param value literal value
     * @return literal
     */
    private static Optional<Expr> numericLiteral(Object value) {
        Object converted = value instanceof org.apache.spark.sql.types.Decimal d ? d.toJavaBigDecimal() : value;
        if (converted instanceof BigDecimal || converted instanceof Byte || converted instanceof Short
                || converted instanceof Integer || converted instanceof Long
                || converted instanceof Float || converted instanceof Double) {
            return Expr.Literal.of(converted).map(Expr.class::cast);
        }
        return Optional.empty();
    }

    /**
     * Infers the Spark type of a supported numeric expression.
     * @param expression Spark expression
     * @param schema table schema
     * @return inferred type
     */
    private static Optional<DataType> numericType(Expression expression, StructType schema) {
        if (expression instanceof NamedReference) {
            return schemaColumn(expression, schema).flatMap(c -> Arrays.stream(schema.fields())
                    .filter(f -> f.name().equalsIgnoreCase(c.name()))
                    .map(StructField::dataType)
                    .findFirst());
        }
        if (expression instanceof Literal<?> literal) {
            return Optional.of(literal.dataType());
        }
        if (expression instanceof Cast cast && isNumericType(cast.dataType())) {
            return Optional.of(cast.dataType());
        }
        if (expression instanceof GeneralScalarExpression scalar && isArithmeticOperator(scalar.name())
                && scalar.children().length == 2) {
            Optional<DataType> left = numericType(scalar.children()[0], schema);
            Optional<DataType> right = numericType(scalar.children()[1], schema);
            if (left.isPresent() && right.isPresent()) {
                return arithmeticType(scalar.name(), left.get(), right.get());
            }
        }
        return Optional.empty();
    }

    /**
     * Infers decimal arithmetic using Spark's precision formulas.
     * @param operator arithmetic operator
     * @param left left operand type
     * @param right right operand type
     * @return arithmetic result type
     */
    private static Optional<DataType> arithmeticType(String operator, DataType left, DataType right) {
        if (!isNumericType(left) || !isNumericType(right)) {
            return Optional.empty();
        }
        if (left.equals(DataTypes.DoubleType) || right.equals(DataTypes.DoubleType)
                || left.equals(DataTypes.FloatType) || right.equals(DataTypes.FloatType)) {
            return Optional.of(DataTypes.DoubleType);
        }
        Optional<DecimalType> leftDecimal = asDecimal(left);
        Optional<DecimalType> rightDecimal = asDecimal(right);
        if (leftDecimal.isEmpty() || rightDecimal.isEmpty()) {
            return Optional.empty();
        }
        DecimalType first = leftDecimal.get();
        DecimalType second = rightDecimal.get();
        int precision;
        int scale;
        if ("*".equals(operator)) {
            precision = first.precision() + second.precision() + 1;
            scale = first.scale() + second.scale();
        } else {
            scale = Math.max(first.scale(), second.scale());
            precision = Math.max(first.precision() - first.scale(), second.precision() - second.scale()) + scale + 1;
        }
        return Optional.of(adjustPrecisionScale(precision, scale));
    }

    /**
     * Converts integral Spark types to their decimal arithmetic equivalents.
     * @param type Spark numeric type
     * @return decimal representation when supported
     */
    private static Optional<DecimalType> asDecimal(DataType type) {
        if (type instanceof DecimalType decimal) {
            return Optional.of(decimal);
        }
        if (type.equals(DataTypes.ByteType)) {
            return Optional.of(DataTypes.createDecimalType(3, 0));
        }
        if (type.equals(DataTypes.ShortType)) {
            return Optional.of(DataTypes.createDecimalType(5, 0));
        }
        if (type.equals(DataTypes.IntegerType)) {
            return Optional.of(DataTypes.createDecimalType(10, 0));
        }
        if (type.equals(DataTypes.LongType)) {
            return Optional.of(DataTypes.createDecimalType(20, 0));
        }
        return Optional.empty();
    }

    /**
     * Caps decimal precision using Spark's default precision-loss policy.
     * @param precision requested precision
     * @param scale requested scale
     * @return adjusted decimal type
     */
    private static DecimalType adjustPrecisionScale(int precision, int scale) {
        if (precision <= DecimalType.MAX_PRECISION()) {
            return DataTypes.createDecimalType(precision, scale);
        }
        int integerDigits = precision - scale;
        int minimumScale = Math.min(scale, DecimalType.MINIMUM_ADJUSTED_SCALE());
        int adjustedScale = Math.max(DecimalType.MAX_PRECISION() - integerDigits, minimumScale);
        return DataTypes.createDecimalType(DecimalType.MAX_PRECISION(), adjustedScale);
    }

    /**
     * Checks whether a data type is supported in pushed numeric expressions.
     * @param type Spark data type
     * @return true for supported numeric types
     */
    private static boolean isNumericType(DataType type) {
        return type instanceof DecimalType
                || type.equals(DataTypes.ByteType)
                || type.equals(DataTypes.ShortType)
                || type.equals(DataTypes.IntegerType)
                || type.equals(DataTypes.LongType)
                || type.equals(DataTypes.FloatType)
                || type.equals(DataTypes.DoubleType);
    }

    /**
     * Checks whether an operator has an equivalent safe server expression.
     * @param operator Spark connector scalar name
     * @return true for supported binary arithmetic
     */
    private static boolean isArithmeticOperator(String operator) {
        return "+".equals(operator) || "-".equals(operator) || "*".equals(operator);
    }

    /**
     * Checks whether the server can preserve a SUM input type exactly.
     * @param type inferred SUM input type
     * @return true for floating-point and decimal inputs
     */
    private static boolean safeSumType(DataType type) {
        return type instanceof DecimalType || type.equals(DataTypes.FloatType) || type.equals(DataTypes.DoubleType);
    }

    /**
     * Combines two optional results; empty if either side is empty.
     * @param left left result
     * @param right right result
     * @return both expressions as a two-element array
     */
    private static Optional<Expr[]> both(Optional<? extends Expr> left, Optional<? extends Expr> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Expr[] {left.get(), right.get()});
    }
}
