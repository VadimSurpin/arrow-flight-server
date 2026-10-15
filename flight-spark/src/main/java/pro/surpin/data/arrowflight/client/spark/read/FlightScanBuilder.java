package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Build flight scans which supports pushed-down filter, fields & aggregates
 */
public final class FlightScanBuilder implements ScanBuilder, SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns, SupportsPushDownAggregates {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);

    private final Configuration configuration;
    private final Table table;
    private final PartitionBehavior partitionBehavior;

    //the pushed-down filters
    private Filter[] pdFilters = new Filter[0];
    //the pushed-down V2 predicates
    private Predicate[] pdPredicates = new Predicate[0];
    //the pushed-down columns
    private StructField[] pdColumns = new StructField[0];

    //pushed-down aggregation
    private PushAggregation pdAggregation = null;

    /**
     * Construct a flight-scan builder
     * @param configuration - the configuration of remote flight-service
     * @param table - the table instance representing a table in remote flight-service
     * @param partitionBehavior - the partitioning behavior when loading data from remote flight service
     */
    public FlightScanBuilder(Configuration configuration, Table table, PartitionBehavior partitionBehavior) {
        LOGGER.debug("{}()", this.getClass().getName());
        this.configuration = configuration;
        this.table = table;
        this.partitionBehavior = partitionBehavior;
    }

    /**
     * Collection aggregations that will be pushed down
     * @param aggregation - the pushed aggregation
     * @return - pushed aggregation
     */
    @Override
    @SuppressWarnings("java:S3776")
    public boolean pushAggregation(Aggregation aggregation) {
        LOGGER.debug("{}.pushAggregation()", this.getClass().getName());

        // An aggregation with neither aggregate functions nor grouping keys has
        // nothing to push. Spark can offer this on a re-optimization pass for a
        // global aggregate; accepting it would render "select  from ..." (empty
        // projection) and fail server-side parsing. Decline so Spark keeps the
        // plain scan and applies the aggregate itself.
        if (aggregation.aggregateExpressions().length == 0
                && aggregation.groupByExpressions().length == 0) {
            this.pdAggregation = null;
            return false;
        }

        List<String> pdAggregateColumns = new ArrayList<>();
        for (AggregateFunc agg : aggregation.aggregateExpressions()) {
            if (agg instanceof CountStar) {
                pdAggregateColumns.add("count(*)");
            } else if (agg instanceof Count) {
                Count c = (Count) agg;
                Optional<String> column = simpleColumn(c.column());
                if (c.isDistinct() || column.isEmpty()) {
                    this.pdAggregation = null;
                    return false;
                }
                pdAggregateColumns.add(String.format("count(%s)", quote(column.get())));
            } else if (agg instanceof Min) {
                Min m = (Min) agg;
                Optional<String> column = simpleColumn(m.column());
                if (column.isEmpty()) {
                    this.pdAggregation = null;
                    return false;
                }
                pdAggregateColumns.add(String.format("min(%s)", quote(column.get())));
            } else if (agg instanceof Max) {
                Max m = (Max) agg;
                Optional<String> column = simpleColumn(m.column());
                if (column.isEmpty()) {
                    this.pdAggregation = null;
                    return false;
                }
                pdAggregateColumns.add(String.format("max(%s)", quote(column.get())));
            } else if (agg instanceof Sum) {
                Sum s = (Sum) agg;
                Optional<String> input = renderExpression(s.column());
                Optional<DataType> inputType = expressionType(s.column());
                if (s.isDistinct() || input.isEmpty() || inputType.isEmpty()
                        || !safeSumType(inputType.get())) {
                    this.pdAggregation = null;
                    return false;
                }
                String sumInput = input.get();
                if (inputType.get() instanceof DecimalType decimal) {
                    sumInput = String.format("cast(%s as decimal(%d,%d))",
                            sumInput, decimal.precision(), decimal.scale());
                }
                pdAggregateColumns.add(String.format("sum(%s)", sumInput));
            } else {
                this.pdAggregation = null;
                return false;
            }
        }

        String[] pdGroupByColumns = new String[aggregation.groupByExpressions().length];
        for (int i = 0; i < aggregation.groupByExpressions().length; i++) {
            Optional<String> column = simpleColumn(aggregation.groupByExpressions()[i]);
            if (column.isEmpty()) {
                this.pdAggregation = null;
                return false;
            }
            pdGroupByColumns[i] = quote(column.get());
        }
        pdAggregateColumns.addAll(0, Arrays.asList(pdGroupByColumns));
        this.pdAggregation = pdGroupByColumns.length > 0
                ? new PushAggregation(pdAggregateColumns.toArray(new String[0]), pdGroupByColumns)
                : new PushAggregation(pdAggregateColumns.toArray(new String[0]));
        return true;
    }

    /**
     * Resolves a single column reference against the table schema.
     *
     * @param expression Spark connector expression
     * @return canonical column name when the expression is a known column
     */
    private Optional<String> simpleColumn(Expression expression) {
        if (!(expression instanceof NamedReference reference)
                || reference.fieldNames().length != 1) {
            return Optional.empty();
        }
        String name = reference.fieldNames()[0];
        return Arrays.stream(this.table.getSparkSchema().fields())
                .map(StructField::name)
                .filter(field -> field.equalsIgnoreCase(name))
                .findFirst();
    }

    /**
     * Renders the numeric Spark expression subset supported by DuckDB aggregation.
     *
     * @param expression Spark connector expression
     * @return safe SQL expression when every node is supported
     */
    private Optional<String> renderExpression(Expression expression) {
        Optional<String> column = simpleColumn(expression);
        if (column.isPresent()) {
            return Optional.of(quote(column.get()));
        }
        if (expression instanceof Literal<?> literal) {
            return renderLiteral(literal.value());
        }
        if (expression instanceof Cast cast && isNumericType(cast.dataType())) {
            return renderExpression(cast.expression())
                    .map(sql -> String.format("cast(%s as %s)", sql, cast.dataType().sql()));
        }
        if (expression instanceof GeneralScalarExpression scalar
                && isArithmeticOperator(scalar.name())
                && scalar.children().length == 2) {
            Optional<String> left = renderExpression(scalar.children()[0]);
            Optional<String> right = renderExpression(scalar.children()[1]);
            if (left.isPresent() && right.isPresent()) {
                return Optional.of(String.format("(%s %s %s)",
                        left.get(), scalar.name(), right.get()));
            }
        }
        return Optional.empty();
    }

    /**
     * Renders a numeric literal without allowing arbitrary SQL text.
     *
     * @param value literal value
     * @return SQL literal when the value is numeric
     */
    private static Optional<String> renderLiteral(Object value) {
        if (value instanceof BigDecimal decimal) {
            return Optional.of(decimal.toPlainString());
        }
        if (value instanceof org.apache.spark.sql.types.Decimal decimal) {
            return Optional.of(decimal.toPlainString());
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return Optional.of(value.toString());
        }
        if (value instanceof Float number && Float.isFinite(number)) {
            return Optional.of(number.toString());
        }
        if (value instanceof Double number && Double.isFinite(number)) {
            return Optional.of(number.toString());
        }
        return Optional.empty();
    }

    /**
     * Infers the Spark numeric type of a supported connector expression.
     *
     * @param expression Spark connector expression
     * @return inferred expression type
     */
    private Optional<DataType> expressionType(Expression expression) {
        if (expression instanceof NamedReference) {
            Optional<String> column = simpleColumn(expression);
            return column.flatMap(this::columnType);
        }
        if (expression instanceof Literal<?> literal) {
            return Optional.of(literal.dataType());
        }
        if (expression instanceof Cast cast && isNumericType(cast.dataType())) {
            return Optional.of(cast.dataType());
        }
        if (expression instanceof GeneralScalarExpression scalar
                && isArithmeticOperator(scalar.name())
                && scalar.children().length == 2) {
            Optional<DataType> left = expressionType(scalar.children()[0]);
            Optional<DataType> right = expressionType(scalar.children()[1]);
            if (left.isPresent() && right.isPresent()) {
                return arithmeticType(scalar.name(), left.get(), right.get());
            }
        }
        return Optional.empty();
    }

    /**
     * Resolves a table column data type case-insensitively.
     *
     * @param column canonical or case-insensitive column name
     * @return column data type
     */
    private Optional<DataType> columnType(String column) {
        return Arrays.stream(this.table.getSparkSchema().fields())
                .filter(field -> field.name().equalsIgnoreCase(column))
                .map(StructField::dataType)
                .findFirst();
    }

    /**
     * Infers decimal arithmetic using Spark's precision formulas.
     *
     * @param operator arithmetic operator
     * @param left left operand type
     * @param right right operand type
     * @return arithmetic result type
     */
    private static Optional<DataType> arithmeticType(
            String operator, DataType left, DataType right) {
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
            precision = Math.max(first.precision() - first.scale(),
                    second.precision() - second.scale()) + scale + 1;
        }
        return Optional.of(adjustPrecisionScale(precision, scale));
    }

    /**
     * Converts integral Spark types to their decimal arithmetic equivalents.
     *
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
     *
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
        int adjustedScale = Math.max(DecimalType.MAX_PRECISION() - integerDigits,
                minimumScale);
        return DataTypes.createDecimalType(DecimalType.MAX_PRECISION(), adjustedScale);
    }

    /**
     * Checks whether a data type is supported in pushed numeric expressions.
     *
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
     * Checks whether an operator has an equivalent safe DuckDB expression.
     *
     * @param operator Spark connector scalar name
     * @return true for supported binary arithmetic
     */
    private static boolean isArithmeticOperator(String operator) {
        return "+".equals(operator) || "-".equals(operator) || "*".equals(operator);
    }

    /**
     * Checks whether the server can preserve a SUM input type exactly.
     *
     * @param type inferred SUM input type
     * @return true for floating-point and decimal inputs
     */
    private static boolean safeSumType(DataType type) {
        return type instanceof DecimalType
                || type.equals(DataTypes.FloatType)
                || type.equals(DataTypes.DoubleType);
    }

    /**
     * Quotes a column name using the remote table delimiter.
     *
     * @param column column name
     * @return quoted column name
     */
    private String quote(String column) {
        String delimiter = this.table.getColumnQuote();
        if (delimiter == null || delimiter.isEmpty()) {
            return column;
        }
        return delimiter + column.replace(delimiter, delimiter + delimiter) + delimiter;
    }

    /**
     * For SupportsPushDownFilters interface
     * @param filters - the pushed-down filters
     * @return - not-accepted filters
     */
    public Filter[] pushFilters(Filter[] filters) {
        LOGGER.debug("{}.pushFilters()", this.getClass().getName());

        List<Filter> pushed = new ArrayList<>();
        List<Filter> unhandled = new ArrayList<>();
        for (Filter filter : filters) {
            if (this.table.canPushFilter(filter)) {
                pushed.add(filter);
            } else {
                unhandled.add(filter);
            }
        }
        this.pdFilters = pushed.toArray(new Filter[0]);
        return unhandled.toArray(new Filter[0]);
    }

    /**
     * For SupportsPushDownFilters interface
     * @return - the pushed-down filters
     */
    public Filter[] pushedFilters() {
        return this.pdFilters;
    }

    /**
     * Accepts Spark V2 predicates that the Flight SQL server can evaluate exactly.
     *
     * @param predicates candidate predicates
     * @return predicates that still need evaluation in Spark
     */
    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        LOGGER.debug("{}.pushPredicates()", this.getClass().getName());

        List<Predicate> pushed = new ArrayList<>();
        List<Predicate> unhandled = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (this.table.canPushPredicate(predicate)) {
                pushed.add(predicate);
            } else {
                unhandled.add(predicate);
            }
        }
        this.pdPredicates = pushed.toArray(new Predicate[0]);
        return unhandled.toArray(new Predicate[0]);
    }

    /**
     * Returns the Spark V2 predicates accepted by this scan.
     *
     * @return pushed V2 predicates
     */
    @Override
    public Predicate[] pushedPredicates() {
        return this.pdPredicates;
    }

    /**
     * For SupportsPushDownRequiredColumns interface
     * @param columns - the schema containing the required columns
     */
    @Override
    public void pruneColumns(StructType columns) {
        this.pdColumns = columns.fields();
        LOGGER.debug("{}.pruneColumns(): columns={}", this.getClass().getName(),
                Arrays.toString(Arrays.stream(this.pdColumns)
                        .map(StructField::name).toArray(String[]::new)));
    }

    /**
     * To build a flight-scan
     * @return - A flight scan
     */
    @Override
    public Scan build() {
        LOGGER.debug("{}.build()", this.getClass().getName());

        //adjust flight-table upon pushed filters & columns
        List<String> whereParts = new ArrayList<>();
        Arrays.stream(this.pdFilters).map(this.table::toWhereClause)
                .forEach(whereParts::add);
        Arrays.stream(this.pdPredicates).map(this.table::toWhereClause)
                .forEach(whereParts::add);
        String where = String.join(" and ", whereParts);
        LOGGER.debug("{}.build(): columns={} filters={} predicates={}",
                this.getClass().getName(),
                Arrays.toString(Arrays.stream(this.pdColumns)
                        .map(StructField::name).toArray(String[]::new)),
                this.pdFilters.length, this.pdPredicates.length);
        this.table.probe(where, this.pdColumns, this.pdAggregation, this.partitionBehavior);
        // Plan exactly once, after Spark has supplied every accepted pushdown.
        // Earlier schema-only planning creates unused server handles and makes
        // Flight latency look worse than the actual scan.
        this.table.initialize(this.configuration);
        return new FlightScan(this.configuration, this.table);
    }
}
