package pro.surpin.data.arrowflight.client.model;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import pro.surpin.data.arrowflight.client.query.QueryStatement;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

/**
 * Describes a flight table
 */
public final class Table implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    private static final String IS_NULL = " is null";
    private static final String IS_NOT_NULL = " is not null";
    private static final String AND = " and ";
    private static final String AND_OPERATOR = "and";

    // Raw (pre-pushdown) table row counts learned from the server, keyed by table
    // name. Populated by initialize() on any scan and read by aggregate-pushdown
    // gating during a later scan's planning, before that scan has its own estimate.
    private static final java.util.concurrent.ConcurrentMap<String, Long> RAW_ROW_COUNTS =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Holds the generated select and grouping clauses. */
    private record QueryParts(String select, String groupBy, boolean aggregationWithoutGroupBy) {
    }

    //the name of a flight table whose data will be queried/updated
    private final String name;
    //the character for quoting columns in sql statements
    private final String columnQuote;

    //the read-statement
    private QueryStatement stmt;

    //the spark schema
    private StructType sparkSchema = null;
    //the flight schema
    private transient Schema schema = null;
    //the end-points exposed by the remote flight-service for fetching data of this table
    private Endpoint[] endpoints = new Endpoint[0];
    //the estimated input size exposed through FlightInfo
    private long estimatedBytes = -1L;
    //the estimated input row count exposed through FlightInfo
    private long estimatedRows = -1L;

    // Post-pushdown scan-output estimate hints, supplied by the scan builder before
    // initialize(). The server reports raw table statistics (it sums Parquet file
    // sizes and row counts regardless of pushdown); these let the reported estimate
    // reflect the pushed filter and aggregation so Spark can pick broadcast joins.
    private double pushdownSelectivity = 1.0;
    private boolean pushedGlobalAggregation = false;

    //the container for holding the partitioning queries
    private final java.util.List<String> partitionStmts = new java.util.ArrayList<>();

    /**
     * Construct a Table object
     * @param name - the name of the table
     * @param columnQuote - the character for quoting columns in sql statements
     */
    private Table(String name, String columnQuote) {
        this.name = name;
        this.columnQuote = columnQuote;

        this.prepareQueryStatement(null, null, null, null);
    }

    /**
     * Get the name of the table
     * @return - the name of the table
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the sql-statement for querying the table
     * @return - the physical query which will be submitted to remote flight service
     * @throws IllegalStateException if the read statement has not been prepared
     */
    public String getQueryStatement() {
        if (this.stmt == null) {
            throw new IllegalStateException("The read statement is not valid.");
        }
        return this.stmt.getStatement();
    }

    /**
     * Get the partition queries
     * @return - the partition queries with each of which is submitted from a spark executor
     */
    public String[] getPartitionStatements() {
        return this.partitionStmts.toArray(new String[0]);
    }

    /**
     * Get the spark schema
     * @return - the spark schema
     */
    public StructType getSparkSchema() {
        return this.sparkSchema;
    }

    /**
     * Seeds schema metadata restored from Spark's persisted catalog entry.
     * No Flight query is submitted; the actual scan still obtains its Arrow
     * schema and endpoints after pushdown has been applied.
     *
     * @param sparkSchema persisted Spark schema
     */
    public void setSparkSchema(StructType sparkSchema) {
        this.sparkSchema = sparkSchema;
    }

    /**
     * Get the end-points
     * @return - end-points exposed by the remote flight service upon submitted query
     */
    public Endpoint[] getEndpoints() {
        return this.endpoints;
    }

    /**
     * Returns the estimated input size for this physical scan.
     *
     * @return estimated bytes, or a negative value when unavailable
     */
    public long getEstimatedBytes() {
        return estimatedBytes;
    }

    /**
     * Returns the estimated input row count for this physical scan.
     *
     * @return estimated rows, or a negative value when unavailable
     */
    public long getEstimatedRows() {
        return estimatedRows;
    }

    /**
     * Get the flight schema
     * @return - the flight schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the character for quoting columns
     * @return - the character for quoting columns
     */
    public String getColumnQuote() {
        return this.columnQuote;
    }

    /**
     * Creates an independent table state for one Spark scan.
     *
     * <p>Projection and filter pushdown mutate the physical statement, schema and
     * endpoints. A relation may be planned more than once, so sharing that mutable
     * state between scans can reuse endpoints for an older query.</p>
     *
     * @return a table with the same source and quoting rules and a fresh scan state
     */
    public Table newScan() {
        Table scan = new Table(this.name, this.columnQuote);
        scan.sparkSchema = this.sparkSchema;
        return scan;
    }

    /**
     * Initialize the schema and end-points by submitting the physical query
     * @param config - the connection configuration
     */
    public void initialize(Configuration config) {
        LOGGER.debug("Table.initialize(): config: {}", config);
        try {
            Client client = Client.getOrCreate(config);
            QueryEndpoints eps = client.getQueryEndpoints(this.getQueryStatement());
            LOGGER.debug("Table.initialize(): endpoints: {}", eps);

            this.sparkSchema = new StructType(Arrays.stream(Field.from(eps.getSchema())).map(fs -> new StructField(fs.getName(), FieldType.toSpark(fs.getType()), true, Metadata.empty())).toArray(StructField[]::new));
            this.schema = eps.getSchema();
            this.endpoints = eps.getEndpoints();
            if (eps.getTotalRecords() >= 0) {
                recordRawRowCount(this.name, eps.getTotalRecords());
            }
            long[] estimate = estimateScanOutput(eps.getTotalRecords(), eps.getTotalBytes());
            this.estimatedRows = estimate[0];
            this.estimatedBytes = estimate[1];
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Records post-pushdown estimate hints used to translate the server's raw
     * table statistics into the scan's actual output size. Called by the scan
     * builder after pushdown is finalized and before {@link #initialize}.
     *
     * @param selectivity combined selectivity of pushed filters in [0, 1]
     * @param globalAggregation true when a pushed aggregation has no grouping keys
     *                          and therefore yields a single row
     */
    public void applyPushdownEstimateHints(double selectivity, boolean globalAggregation) {
        this.pushdownSelectivity = (selectivity >= 0.0 && selectivity <= 1.0) ? selectivity : 1.0;
        this.pushedGlobalAggregation = globalAggregation;
    }

    /**
     * Returns the last raw (pre-pushdown) row count learned for a table from the
     * server, or {@code null} if no scan of that table has reported one yet.
     *
     * @param tableName physical table name
     * @return cached raw row count, or null when unknown
     */
    public static Long cachedRawRowCount(String tableName) {
        return RAW_ROW_COUNTS.get(tableName);
    }

    /**
     * Records a raw (pre-pushdown) row count learned for a table. Called by
     * {@link #initialize} after each scan; exposed so callers (and tests) can
     * seed the count used by aggregate-pushdown gating.
     *
     * @param tableName physical table name
     * @param rawRows raw row count
     */
    public static void recordRawRowCount(String tableName, long rawRows) {
        RAW_ROW_COUNTS.put(tableName, rawRows);
    }

    /**
     * Translates the server's raw table statistics into the scan's estimated output
     * {rows, bytes}, applying the pushed filter selectivity and aggregation hints.
     *
     * @param rawRows raw table row count from the server, or negative if unknown
     * @param rawBytes raw table byte size from the server, or negative if unknown
     * @return two-element array: [estimatedRows, estimatedBytes]
     */
    long[] estimateScanOutput(long rawRows, long rawBytes) {
        long rows = estimateOutputRows(rawRows);
        long bytes = estimateOutputBytes(rawBytes, rawRows, rows);
        return new long[]{rows, bytes};
    }

    /**
     * Translates the server's raw row count into the scan's estimated output rows,
     * applying pushed filter selectivity and global-aggregation cardinality.
     *
     * @param rawRows raw table row count from the server, or negative if unknown
     * @return estimated output rows, or negative when the raw count is unknown
     */
    private long estimateOutputRows(long rawRows) {
        if (this.pushedGlobalAggregation) {
            return 1L;
        }
        if (rawRows < 0) {
            return rawRows;
        }
        return Math.max(1L, Math.round(rawRows * this.pushdownSelectivity));
    }

    /**
     * Scales the server's raw byte estimate by the same row reduction. Keeping the
     * server's compressed on-disk basis and scaling by the row fraction avoids the
     * over-estimation that recomputing width-from-schema (uncompressed) would cause.
     *
     * @param rawBytes raw table size from the server, or negative if unknown
     * @param rawRows raw table row count from the server
     * @param outputRows estimated output rows from {@link #estimateOutputRows}
     * @return estimated output bytes, or negative when unknown
     */
    private static long estimateOutputBytes(long rawBytes, long rawRows, long outputRows) {
        if (rawBytes < 0) {
            return rawBytes;
        }
        if (rawRows <= 0 || outputRows < 0) {
            return rawBytes;
        }
        double rowFraction = Math.min(1.0, (double) outputRows / rawRows);
        return Math.max(1L, Math.round(rawBytes * rowFraction));
    }

    /**
     * Initializes result schema without allocating endpoints or server handles.
     * Used during Spark catalog/table schema inference; the real scan is planned
     * only after pushdown is known.
     *
     * @param config connection configuration
     */
    public void initializeSchema(Configuration config) {
        LOGGER.debug("Table.initializeSchema(): config: {}", config);
        try {
            Schema resultSchema = Client.getOrCreate(config).getQuerySchema(this.getQueryStatement());
            this.sparkSchema = new StructType(Arrays.stream(Field.from(resultSchema))
                    .map(fs -> new StructField(fs.getName(), FieldType.toSpark(fs.getType()),
                            true, Metadata.empty()))
                    .toArray(StructField[]::new));
            this.schema = resultSchema;
            this.endpoints = new Endpoint[0];
            this.estimatedBytes = -1L;
            this.estimatedRows = -1L;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds or updates the query statement and partition queries.
     *
     * @param aggregation optional aggregation
     * @param fields projected fields
     * @param filter filter clause
     * @param partitionBehavior partitioning config
     * @return true if query changed
     */
    private boolean prepareQueryStatement(PushAggregation aggregation, StructField[] fields, String filter, PartitionBehavior partitionBehavior) {
        QueryParts parts = buildQueryParts(aggregation, fields);
        QueryStatement query = new QueryStatement(parts.select(), filter, parts.groupBy());
        boolean changed = query.different(this.stmt);
        if (changed) {
            this.stmt = query;
        }

        this.partitionStmts.clear();
        if (!parts.aggregationWithoutGroupBy()
                && partitionBehavior != null && partitionBehavior.enabled().booleanValue()) {
            preparePartitionStatements(parts, fields, filter, partitionBehavior);
        }
        return changed;
    }

    private QueryParts buildQueryParts(
            PushAggregation aggregation,
            StructField[] fields) {
        if (aggregation != null) {
            String[] groupBy = aggregation.getGroupByColumns();
            return new QueryParts(
                    String.format(
                            "select %s from %s",
                            String.join(",", aggregation.getColumnExpressions()),
                            this.name),
                    groupBy == null || groupBy.length == 0
                            ? "" : String.join(",", groupBy),
                    groupBy == null || groupBy.length == 0);
        }
        String select = fields == null || fields.length == 0
                ? String.format("select * from %s", this.name)
                : String.format(
                        "select %s from %s",
                        String.join(",", Arrays.stream(fields)
                                .map(column -> String.format(
                                        "%s%s%s",
                                        this.columnQuote,
                                        column.name(),
                                        this.columnQuote))
                                .toArray(String[]::new)),
                        this.name);
        return new QueryParts(select, "", false);
    }

    private void preparePartitionStatements(
            QueryParts parts,
            StructField[] fields,
            String filter,
            PartitionBehavior partitionBehavior) {
        String where = filter == null || filter.isEmpty()
                ? "" : String.format("(%s)%s", filter, AND);
        StructField[] predicateFields = this.sparkSchema == null
                ? fields : mergeFields(fields, this.sparkSchema.fields());
        String[] predicates = partitionBehavior.predicateDefined().booleanValue()
                ? partitionBehavior.getPredicates()
                : partitionBehavior.calculatePredicates(predicateFields);
        for (String predicate : predicates) {
            QueryStatement query = new QueryStatement(
                    parts.select(),
                    String.format("%s(%s)", where, predicate),
                    parts.groupBy());
            this.partitionStmts.add(query.getStatement());
        }
    }

    private static StructField[] mergeFields(
            StructField[] first,
            StructField[] second) {
        java.util.Map<String, StructField> fields = new java.util.LinkedHashMap<>();
        if (first != null) {
            for (StructField field : first) {
                fields.put(field.name(), field);
            }
        }
        if (second != null) {
            for (StructField field : second) {
                fields.put(field.name(), field);
            }
        }
        return fields.values().toArray(new StructField[0]);
    }

    /**
     * Translates a Spark Filter to a SQL WHERE clause.
     *
     * @param filter Spark filter
     * @return SQL WHERE clause string
     */
    public String toWhereClause(Filter filter) {
        return toWhereClauseIfSupported(filter).orElseThrow(() ->
                new IllegalArgumentException("Unsupported Spark filter: " + filter));
    }

    /**
     * Returns whether a Spark filter can be translated without changing its semantics.
     * Composite filters are accepted only when every child is accepted.
     *
     * @param filter Spark filter
     * @return true when the complete filter can be evaluated by the Flight server
     */
    public boolean canPushFilter(Filter filter) {
        return toWhereClauseIfSupported(filter).isPresent();
    }

    /**
     * Translates a Spark V2 predicate to a SQL WHERE clause.
     *
     * @param predicate Spark V2 predicate
     * @return SQL WHERE clause string
     * @throws IllegalArgumentException if the predicate is unsupported
     */
    public String toWhereClause(Predicate predicate) {
        return toWhereClauseIfSupported(predicate).orElseThrow(() ->
                new IllegalArgumentException("Unsupported Spark V2 predicate: " + predicate));
    }

    /**
     * Returns whether a Spark V2 predicate can be translated exactly.
     *
     * @param predicate Spark V2 predicate
     * @return true when the complete predicate can be evaluated by the Flight server
     */
    public boolean canPushPredicate(Predicate predicate) {
        return toWhereClauseIfSupported(predicate).isPresent();
    }

    /**
     * Attempts to translate a complete Spark V2 predicate.
     *
     * @param predicate Spark V2 predicate
     * @return translated SQL clause when every expression is supported
     */
    private Optional<String> toWhereClauseIfSupported(Predicate predicate) {
        if (predicate == null) {
            return Optional.empty();
        }
        String predicateName = predicate.name().toUpperCase(Locale.ROOT);
        Expression[] children = predicate.children();
        if (isComparisonPredicate(predicateName)) {
            return comparisonPredicate(predicateName, children);
        }
        return switch (predicateName) {
            case "ALWAYS_TRUE" -> fixedPredicate(children, "(1 = 1)");
            case "ALWAYS_FALSE" -> fixedPredicate(children, "(1 = 0)");
            case "AND", "OR" -> logicalPredicate(predicateName, children);
            case "NOT" -> notPredicate(children);
            case "IS_NULL", "IS_NOT_NULL" -> nullPredicate(predicateName, children);
            case "IN" -> inPredicate(children);
            case "STARTS_WITH", "ENDS_WITH", "CONTAINS" ->
                    likePredicate(predicateName, children);
            default -> Optional.empty();
        };
    }

    private Optional<String> fixedPredicate(Expression[] children, String sql) {
        return children.length == 0 ? Optional.of(sql) : Optional.empty();
    }

    private Optional<String> logicalPredicate(String name, Expression[] children) {
        if (children.length != 2 || !(children[0] instanceof Predicate left)
                || !(children[1] instanceof Predicate right)) {
            return Optional.empty();
        }
        Optional<String> leftSql = toWhereClauseIfSupported(left);
        Optional<String> rightSql = toWhereClauseIfSupported(right);
        if (leftSql.isEmpty() || rightSql.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of("(" + leftSql.get() + " "
                + name.toLowerCase(Locale.ROOT) + " " + rightSql.get() + ")");
    }

    private Optional<String> notPredicate(Expression[] children) {
        if (children.length != 1 || !(children[0] instanceof Predicate child)) {
            return Optional.empty();
        }
        return toWhereClauseIfSupported(child).map(sql -> "not (" + sql + ")");
    }

    private Optional<String> nullPredicate(String name, Expression[] children) {
        if (children.length != 1) {
            return Optional.empty();
        }
        return predicateOperand(children[0]).map(sql ->
                sql + ("IS_NULL".equals(name) ? IS_NULL : IS_NOT_NULL));
    }

    private Optional<String> comparisonPredicate(String name, Expression[] children) {
        if (children.length != 2) {
            return Optional.empty();
        }
        Optional<String> left = predicateOperand(children[0]);
        Optional<String> right = predicateOperand(children[1]);
        if (left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }
        if ("<=>".equals(name)) {
            return Optional.of("((" + left.get() + IS_NOT_NULL + AND
                    + right.get() + IS_NOT_NULL + AND + left.get() + " = "
                    + right.get() + ") or (" + left.get() + IS_NULL + AND
                    + right.get() + IS_NULL + "))");
        }
        return Optional.of(left.get() + " " + name + " " + right.get());
    }

    private Optional<String> inPredicate(Expression[] children) {
        if (children.length <= 1) {
            return Optional.empty();
        }
        Optional<String> value = predicateOperand(children[0]);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        String[] literals = new String[children.length - 1];
        for (int i = 1; i < children.length; i++) {
            if (!(children[i] instanceof Literal<?> literal)) {
                return Optional.empty();
            }
            Optional<String> sql = sqlLiteral(literal);
            if (sql.isEmpty()) {
                return Optional.empty();
            }
            literals[i - 1] = sql.get();
        }
        return Optional.of(value.get() + " in (" + String.join(",", literals) + ")");
    }

    private Optional<String> likePredicate(String name, Expression[] children) {
        if (children.length != 2 || !(children[0] instanceof NamedReference reference)
                || !(children[1] instanceof Literal<?> literal)) {
            return Optional.empty();
        }
        Optional<String> attribute = predicateReference(reference);
        Optional<String> value = stringLiteralValue(literal);
        if (attribute.isEmpty() || value.isEmpty()) {
            return Optional.empty();
        }
        String escaped = escapeLike(value.get());
        String pattern = switch (name) {
            case "STARTS_WITH" -> escaped + "%";
            case "ENDS_WITH" -> "%" + escaped;
            default -> "%" + escaped + "%";
        };
        return Optional.of(attribute.get() + " like " + quoteString(pattern)
                + " escape '\\'");
    }

    private Optional<String> predicateOperand(Expression expression) {
        if (expression instanceof NamedReference reference) {
            return predicateReference(reference);
        }
        if (expression instanceof Literal<?> literal) {
            return sqlLiteral(literal);
        }
        return Optional.empty();
    }

    /**
     * Translates a single-column Spark reference.
     *
     * @param reference Spark column reference
     * @return quoted SQL identifier when supported
     */
    private Optional<String> predicateReference(NamedReference reference) {
        String[] fields = reference.fieldNames();
        if (fields == null || fields.length != 1 || fields[0] == null
                || fields[0].isBlank()) {
            return Optional.empty();
        }
        return Optional.of(quoteIdentifier(fields[0]));
    }

    /**
     * Translates a typed Spark V2 literal.
     *
     * @param literal typed Spark literal
     * @return SQL literal when supported
     */
    private Optional<String> sqlLiteral(Literal<?> literal) {
        Object value = literal.value();
        if (value == null) {
            return Optional.of("null");
        }
        if (literal.dataType().equals(DataTypes.DateType) && value instanceof Number days) {
            return Optional.of(quoteString(LocalDate.ofEpochDay(days.longValue()).toString()));
        }
        if ((literal.dataType().equals(DataTypes.TimestampType)
                || literal.dataType().equals(DataTypes.TimestampNTZType))
                && value instanceof Number micros) {
            long epochMicros = micros.longValue();
            long seconds = Math.floorDiv(epochMicros, 1_000_000L);
            int nanos = Math.toIntExact(Math.floorMod(epochMicros, 1_000_000L) * 1_000L);
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
            return Optional.of(quoteString(timestamp.toString().replace('T', ' ')));
        }
        if (literal.dataType().equals(DataTypes.StringType)) {
            return Optional.of(quoteString(value.toString()));
        }
        if (value instanceof org.apache.spark.sql.types.Decimal decimal) {
            return Optional.of(decimal.toPlainString());
        }
        return sqlLiteral(value, true);
    }

    /**
     * Extracts a non-null Spark string literal.
     *
     * @param literal typed Spark literal
     * @return string value when supported
     */
    private static Optional<String> stringLiteralValue(Literal<?> literal) {
        if (literal.value() == null || !literal.dataType().equals(DataTypes.StringType)) {
            return Optional.empty();
        }
        return Optional.of(literal.value().toString());
    }

    /**
     * Returns whether a predicate name is a supported comparison operator.
     *
     * @param name normalized predicate name
     * @return true for supported comparisons
     */
    private static boolean isComparisonPredicate(String name) {
        return "=".equals(name) || "<>".equals(name) || "<=>".equals(name)
                || "<".equals(name) || "<=".equals(name)
                || ">".equals(name) || ">=".equals(name);
    }

    /**
     * Attempts to translate a complete Spark filter.
     *
     * @param filter Spark filter
     * @return translated SQL clause when every expression is supported
     */
    private Optional<String> toWhereClauseIfSupported(Filter filter) {
        if (filter == null) {
            return Optional.empty();
        }
        Optional<String> result = comparisonFilter(filter);
        if (result.isPresent()) {
            return result;
        }
        result = logicalFilter(filter);
        if (result.isPresent()) {
            return result;
        }
        result = unaryFilter(filter);
        if (result.isPresent()) {
            return result;
        }
        result = nullFilter(filter);
        if (result.isPresent()) {
            return result;
        }
        result = stringFilter(filter);
        return result.isPresent() ? result : inFilter(filter);
    }

    private Optional<String> comparisonFilter(Filter filter) {
        if (filter instanceof EqualTo equalTo) {
            return comparison(equalTo.attribute(), "=", equalTo.value());
        }
        if (filter instanceof EqualNullSafe equalNullSafe) {
            return equalNullSafeFilter(equalNullSafe);
        }
        if (filter instanceof LessThan lessThan) {
            return comparison(lessThan.attribute(), "<", lessThan.value());
        }
        if (filter instanceof LessThanOrEqual lessThanOrEqual) {
            return comparison(lessThanOrEqual.attribute(), "<=", lessThanOrEqual.value());
        }
        if (filter instanceof GreaterThan greaterThan) {
            return comparison(greaterThan.attribute(), ">", greaterThan.value());
        }
        if (filter instanceof GreaterThanOrEqual greaterThanOrEqual) {
            return comparison(greaterThanOrEqual.attribute(), ">=", greaterThanOrEqual.value());
        }
        return Optional.empty();
    }

    private Optional<String> equalNullSafeFilter(EqualNullSafe filter) {
        if (filter.value() == null) {
            return Optional.of(quoteIdentifier(filter.attribute()) + IS_NULL);
        }
        Optional<String> equality = comparison(filter.attribute(), "=", filter.value());
        String identifier = quoteIdentifier(filter.attribute());
        return equality.map(sql -> "(" + identifier + IS_NOT_NULL + AND + sql + ")");
    }

    private Optional<String> logicalFilter(Filter filter) {
        if (filter instanceof And and) {
            return combine(and.left(), AND_OPERATOR, and.right());
        }
        if (filter instanceof Or or) {
            return combine(or.left(), "or", or.right());
        }
        return Optional.empty();
    }

    private Optional<String> unaryFilter(Filter filter) {
        if (filter instanceof Not not) {
            return toWhereClauseIfSupported(not.child()).map(sql -> "not (" + sql + ")");
        }
        return Optional.empty();
    }

    private Optional<String> nullFilter(Filter filter) {
        if (filter instanceof IsNull isNull) {
            return Optional.of(quoteIdentifier(isNull.attribute()) + IS_NULL);
        }
        if (filter instanceof IsNotNull isNotNull) {
            return Optional.of(quoteIdentifier(isNotNull.attribute()) + IS_NOT_NULL);
        }
        return Optional.empty();
    }

    private Optional<String> stringFilter(Filter filter) {
        if (filter instanceof StringStartsWith startsWith) {
            return Optional.of(like(startsWith.attribute(), escapeLike(startsWith.value()) + "%"));
        }
        if (filter instanceof StringContains contains) {
            return Optional.of(like(contains.attribute(), "%" + escapeLike(contains.value()) + "%"));
        }
        if (filter instanceof StringEndsWith endsWith) {
            return Optional.of(like(endsWith.attribute(), "%" + escapeLike(endsWith.value())));
        }
        return Optional.empty();
    }

    private Optional<String> inFilter(Filter filter) {
        if (!(filter instanceof In in)) {
            return Optional.empty();
        }
        Object[] values = in.values();
        if (values == null || values.length == 0) {
            return Optional.of("(1 = 0)");
        }
        String[] literals = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            Optional<String> literal = sqlLiteral(values[i], true);
            if (literal.isEmpty()) {
                return Optional.empty();
            }
            literals[i] = literal.get();
        }
        return Optional.of(String.format("%s in (%s)", quoteIdentifier(in.attribute()),
                String.join(",", literals)));
    }

    private Optional<String> comparison(String attribute, String operator, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        return sqlLiteral(value, false).map(literal ->
                String.format("%s %s %s", quoteIdentifier(attribute), operator, literal));
    }

    private Optional<String> combine(Filter left, String operator, Filter right) {
        Optional<String> leftSql = toWhereClauseIfSupported(left);
        Optional<String> rightSql = toWhereClauseIfSupported(right);
        if (leftSql.isEmpty() || rightSql.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.format("(%s %s %s)", leftSql.get(), operator, rightSql.get()));
    }

    private Optional<String> sqlLiteral(Object value, boolean allowNull) {
        switch (value) {
            case null -> {
                return allowNull ? Optional.of("null") : Optional.empty();
            }
            case Double number when !Double.isFinite(number) -> {
                return Optional.empty();
            }
            case Float number when !Float.isFinite(number) -> {
                return Optional.empty();
            }
            case Number ignored1 -> {
                return Optional.of(value.toString());
            }
            case Boolean ignored -> {
                return Optional.of(value.toString());
            }
            default -> {
                if (value instanceof CharSequence || value instanceof Character
                        || value instanceof Date || value instanceof Time
                        || value instanceof Timestamp || value instanceof TemporalAccessor) {
                    return Optional.of(quoteString(value.toString()));
                }
                return Optional.empty();
            }
        }
    }

    private String quoteIdentifier(String identifier) {
        if (this.columnQuote == null || this.columnQuote.isEmpty()) {
            return identifier;
        }
        return this.columnQuote + identifier.replace(this.columnQuote, this.columnQuote + this.columnQuote) + this.columnQuote;
    }

    private static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private String like(String attribute, String pattern) {
        return quoteIdentifier(attribute) + " like " + quoteString(pattern) + " escape '\\'";
    }

    private static String escapeLike(String value) {
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    /**
     * Probe if the pushed filter, fields and aggregation would affect the existing schema & end-points
     * @param pushedFilter - the pushed filter
     * @param pushedFields - the pushed fields
     * @param pushedAggregation - the pushed aggregation
     * @param partitionBehavior - the partitioning behavior
     * @return - true if initialization is required
     */
    public Boolean probe(String pushedFilter, StructField[] pushedFields, PushAggregation pushedAggregation, PartitionBehavior partitionBehavior) {
        if ((pushedFilter == null || pushedFilter.isEmpty()) && (pushedFields == null || pushedFields.length == 0) && pushedAggregation == null) {
            return false;
        }
        return this.prepareQueryStatement(pushedAggregation, pushedFields, pushedFilter, partitionBehavior);
    }

    /**
     * Table with name
     * @param tableName - the name of a table
     * @param columnQuote - the character for quoting columns in sql statements
     * @return - a Table object
     */
    public static Table forTable(String tableName, String columnQuote) {
        java.util.function.Predicate<String> isQuery = t -> {
            String normalized = t.replace('\r', ' ').replace('\n', ' ').trim().toLowerCase();
            return normalized.startsWith("select ") && normalized.contains(" from ");
        };
        return new Table(isQuery.test(tableName) ? String.format("(%s) t", tableName) : tableName, columnQuote);
    }
}
