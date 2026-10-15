package pro.surpin.data.arrowflight.client.model;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import pro.surpin.data.arrowflight.client.query.QueryStatement;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.spark.SparkPushdown;
import pro.surpin.data.arrowflight.client.spark.SparkTypes;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Describes a flight table
 */
public final class Table implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    private static final String AND = " and ";

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

            this.sparkSchema = SparkTypes.toSpark(eps.getSchema());
            this.schema = eps.getSchema();
            this.endpoints = eps.getEndpoints();
            this.estimatedBytes = eps.getTotalBytes();
            this.estimatedRows = eps.getTotalRecords();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
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
            this.sparkSchema = SparkTypes.toSpark(resultSchema);
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
     * @throws IllegalArgumentException if the filter is unsupported
     */
    public String toWhereClause(Filter filter) {
        return SparkPushdown.translate(filter).map(renderer()::render).orElseThrow(() ->
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
        return SparkPushdown.translate(filter).isPresent();
    }

    /**
     * Translates a Spark V2 predicate to a SQL WHERE clause.
     *
     * @param predicate Spark V2 predicate
     * @return SQL WHERE clause string
     * @throws IllegalArgumentException if the predicate is unsupported
     */
    public String toWhereClause(Predicate predicate) {
        return SparkPushdown.translate(predicate).map(renderer()::render).orElseThrow(() ->
                new IllegalArgumentException("Unsupported Spark V2 predicate: " + predicate));
    }

    /**
     * Returns whether a Spark V2 predicate can be translated exactly.
     *
     * @param predicate Spark V2 predicate
     * @return true when the complete predicate can be evaluated by the Flight server
     */
    public boolean canPushPredicate(Predicate predicate) {
        return SparkPushdown.translate(predicate).isPresent();
    }

    /**
     * Creates the SQL renderer for this table's identifier quoting.
     *
     * @return SQL renderer
     */
    public SqlRenderer renderer() {
        return new SqlRenderer(this.columnQuote);
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
