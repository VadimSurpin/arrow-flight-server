package pro.surpin.data.arrowflight.client.model;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.query.TableScan;
import pro.surpin.data.arrowflight.client.spark.SparkPushdown;
import pro.surpin.data.arrowflight.client.spark.SparkTypes;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Spark view of a Flight table: keeps the Spark schema and delegates query planning to TableScan.
 */
public final class Table implements Serializable {
    private final TableScan scan;
    private StructType sparkSchema;

    /**
     * Wraps a scan.
     * @param scan engine-neutral scan
     * @param sparkSchema known Spark schema, or null
     */
    private Table(TableScan scan, StructType sparkSchema) {
        this.scan = scan;
        this.sparkSchema = sparkSchema;
    }

    /**
     * Get the name of the table
     * @return - the name of the table
     */
    public String getName() {
        return scan.getSource();
    }

    /**
     * Get the sql-statement for querying the table
     * @return - the physical query which will be submitted to remote flight service
     * @throws IllegalStateException if the read statement has not been prepared
     */
    public String getQueryStatement() {
        return scan.getQueryStatement();
    }

    /**
     * Get the partition queries
     * @return - the partition queries with each of which is submitted from a spark executor
     */
    public String[] getPartitionStatements() {
        return scan.getPartitionStatements();
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
        return scan.getEndpoints();
    }

    /**
     * Returns the estimated input size for this physical scan.
     *
     * @return estimated bytes, or a negative value when unavailable
     */
    public long getEstimatedBytes() {
        return scan.getEstimatedBytes();
    }

    /**
     * Returns the estimated input row count for this physical scan.
     *
     * @return estimated rows, or a negative value when unavailable
     */
    public long getEstimatedRows() {
        return scan.getEstimatedRows();
    }

    /**
     * Get the flight schema
     * @return - the flight schema
     */
    public Schema getSchema() {
        return scan.getSchema();
    }

    /**
     * Get the character for quoting columns
     * @return - the character for quoting columns
     */
    public String getColumnQuote() {
        return scan.getColumnQuote();
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
        return new Table(scan.newScan(), this.sparkSchema);
    }

    /**
     * Initialize the schema and end-points by submitting the physical query
     * @param config - the connection configuration
     */
    public void initialize(Configuration config) {
        scan.initialize(config);
        this.sparkSchema = SparkTypes.toSpark(scan.getSchema());
    }

    /**
     * Initializes result schema without allocating endpoints or server handles.
     * Used during Spark catalog/table schema inference; the real scan is planned
     * only after pushdown is known.
     *
     * @param config connection configuration
     */
    public void initializeSchema(Configuration config) {
        scan.initializeSchema(config);
        this.sparkSchema = SparkTypes.toSpark(scan.getSchema());
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
        return scan.renderer();
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
        StructField[] pushed = pushedFields == null ? new StructField[0] : pushedFields;
        List<String> columns = Arrays.stream(pushed).map(StructField::name).toList();
        Field[] fields = TableScan.mergeFields(
                Arrays.asList(SparkTypes.toFields(pushed)),
                this.sparkSchema == null ? null : Arrays.asList(SparkTypes.toFields(this.sparkSchema.fields())));
        return scan.probe(pushedFilter, columns, pushedAggregation, partitionBehavior, fields);
    }

    /**
     * Table with name
     * @param tableName - the name of a table
     * @param columnQuote - the character for quoting columns in sql statements
     * @return - a Table object
     */
    public static Table forTable(String tableName, String columnQuote) {
        return new Table(TableScan.forTable(tableName, columnQuote), null);
    }
}
