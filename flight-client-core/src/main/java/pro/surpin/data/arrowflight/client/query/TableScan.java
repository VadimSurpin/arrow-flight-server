package pro.surpin.data.arrowflight.client.query;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Engine-neutral scan of one Flight table or sub-query: builds the pushed-down SQL,
 * optional partition queries, and asks the Flight server for schema and endpoints.
 */
public final class TableScan implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableScan.class);
    private static final String AND = " and ";

    /** Holds the generated select and grouping clauses. */
    private record QueryParts(String select, String groupBy, boolean aggregationWithoutGroupBy) {
    }

    private final String source;
    private final String columnQuote;
    private QueryStatement stmt;
    private final List<String> partitionStmts = new ArrayList<>();

    private transient Schema schema;
    private Endpoint[] endpoints = new Endpoint[0];
    private long estimatedBytes = -1L;
    private long estimatedRows = -1L;

    /**
     * Creates a scan of the whole source.
     * @param source table name or parenthesized sub-query with alias
     * @param columnQuote identifier quote used in generated SQL
     */
    private TableScan(String source, String columnQuote) {
        this.source = source;
        this.columnQuote = columnQuote;
        prepare(null, List.of(), null, null, new Field[0]);
    }

    /**
     * Creates a scan for a table name or a SELECT statement; a statement is wrapped as a sub-query.
     * @param tableOrQuery table name or SELECT statement
     * @param columnQuote identifier quote used in generated SQL
     * @return scan
     */
    public static TableScan forTable(String tableOrQuery, String columnQuote) {
        String normalized = tableOrQuery.replace('\r', ' ').replace('\n', ' ').trim().toLowerCase(Locale.ROOT);
        boolean isQuery = normalized.startsWith("select ") && normalized.contains(" from ");
        return new TableScan(isQuery ? String.format("(%s) t", tableOrQuery) : tableOrQuery, columnQuote);
    }

    /**
     * Creates an independent scan of the same source with fresh pushdown state.
     * @return new scan
     */
    public TableScan newScan() {
        return new TableScan(this.source, this.columnQuote);
    }

    /**
     * Returns the source used in the FROM clause.
     * @return table name or sub-query
     */
    public String getSource() {
        return source;
    }

    /**
     * Returns the identifier quote.
     * @return quote string
     */
    public String getColumnQuote() {
        return columnQuote;
    }

    /**
     * Creates a SQL renderer using this scan's identifier quote.
     * @return renderer
     */
    public SqlRenderer renderer() {
        return new SqlRenderer(columnQuote);
    }

    /**
     * Returns the query sent to the Flight server.
     * @return SQL statement
     * @throws IllegalStateException if the statement has not been prepared
     */
    public String getQueryStatement() {
        if (this.stmt == null) {
            throw new IllegalStateException("The read statement is not valid.");
        }
        return this.stmt.getStatement();
    }

    /**
     * Returns the partition queries, each read by one engine task.
     * @return partition statements, empty when partitioning is off
     */
    public String[] getPartitionStatements() {
        return partitionStmts.toArray(new String[0]);
    }

    /**
     * Returns the Arrow schema of the result.
     * @return schema, or null before initialization
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns the endpoints of the planned query.
     * @return endpoints, empty before initialization
     */
    public Endpoint[] getEndpoints() {
        return endpoints;
    }

    /**
     * Returns the estimated input size.
     * @return bytes, or a negative value when unknown
     */
    public long getEstimatedBytes() {
        return estimatedBytes;
    }

    /**
     * Returns the estimated input row count.
     * @return rows, or a negative value when unknown
     */
    public long getEstimatedRows() {
        return estimatedRows;
    }

    /**
     * Applies pushed-down filter, projection and aggregation.
     * @param where rendered WHERE clause, or null/empty
     * @param columns projected column names; empty means all columns
     * @param aggregation rendered aggregation, or null
     * @param partitionBehavior partitioning settings, or null
     * @param fields known table fields; used to pick range partitioning by column type
     * @return true when the query changed and the scan must be re-planned
     */
    public boolean probe(String where, List<String> columns, PushAggregation aggregation,
                         PartitionBehavior partitionBehavior, Field[] fields) {
        if ((where == null || where.isEmpty()) && columns.isEmpty() && aggregation == null) {
            return false;
        }
        return prepare(aggregation, columns, where, partitionBehavior, fields);
    }

    /**
     * Plans the query on the Flight server and stores schema, endpoints and estimates.
     * @param config connection configuration
     * @throws IllegalStateException if the server call fails
     */
    public void initialize(Configuration config) {
        LOGGER.debug("TableScan.initialize(): config: {}", config);
        try {
            QueryEndpoints eps = Client.getOrCreate(config).getQueryEndpoints(getQueryStatement());
            LOGGER.debug("TableScan.initialize(): endpoints: {}", eps);
            this.schema = eps.getSchema();
            this.endpoints = eps.getEndpoints();
            this.estimatedBytes = eps.getTotalBytes();
            this.estimatedRows = eps.getTotalRecords();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Fetches the result schema without allocating endpoints or server handles.
     * @param config connection configuration
     * @throws IllegalStateException if the server call fails
     */
    public void initializeSchema(Configuration config) {
        LOGGER.debug("TableScan.initializeSchema(): config: {}", config);
        try {
            this.schema = Client.getOrCreate(config).getQuerySchema(getQueryStatement());
            this.endpoints = new Endpoint[0];
            this.estimatedBytes = -1L;
            this.estimatedRows = -1L;
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Builds the query statement and partition queries.
     * @param aggregation optional aggregation
     * @param columns projected columns
     * @param filter filter clause
     * @param partitionBehavior partitioning settings
     * @param fields known table fields
     * @return true if the query changed
     */
    private boolean prepare(PushAggregation aggregation, List<String> columns, String filter,
                            PartitionBehavior partitionBehavior, Field[] fields) {
        QueryParts parts = buildQueryParts(aggregation, columns);
        QueryStatement query = new QueryStatement(parts.select(), filter, parts.groupBy());
        boolean changed = query.different(this.stmt);
        if (changed) {
            this.stmt = query;
        }
        this.partitionStmts.clear();
        if (!parts.aggregationWithoutGroupBy()
                && partitionBehavior != null && partitionBehavior.enabled().booleanValue()) {
            preparePartitionStatements(parts, filter, partitionBehavior, fields);
        }
        return changed;
    }

    /**
     * Builds the select list and grouping clause.
     * @param aggregation optional aggregation
     * @param columns projected columns
     * @return query parts
     */
    private QueryParts buildQueryParts(PushAggregation aggregation, List<String> columns) {
        if (aggregation != null) {
            String[] groupBy = aggregation.getGroupByColumns();
            boolean noGroupBy = groupBy == null || groupBy.length == 0;
            return new QueryParts(
                    String.format("select %s from %s", String.join(",", aggregation.getColumnExpressions()), source),
                    noGroupBy ? "" : String.join(",", groupBy),
                    noGroupBy);
        }
        String select = columns.isEmpty()
                ? String.format("select * from %s", source)
                : String.format("select %s from %s", String.join(",", columns.stream()
                        .map(column -> columnQuote + column + columnQuote).toList()), source);
        return new QueryParts(select, "", false);
    }

    /**
     * Builds one query per partition predicate.
     * @param parts select and grouping parts
     * @param filter filter clause
     * @param partitionBehavior partitioning settings
     * @param fields known table fields
     */
    private void preparePartitionStatements(QueryParts parts, String filter,
                                            PartitionBehavior partitionBehavior, Field[] fields) {
        String where = filter == null || filter.isEmpty() ? "" : String.format("(%s)%s", filter, AND);
        String[] predicates = partitionBehavior.predicateDefined().booleanValue()
                ? partitionBehavior.getPredicates()
                : partitionBehavior.calculatePredicates(fields);
        for (String predicate : predicates) {
            QueryStatement query = new QueryStatement(parts.select(),
                    String.format("%s(%s)", where, predicate), parts.groupBy());
            this.partitionStmts.add(query.getStatement());
        }
    }

    /**
     * Merges field lists by case-sensitive name; later lists win.
     * @param lists field lists, nulls ignored
     * @return merged fields in first-seen order
     */
    @SafeVarargs
    public static Field[] mergeFields(Collection<Field>... lists) {
        Map<String, Field> merged = new LinkedHashMap<>();
        for (Collection<Field> list : lists) {
            if (list != null) {
                list.forEach(f -> merged.put(f.getName(), f));
            }
        }
        return merged.values().toArray(new Field[0]);
    }
}
