package pro.surpin.data.arrowflight.starrocks;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Builds the restricted SQL text sent by a StarRocks table scan. */
public final class ScanSql {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Prevents construction of a utility class. */
    private ScanSql() { }

    /**
     * Builds a full table scan without projection or predicate pushdown.
     *
     * @param schema schema name
     * @param table table name
     * @return Flight SQL query
     */
    public static String fullTable(String schema, String table) {
        return select(schema, table, List.of());
    }

    /**
     * Builds a scan with a validated column projection.
     *
     * @param schema schema name
     * @param table table name
     * @param columns selected columns, or empty for all columns
     * @return Flight SQL query
     */
    public static String select(String schema, String table, List<String> columns) {
        Objects.requireNonNull(columns, "columns");
        String projection = columns.isEmpty() ? "*"
                : String.join(", ", columns.stream().map(ScanSql::identifier).toList());
        return "SELECT " + projection + " FROM " + identifier(schema)
                + "." + identifier(table);
    }

    /**
     * Builds a scan with a column projection and an optional pushed-down filter.
     *
     * @param schema schema name
     * @param table table name
     * @param columns selected columns, or empty for all columns
     * @param where filter to push down, or null for none
     * @return Flight SQL query
     */
    public static String select(String schema, String table, List<String> columns,
            ScanPredicate where) {
        String base = select(schema, table, columns);
        return where == null ? base : base + " WHERE " + where.toSql();
    }

    /**
     * Builds a partial-aggregate query over an optionally filtered table.
     *
     * @param schema schema name
     * @param table table name
     * @param aggregate group keys and partial aggregates
     * @param where filter to push down, or null for none
     * @return Flight SQL query
     */
    public static String select(String schema, String table, ScanAggregate aggregate,
            ScanPredicate where) {
        Objects.requireNonNull(aggregate, "aggregate");
        List<String> items = new java.util.ArrayList<>();
        aggregate.groupKeys().forEach(key -> items.add(identifier(key)));
        aggregate.partials().forEach(partial -> items.add(partial.toSql()));
        // The hint asks the server to keep every value of the column in one endpoint (see the server's CompleteGroups).
        StringBuilder sql = new StringBuilder(aggregate.completeBy() == null ? ""
                : "/*+ complete_groups(" + aggregate.completeBy() + ") */ ");
        sql.append("SELECT ").append(String.join(", ", items))
                .append(" FROM ").append(identifier(schema)).append('.').append(identifier(table));
        if (where != null) {
            sql.append(" WHERE ").append(where.toSql());
        }
        if (!aggregate.groupKeys().isEmpty()) {
            sql.append(" GROUP BY ")
                    .append(String.join(", ", aggregate.groupKeys().stream().map(ScanSql::identifier).toList()));
        }
        return sql.toString();
    }

    /**
     * Builds the query that lists the distinct values of one column of an optionally filtered table. Every endpoint
     * answers for its own files, so a value can appear once per endpoint.
     *
     * @param schema schema name
     * @param table table name
     * @param column key column
     * @param where filter to push down, or null for none
     * @return Flight SQL query
     */
    public static String distinctKeys(String schema, String table, String column, ScanPredicate where) {
        String key = identifier(column);
        StringBuilder sql = new StringBuilder("SELECT ").append(key).append(" FROM ")
                .append(identifier(schema)).append('.').append(identifier(table));
        if (where != null) {
            sql.append(" WHERE ").append(where.toSql());
        }
        return sql.append(" GROUP BY ").append(key).toString();
    }

    /**
     * Validates an identifier supported by this server's SQL parser.
     *
     * @param name identifier text
     * @return validated identifier
     */
    static String identifier(String name) {
        Objects.requireNonNull(name, "identifier");
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Unsupported SQL identifier: " + name);
        }
        return name;
    }
}
