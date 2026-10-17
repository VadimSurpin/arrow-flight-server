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
     * Validates an identifier supported by this server's SQL parser.
     *
     * @param name identifier text
     * @return validated identifier
     */
    private static String identifier(String name) {
        Objects.requireNonNull(name, "identifier");
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Unsupported SQL identifier: " + name);
        }
        return name;
    }
}
