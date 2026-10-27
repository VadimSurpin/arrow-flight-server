package pro.surpin.data.arrowflight.server.services;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A request, written as a leading SQL hint, that no value of one grouping column may be read by two endpoints:
 * {@code /*+ complete_groups(column) *}{@code /  SELECT ...}. A client that aggregates by the column inside each endpoint
 * and treats the result as final sends it; {@link QueryPlanner#plan(String, String)} then keeps the guarantee or fails.
 */
public final class CompleteGroups {
    private static final Pattern HINT = Pattern.compile(
            "^\\s*/\\*\\+\\s*complete_groups\\(\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)\\s*\\*/\\s*(.*)$",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    /**
     * A query with its hint taken off.
     *
     * @param sql the query without the hint
     * @param column the grouping column to keep whole, or null when the query had no hint
     */
    public record Request(String sql, String column) { }

    private CompleteGroups() {
    }

    /**
     * Splits the hint from a query.
     *
     * @param query query text, possibly starting with the hint
     * @return the plain query and the column named by the hint
     */
    public static Request parse(String query) {
        Matcher m = HINT.matcher(query);
        return m.matches() ? new Request(m.group(2), m.group(1)) : new Request(query, null);
    }

    /**
     * Puts the hint in front of a query.
     *
     * @param column grouping column to keep whole
     * @param sql plain query
     * @return query with the hint
     */
    public static String hint(String column, String sql) {
        return "/*+ complete_groups(" + column + ") */ " + sql;
    }
}
