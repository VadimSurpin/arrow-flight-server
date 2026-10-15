package pro.surpin.data.arrowflight.client.expr;

import pro.surpin.data.arrowflight.client.query.PushAggregation;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Renders engine-neutral expressions into the SQL dialect of the Flight server.
 */
public final class SqlRenderer {
    private static final String IS_NULL = " is null";
    private static final String IS_NOT_NULL = " is not null";
    private static final String AND = " and ";

    private final String columnQuote;

    /**
     * Creates a renderer.
     * @param columnQuote identifier quote, or null/empty to leave identifiers unquoted
     */
    public SqlRenderer(String columnQuote) {
        this.columnQuote = columnQuote;
    }

    /**
     * Renders an expression.
     * @param expr expression
     * @return SQL text
     */
    public String render(Expr expr) {
        return switch (expr) {
            case Expr.Column c -> quoteIdentifier(c.name());
            case Expr.Literal l -> literal(l.value());
            case Expr.Compare c -> compare(c);
            case Expr.And a -> "(" + render(a.left()) + AND + render(a.right()) + ")";
            case Expr.Or o -> "(" + render(o.left()) + " or " + render(o.right()) + ")";
            case Expr.Not n -> "not (" + render(n.child()) + ")";
            case Expr.IsNull n -> render(n.operand()) + (n.negated() ? IS_NOT_NULL : IS_NULL);
            case Expr.In in -> render(in.operand()) + " in ("
                    + String.join(",", in.values().stream().map(this::render).toList()) + ")";
            case Expr.Like like -> like(like);
            case Expr.Constant c -> c.value() ? "(1 = 1)" : "(1 = 0)";
            case Expr.Arithmetic a -> "(" + render(a.left()) + " " + a.op() + " " + render(a.right()) + ")";
            case Expr.Cast c -> "cast(" + render(c.child()) + " as " + c.sqlType() + ")";
        };
    }

    /**
     * Renders an aggregation as select expressions and grouping columns.
     * @param aggregation aggregation; group-by columns come first in the select list
     * @return pushed aggregation
     */
    public PushAggregation render(Aggregation aggregation) {
        String[] groupBy = aggregation.groupBy().stream().map(this::render).toArray(String[]::new);
        List<String> select = new ArrayList<>(List.of(groupBy));
        for (Aggregation.Call call : aggregation.aggregates()) {
            select.add(call.function() == Aggregation.Function.COUNT_STAR
                    ? "count(*)"
                    : call.function().name().toLowerCase(Locale.ROOT) + "(" + render(call.argument()) + ")");
        }
        String[] columns = select.toArray(new String[0]);
        return groupBy.length > 0 ? new PushAggregation(columns, groupBy) : new PushAggregation(columns);
    }

    /**
     * Quotes an identifier, doubling embedded quote characters.
     * @param identifier identifier
     * @return quoted identifier
     */
    public String quoteIdentifier(String identifier) {
        if (columnQuote == null || columnQuote.isEmpty()) {
            return identifier;
        }
        return columnQuote + identifier.replace(columnQuote, columnQuote + columnQuote) + columnQuote;
    }

    /**
     * Renders a comparison; null-safe equality is expanded into plain SQL.
     * @param c comparison
     * @return SQL text
     */
    private String compare(Expr.Compare c) {
        String left = render(c.left());
        String right = render(c.right());
        if (c.op() == Expr.CompareOp.NULL_SAFE_EQ) {
            return "((" + left + IS_NOT_NULL + AND + right + IS_NOT_NULL + AND + left + " = " + right
                    + ") or (" + left + IS_NULL + AND + right + IS_NULL + "))";
        }
        return left + " " + c.op().symbol() + " " + right;
    }

    /**
     * Renders a LIKE predicate with escaped wildcards.
     * @param like like predicate
     * @return SQL text
     */
    private String like(Expr.Like like) {
        String escaped = like.value().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
        String pattern = switch (like.kind()) {
            case STARTS_WITH -> escaped + "%";
            case ENDS_WITH -> "%" + escaped;
            case CONTAINS -> "%" + escaped + "%";
        };
        return render(like.column()) + " like " + quoteString(pattern) + " escape '\\'";
    }

    /**
     * Renders a literal value.
     * @param value supported literal value
     * @return SQL literal
     */
    private static String literal(Object value) {
        return switch (value) {
            case null -> "null";
            case BigDecimal d -> d.toPlainString();
            case Number n -> n.toString();
            case Boolean b -> b.toString();
            case LocalDateTime t -> quoteString(t.toString().replace('T', ' '));
            default -> quoteString(value.toString());
        };
    }

    /**
     * Quotes a string literal.
     * @param value string value
     * @return quoted literal
     */
    private static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
