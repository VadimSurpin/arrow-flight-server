package pro.surpin.data.arrowflight.trino.pushdown;

import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.metadata.FlightTableHandle;

import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.query.TableScan;

import java.util.List;
import java.util.ArrayList;

/** Builds the Flight server query for a Trino table scan. */
public final class FlightQueries {
    /** Prevents construction of a utility class. */
    private FlightQueries() { }

    /**
     * Builds the query for a table handle.
     *
     * @param table table handle with pushed projection and filter
     * @param columnQuote identifier quote
     * @return SQL text
     */
    public static String select(FlightTableHandle table, String columnQuote) {
        return select(table, columnQuote, null);
    }

    /**
     * Builds a scan with an additional partition predicate.
     * @param table table handle with pushed projection and filter
     * @param columnQuote identifier quote
     * @param predicate partition predicate, or null for the whole scan
     * @return SQL text
     */
    public static String select(FlightTableHandle table, String columnQuote, String predicate) {
        SqlRenderer renderer = new SqlRenderer(columnQuote);
        String source = renderer.quoteIdentifier(table.remoteSchema()) + "."
                + renderer.quoteIdentifier(table.remoteTable());
        TableScan scan = TableScan.forTable(source, columnQuote);
        List<String> columns = table.columns()
                .map(list -> list.stream().map(FlightColumnHandle::remoteName).toList())
                .orElse(List.of());
        String where = FlightFilters.toExpr(table.constraint()).map(renderer::render).orElse(null);
        if (predicate != null) {
            where = where == null ? predicate : "(" + where + ") AND (" + predicate + ")";
        }
        if (table.aggregation().isPresent()) {
            var aggregation = table.aggregation().orElseThrow();
            List<String> expressions = new ArrayList<>(aggregation.groupingColumns().stream()
                    .map(column -> renderer.quoteIdentifier(column.remoteName())).toList());
            aggregation.aggregates().forEach(aggregate -> aggregate.remoteAggregates().forEach(remote -> {
                        String input = remote.input().map(column -> column.expression() == null
                                ? renderer.quoteIdentifier(column.remoteName()) : column.expression()).orElse("*");
                        if (aggregate.filter().isPresent()) {
                            var mask = aggregate.filter().get();
                            String condition = mask.expression() == null ? renderer.quoteIdentifier(mask.remoteName()) : mask.expression();
                            input = "CASE WHEN " + condition + " THEN " + (input.equals("*") ? "1" : input) + " END";
                            if (remote.input().isPresent() && remote.input().get().type() instanceof io.trino.spi.type.DecimalType decimal) {
                                input = "CAST(" + input + " AS DECIMAL(" + decimal.getPrecision() + "," + decimal.getScale() + "))";
                            }
                        }
                        input = switch (remote.inputCast()) {
                            case NONE -> input;
                            case DOUBLE -> "CAST(" + input + " AS DOUBLE)";
                            case DECIMAL_19 -> "CAST(" + input + " AS DECIMAL(19, 0))";
                            case NEGATE -> "-(" + input + ")";
                            case CHARACTER_LENGTH -> input + ", "
                                    + ((io.trino.spi.type.CharType) remote.input().orElseThrow().type()).getLength();
                        };
                        expressions.add(remote.function().name() + "(" + input + ") AS "
                                + renderer.quoteIdentifier(remote.output().remoteName()));
                    }));
            String sql = "SELECT " + String.join(", ", expressions) + " FROM " + source;
            if (where != null) {
                sql += " WHERE " + where;
            }
            if (!aggregation.groupingColumns().isEmpty()) {
                sql += " GROUP BY " + String.join(", ", aggregation.groupingColumns().stream()
                        .map(column -> renderer.quoteIdentifier(column.remoteName())).toList());
            }
            return sql;
        }
        if (table.columns().isPresent() && table.columns().get().stream().anyMatch(column -> column.expression() != null)) {
            String projection = String.join(", ", table.columns().get().stream().map(column -> column.expression() == null
                    ? renderer.quoteIdentifier(column.remoteName())
                    : column.expression() + " AS " + renderer.quoteIdentifier(column.remoteName())).toList());
            return "SELECT " + projection + " FROM " + source + (where == null ? "" : " WHERE " + where);
        }
        scan.probe(where, columns, null, null, new Field[0]);
        return scan.getQueryStatement().trim();
    }
}
