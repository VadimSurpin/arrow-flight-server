package pro.surpin.data.arrowflight.trino.pushdown;

import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.metadata.FlightTableHandle;

import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.query.TableScan;

import java.util.List;

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
        scan.probe(where, columns, null, null, new Field[0]);
        return scan.getQueryStatement().trim();
    }
}
