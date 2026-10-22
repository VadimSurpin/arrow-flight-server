package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.config.FlightPartitioning;
import pro.surpin.data.arrowflight.trino.metadata.FlightTableHandle;
import pro.surpin.data.arrowflight.trino.pushdown.FlightQueries;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import org.apache.arrow.vector.types.pojo.Schema;

/** Plans the scan query on the Flight server and turns each endpoint into a split. */
public final class FlightSplitManager implements ConnectorSplitManager {
    private final FlightService service;
    private final String columnQuote;
    private final FlightPartitioning partitioning;

    /**
     * Creates a split manager.
     *
     * @param service Flight server access
     * @param columnQuote identifier quote
     */
    public FlightSplitManager(FlightService service, String columnQuote) {
        this(service, columnQuote, FlightPartitioning.NONE);
    }

    /**
     * Creates a split manager with optional table partitioning.
     * @param service Flight server access
     * @param columnQuote identifier quote
     * @param partitioning read partition settings
     */
    public FlightSplitManager(FlightService service, String columnQuote, FlightPartitioning partitioning) {
        this.service = service;
        this.columnQuote = columnQuote;
        this.partitioning = partitioning;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
                                          ConnectorTableHandle table, DynamicFilter dynamicFilter,
                                          Constraint constraint) {
        FlightTableHandle handle = (FlightTableHandle) table;
        if (handle.constraint().isNone()) {
            return FixedSplitSource.emptySplitSource();
        }
        List<String> queries = List.of(FlightQueries.select(handle, columnQuote));
        if (partitioning.appliesTo(handle.remoteSchema(), handle.remoteTable())) {
            Schema schema = service.listTables(handle.remoteSchema()).get(handle.remoteTable());
            if (schema == null) {
                throw new IllegalArgumentException("Partitioned table no longer exists: " + partitioning.table());
            }
            queries = partitioning.predicates(schema, columnQuote).stream()
                    .map(predicate -> FlightQueries.select(handle, columnQuote, predicate)).toList();
        }
        List<FlightSplit> splits = new ArrayList<>();
        for (String query : queries) {
            QueryEndpoints planned = service.plan(query);
            Arrays.stream(planned.getEndpoints())
                    .map(endpoint -> new FlightSplit(Arrays.stream(endpoint.getURIs()).map(URI::toString).toList(),
                            endpoint.getTicket()))
                    .forEach(splits::add);
        }
        return new FixedSplitSource(splits);
    }
}
