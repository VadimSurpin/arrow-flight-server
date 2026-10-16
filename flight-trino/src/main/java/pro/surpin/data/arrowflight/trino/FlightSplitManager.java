package pro.surpin.data.arrowflight.trino;

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

/** Plans the scan query on the Flight server and turns each endpoint into a split. */
final class FlightSplitManager implements ConnectorSplitManager {
    private final FlightService service;
    private final String columnQuote;

    /**
     * Creates a split manager.
     *
     * @param service Flight server access
     * @param columnQuote identifier quote
     */
    FlightSplitManager(FlightService service, String columnQuote) {
        this.service = service;
        this.columnQuote = columnQuote;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
                                          ConnectorTableHandle table, DynamicFilter dynamicFilter,
                                          Constraint constraint) {
        FlightTableHandle handle = (FlightTableHandle) table;
        if (handle.constraint().isNone()) {
            return FixedSplitSource.emptySplitSource();
        }
        QueryEndpoints planned = service.plan(FlightQueries.select(handle, columnQuote));
        List<FlightSplit> splits = Arrays.stream(planned.getEndpoints())
                .map(endpoint -> new FlightSplit(Arrays.stream(endpoint.getURIs()).map(URI::toString).toList(),
                        endpoint.getTicket()))
                .toList();
        return new FixedSplitSource(splits);
    }
}
