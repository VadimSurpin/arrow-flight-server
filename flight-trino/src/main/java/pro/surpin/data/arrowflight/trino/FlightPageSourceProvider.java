package pro.surpin.data.arrowflight.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

/** Opens a page source for one Flight split. */
final class FlightPageSourceProvider implements ConnectorPageSourceProvider {
    private final FlightService service;

    /**
     * Creates a provider.
     *
     * @param service Flight server access
     */
    FlightPageSourceProvider(FlightService service) {
        this.service = service;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
                                                ConnectorSplit split, ConnectorTableHandle table,
                                                List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        List<FlightColumnHandle> flightColumns = columns.stream().map(FlightColumnHandle.class::cast).toList();
        return new FlightPageSource(service.read((FlightSplit) split), flightColumns);
    }
}
