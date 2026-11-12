package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.metadata.FlightTableHandle;
import pro.surpin.data.arrowflight.trino.metadata.AggregateSpec;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.List;

/** Opens a page source for one Flight split. */
public final class FlightPageSourceProvider implements ConnectorPageSourceProvider {
    private final FlightService service;

    /**
     * Creates a provider.
     *
     * @param service Flight server access
     */
    public FlightPageSourceProvider(FlightService service) {
        this.service = service;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
                                                ConnectorSplit split, ConnectorTableHandle table,
                                                List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        List<FlightColumnHandle> flightColumns = columns.stream().map(FlightColumnHandle.class::cast).toList();
        if (((FlightSplit) split).emptyAggregation()) {
            if (!(table instanceof FlightTableHandle handle) || handle.aggregation().isEmpty()
                    || !handle.aggregation().orElseThrow().groupingColumns().isEmpty()
                    || !handle.aggregation().orElseThrow().outputColumns().containsAll(flightColumns)) {
                throw new IllegalArgumentException("Empty aggregation split requires a global partial aggregate scan");
            }
            var specifications = handle.aggregation().orElseThrow().aggregates();
            Block[] blocks = flightColumns.stream().map(column -> FlightAggregationStates.empty(specifications.stream()
                    .filter(specification -> specification.output().equals(column)).findFirst().orElseThrow()))
                    .toArray(Block[]::new);
            return new FixedPageSource(List.of(new Page(1, blocks)));
        }
        List<AggregateSpec> aggregates = table instanceof FlightTableHandle handle
                ? handle.aggregation().map(aggregation -> aggregation.aggregates()).orElse(List.of())
                : List.of();
        return new FlightPageSource(service.read((FlightSplit) split), flightColumns, aggregates);
    }
}
