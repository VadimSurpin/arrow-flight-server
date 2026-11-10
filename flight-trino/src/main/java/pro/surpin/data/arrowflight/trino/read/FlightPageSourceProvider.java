package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import pro.surpin.data.arrowflight.trino.metadata.FlightTableHandle;

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
import io.trino.spi.type.BigintType;

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
                throw new IllegalArgumentException("Empty count split requires a global partial count scan");
            }
            Block[] blocks = flightColumns.stream().map(column -> {
                var builder = BigintType.BIGINT.createFixedSizeBlockBuilder(1);
                BigintType.BIGINT.writeLong(builder, 0L);
                return builder.build();
            }).toArray(Block[]::new);
            return new FixedPageSource(List.of(new Page(1, blocks)));
        }
        List<String> countStates = table instanceof FlightTableHandle handle
                ? handle.aggregation().map(aggregation -> aggregation.counts().stream()
                        .map(count -> count.output().remoteName()).toList()).orElse(List.of())
                : List.of();
        return new FlightPageSource(service.read((FlightSplit) split), flightColumns, countStates);
    }
}
