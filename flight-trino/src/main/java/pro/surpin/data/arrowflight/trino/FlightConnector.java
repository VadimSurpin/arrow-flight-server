package pro.surpin.data.arrowflight.trino;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.config.FlightPartitioning;
import pro.surpin.data.arrowflight.trino.metadata.FlightMetadata;
import pro.surpin.data.arrowflight.trino.metadata.FlightTransactionHandle;
import pro.surpin.data.arrowflight.trino.read.FlightPageSourceProvider;
import pro.surpin.data.arrowflight.trino.read.FlightSplitManager;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Objects;

/** Read-only Trino connector backed by an Arrow Flight SQL server. */
public final class FlightConnector implements Connector {
    private final FlightService service;
    private final String columnQuote;
    private final FlightPartitioning partitioning;

    /**
     * Creates a connector.
     *
     * @param service Flight server access
     * @param columnQuote identifier quote used in generated SQL
     */
    public FlightConnector(FlightService service, String columnQuote) {
        this(service, columnQuote, FlightPartitioning.NONE);
    }

    /**
     * Creates a connector with optional read partitioning.
     * @param service Flight server access
     * @param columnQuote identifier quote
     * @param partitioning read partition settings
     */
    public FlightConnector(FlightService service, String columnQuote, FlightPartitioning partitioning) {
        this.service = Objects.requireNonNull(service, "service");
        this.columnQuote = Objects.requireNonNull(columnQuote, "columnQuote");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly,
                                                       boolean autoCommit) {
        return FlightTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
        return new FlightMetadata(service, columnQuote);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new FlightSplitManager(service, columnQuote, partitioning);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new FlightPageSourceProvider(service);
    }

    @Override
    public void shutdown() {
        service.close();
    }
}
