package pro.surpin.data.arrowflight.trino;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Objects;

/** Creates Arrow Flight connectors from catalog properties. */
public final class FlightConnectorFactory implements ConnectorFactory {
    /** Connector name used in the connector.name catalog property. */
    public static final String NAME = "arrow-flight";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Objects.requireNonNull(catalogName, "catalogName");
        FlightConfig flightConfig = FlightConfig.fromProperties(config);
        return new FlightConnector(new CoreFlightService(flightConfig.toClientConfiguration()),
                flightConfig.columnQuote());
    }
}
