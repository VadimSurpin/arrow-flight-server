package pro.surpin.data.arrowflight.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

/** Registers the Arrow Flight connector with Trino. */
public final class FlightPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new FlightConnectorFactory());
    }
}
