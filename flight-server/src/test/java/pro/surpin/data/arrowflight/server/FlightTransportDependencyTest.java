package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that the resolved transport dependencies can start a Flight server. */
@Tag("unit")
class FlightTransportDependencyTest {
    /** Starts the same gRPC transport used by the production server. */
    @Test
    void startsFlightServer() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                FlightServer server = FlightServer.builder(allocator,
                        Location.forGrpcInsecure("localhost", 0), new NoOpFlightProducer() { }).build().start()) {
            assertTrue(server.getPort() > 0);
        }
    }
}
