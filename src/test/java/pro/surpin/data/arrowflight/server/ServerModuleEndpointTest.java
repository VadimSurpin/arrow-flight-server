package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/** Verifies metadata endpoints advertise the configured server address. */
@Tag("unit")
class ServerModuleEndpointTest {

    /** Checks that remote clients receive a routable endpoint and its transport scheme. */
    @Test
    void metadataEndpointsUseAdvertisedLocation() {
        for (String advertisedUri : List.of(
                "grpc+tcp://flight.example:32010", "grpc+tls://flight.example:32010")) {
            ServerModule module = new ServerModule(
                    new String[]{"flight.example"}, advertisedUri,
                    "flight.example", new Configuration());
            FlightSqlProducer producer = module.producer(
                    mock(BufferAllocator.class), mock(MetadataService.class),
                    mock(QueryPlanner.class), mock(ExecutionService.class),
                    mock(ClusterService.class));

            FlightInfo info = producer.getFlightInfoCatalogs(
                    FlightSql.CommandGetCatalogs.getDefaultInstance(), null,
                    FlightDescriptor.command(new byte[]{1}));

            assertEquals(URI.create(advertisedUri),
                    info.getEndpoints().get(0).getLocations().get(0).getUri());
        }
    }
}
