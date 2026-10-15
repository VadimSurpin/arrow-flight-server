package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.query.Endpoint;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Verifies Spark locality hints exposed by Flight input partitions. */
@Tag("unit")
class FlightInputPartitionTest {

    /** Verifies benchmark endpoint aliases map to their colocated Spark worker. */
    @Test
    void endpointPartitionPrefersFlightAndServerNodeHosts() {
        Endpoint endpoint = new Endpoint(
                new URI[]{URI.create("grpc+tcp://flight-server-3:32010")},
                new byte[]{1});
        FlightInputPartition partition =
                new FlightInputPartition.FlightEndpointInputPartition(
                        new Schema(List.of()), endpoint);

        assertArrayEquals(new String[]{"flight-server-3", "server-node-3"},
                partition.preferredLocations());
    }

    /** Verifies generic Flight hosts remain valid locality hints. */
    @Test
    void endpointPartitionPreservesGenericHost() {
        Endpoint endpoint = new Endpoint(
                new URI[]{URI.create("grpc+tcp://flight.example:32010")},
                new byte[]{1});
        FlightInputPartition partition =
                new FlightInputPartition.FlightEndpointInputPartition(
                        new Schema(List.of()), endpoint);

        assertArrayEquals(new String[]{"flight.example"}, partition.preferredLocations());
    }

    /** Verifies an executor parses a serialized Arrow schema only once. */
    @Test
    void cachesParsedSchemaAfterSerialization() throws Exception {
        FlightInputPartition original =
                new FlightInputPartition.FlightQueryInputPartition(
                        new Schema(List.of()), "select 1");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }

        FlightInputPartition restored;
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (FlightInputPartition) input.readObject();
        }

        assertSame(restored.getSchema(), restored.getSchema());
    }
}
