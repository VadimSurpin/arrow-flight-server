package pro.surpin.data.arrowflight.client.spark.read;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.query.Endpoint;

/** Tests the runtime filter block a batch adds to the tickets of its end-points. */
class FlightBatchTest {

    private static Table tableWith(Endpoint... endpoints) {
        Table table = mock(Table.class);
        when(table.getPartitionStatements()).thenReturn(new String[0]);
        when(table.getEndpoints()).thenReturn(endpoints);
        when(table.getSchema()).thenReturn(new org.apache.arrow.vector.types.pojo.Schema(List.of()));
        return table;
    }

    @Test
    void addsTheBlockToEveryEndpointTicket() {
        byte[] block = RuntimeBloom.encode(List.of(RuntimeBloom.of("k", new long[] {1, 2, 3}, 10)));
        Endpoint a = new Endpoint(new URI[] {URI.create("grpc://a:1")}, new byte[] {10, 1, 7});
        Endpoint b = new Endpoint(new URI[] {URI.create("grpc://b:1")}, new byte[] {10, 1, 8});
        FlightBatch batch = new FlightBatch(mock(Configuration.class), tableWith(a, b), block);

        assertEquals(2, batch.planInputPartitions().length);
        assertArrayEquals(RuntimeBloom.appendTo(a.getTicket(), block),
                ((FlightInputPartition.FlightEndpointInputPartition) batch.planInputPartitions()[0])
                        .getEndpoint().getTicket());
    }

    @Test
    void leavesTicketsAloneWithoutABlock() {
        Endpoint a = new Endpoint(new URI[] {URI.create("grpc://a:1")}, new byte[] {10, 1, 7});
        FlightBatch batch = new FlightBatch(mock(Configuration.class), tableWith(a));

        assertArrayEquals(a.getTicket(),
                ((FlightInputPartition.FlightEndpointInputPartition) batch.planInputPartitions()[0])
                        .getEndpoint().getTicket());
    }
}
