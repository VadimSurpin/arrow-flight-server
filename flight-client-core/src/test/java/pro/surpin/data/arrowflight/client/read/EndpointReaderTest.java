package pro.surpin.data.arrowflight.client.read;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EndpointReaderTest {

    private final Schema schema = new Schema(List.of());
    private final Endpoint endpoint = new Endpoint(new URI[] {URI.create("grpc://h:1")}, new byte[] {1});
    private Client client;
    private Configuration config;

    @BeforeEach
    void setUp() {
        client = mock(Client.class);
        config = mock(Configuration.class);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getRetryBackoffMs()).thenReturn(0L);
    }

    /** Builds a stream whose successive batches have the given row counts. */
    private static FlightStream stream(int... rowCounts) {
        FlightStream stream = mock(FlightStream.class);
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        int[] current = {-1};
        when(stream.getRoot()).thenReturn(root);
        when(stream.next()).thenAnswer(inv -> ++current[0] < rowCounts.length);
        when(root.getRowCount()).thenAnswer(inv ->
                current[0] >= 0 && current[0] < rowCounts.length ? rowCounts[current[0]] : 0);
        return stream;
    }

    @Test
    void endpointSkipsEmptyBatchesAndClosesAtEnd() throws Exception {
        FlightStream stream = stream(0, 3, 0, 2);
        when(client.openStream(any(FlightEndpoint.class))).thenReturn(stream);
        EndpointReader reader = EndpointReader.forEndpoint(client, config, endpoint, schema);

        assertTrue(reader.nextBatch());
        assertEquals(3, reader.root().getRowCount());
        assertTrue(reader.nextBatch());
        assertEquals(2, reader.root().getRowCount());
        assertFalse(reader.nextBatch());
        assertFalse(reader.nextBatch());
        assertSame(schema, reader.plannedSchema());
        verify(stream).close();
    }

    @Test
    void endpointWithFirstBatchReturnsItFirst() throws Exception {
        FlightStream stream = stream(5);
        when(client.openStream(any(FlightEndpoint.class))).thenReturn(stream);
        EndpointReader reader = EndpointReader.forEndpoint(client, config, endpoint, schema);

        assertTrue(reader.nextBatch());
        assertEquals(5, reader.root().getRowCount());
        assertFalse(reader.nextBatch());
    }

    @Test
    void retriesOpenWithBackoff() throws Exception {
        FlightStream stream = stream(1);
        when(client.openStream(any(FlightEndpoint.class)))
                .thenThrow(new RuntimeException("down"))
                .thenReturn(stream);
        EndpointReader reader = EndpointReader.forEndpoint(client, config, endpoint, schema);

        assertTrue(reader.nextBatch());
        verify(client, times(2)).openStream(any(FlightEndpoint.class));
    }

    @Test
    void failsAfterMaxRetries() throws Exception {
        when(client.openStream(any(FlightEndpoint.class))).thenThrow(new RuntimeException("down"));
        EndpointReader reader = EndpointReader.forEndpoint(client, config, endpoint, schema);

        IOException e = assertThrows(IOException.class, reader::nextBatch);
        assertEquals("down", e.getCause().getMessage());
        verify(client, times(3)).openStream(any(FlightEndpoint.class));
    }

    @Test
    void midStreamErrorIsNotRetried() throws Exception {
        FlightStream stream = stream(2);
        when(stream.next()).thenReturn(true).thenThrow(new RuntimeException("broken"));
        when(stream.getRoot().getRowCount()).thenReturn(2);
        when(client.openStream(any(FlightEndpoint.class))).thenReturn(stream);
        EndpointReader reader = EndpointReader.forEndpoint(client, config, endpoint, schema);

        assertTrue(reader.nextBatch());
        assertThrows(IOException.class, reader::nextBatch);
        verify(client, times(1)).openStream(any(FlightEndpoint.class));
        verify(stream).close();
    }

    @Test
    void queryWithoutEndpointsHasNoData() throws Exception {
        when(client.getQueryEndpoints("q")).thenReturn(new QueryEndpoints(schema, new Endpoint[0], -1, -1));
        EndpointReader reader = EndpointReader.forQuery(client, config, "q");

        assertFalse(reader.nextBatch());
        assertSame(schema, reader.plannedSchema());
        verify(client, never()).openStream(any(FlightEndpoint.class));
    }

    @Test
    void querySkipsEndpointWithEmptyFirstBatch() throws Exception {
        FlightStream empty = stream();
        FlightStream full = stream(4);
        when(client.getQueryEndpoints("q"))
                .thenReturn(new QueryEndpoints(schema, new Endpoint[] {endpoint, endpoint}, -1, -1));
        when(client.openStream(any(FlightEndpoint.class))).thenReturn(empty, full);
        EndpointReader reader = EndpointReader.forQuery(client, config, "q");

        assertTrue(reader.nextBatch());
        verify(empty).close();
        assertFalse(reader.nextBatch());
        verify(full).close();
    }

    @Test
    void accessorsRequireOpenStream() {
        EndpointReader reader = EndpointReader.forEndpoint(client, config, endpoint, schema);
        assertThrows(IllegalStateException.class, reader::root);
        assertThrows(IllegalStateException.class, reader::streamSchema);
    }
}
