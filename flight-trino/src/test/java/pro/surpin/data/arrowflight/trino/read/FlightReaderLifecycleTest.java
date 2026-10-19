package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.client.CoreFlightService;
import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.config.FlightConfig;
import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies page ownership, memory accounting and service shutdown. */
@Tag("unit")
class FlightReaderLifecycleTest {
    /** Checks that returned pages survive reuse of the stream buffers. */
    @Test
    void pagesOwnTheirValuesAndReleaseAccountedMemory() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                BigIntVector vector = new BigIntVector("id", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 42);
            vector.setValueCount(1);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                FlightService.BatchReader reader = mock(FlightService.BatchReader.class);
                when(reader.next()).thenReturn(true, true, false);
                when(reader.root()).thenReturn(root);
                FlightPageSource source = new FlightPageSource(reader,
                        List.of(new FlightColumnHandle("id", "id", BigintType.BIGINT)));
                Page first = source.getNextPage();
                assertTrue(source.getMemoryUsage() > 0);
                vector.set(0, 99);
                Page second = source.getNextPage();
                assertEquals(42, BigintType.BIGINT.getLong(first.getBlock(0), 0));
                assertEquals(99, BigintType.BIGINT.getLong(second.getBlock(0), 0));
                assertNull(source.getNextPage());
                assertEquals(0, source.getMemoryUsage());
                source.close();
                verify(reader, times(1)).close();
            }
        }
    }

    /** Checks that closing an unused service prevents later connections. */
    @Test
    void closedServiceCannotReconnect() {
        CoreFlightService service = new CoreFlightService(FlightConfig.fromProperties(Map.of(
                "flight.host", "localhost", "flight.port", "32010",
                "flight.bearer-token", "test")).toClientConfiguration());
        service.close();
        service.close();
        assertThrows(IllegalStateException.class, service::listSchemas);
        assertThrows(IllegalStateException.class, () -> service.plan("select * from s.t"));
    }
}
