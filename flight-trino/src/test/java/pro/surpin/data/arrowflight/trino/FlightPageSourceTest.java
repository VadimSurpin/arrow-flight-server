package pro.surpin.data.arrowflight.trino;

import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class FlightPageSourceTest {
    private static final FlightColumnHandle ID = new FlightColumnHandle("id", "ID", BigintType.BIGINT);
    private static final FlightColumnHandle NAME = new FlightColumnHandle("name", "Name", VarcharType.VARCHAR);

    private BufferAllocator allocator;
    private VectorSchemaRoot root;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        BigIntVector ids = new BigIntVector("ID", allocator);
        VarCharVector names = new VarCharVector("Name", allocator);
        ids.allocateNew(2);
        names.allocateNew(2);
        ids.set(0, 1L);
        ids.set(1, 2L);
        names.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
        names.setNull(1);
        ids.setValueCount(2);
        names.setValueCount(2);
        root = VectorSchemaRoot.of(ids, names);
        root.setRowCount(2);
    }

    @AfterEach
    void tearDown() {
        root.close();
        allocator.close();
    }

    @Test
    void convertsBatchesInRequestedColumnOrder() throws Exception {
        FlightService.BatchReader reader = mock(FlightService.BatchReader.class);
        when(reader.next()).thenReturn(true, false);
        when(reader.root()).thenReturn(root);
        FlightPageSource source = new FlightPageSource(reader, List.of(NAME, ID));

        Page page = source.getNextPage();
        assertEquals(2, page.getPositionCount());
        assertEquals("a", VarcharType.VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8());
        assertTrue(page.getBlock(0).isNull(1));
        assertEquals(2L, BigintType.BIGINT.getLong(page.getBlock(1), 1));
        assertFalse(source.isFinished());
        assertEquals(2L, source.getCompletedPositions().getAsLong());
        assertTrue(source.getCompletedBytes() > 0);
        assertTrue(source.getReadTimeNanos() >= 0);
        assertEquals(0L, source.getMemoryUsage());

        assertNull(source.getNextPage());
        assertTrue(source.isFinished());
        assertNull(source.getNextPage());
        verify(reader).close();
    }

    @Test
    void countsRowsWithoutColumns() throws Exception {
        FlightService.BatchReader reader = mock(FlightService.BatchReader.class);
        when(reader.next()).thenReturn(true, false);
        when(reader.root()).thenReturn(root);
        Page page = new FlightPageSource(reader, List.of()).getNextPage();
        assertEquals(2, page.getPositionCount());
        assertEquals(0, page.getChannelCount());
    }

    @Test
    void failsOnMissingColumn() throws Exception {
        FlightService.BatchReader reader = mock(FlightService.BatchReader.class);
        when(reader.next()).thenReturn(true);
        when(reader.root()).thenReturn(root);
        FlightColumnHandle missing = new FlightColumnHandle("x", "X", BigintType.BIGINT);
        FlightPageSource source = new FlightPageSource(reader, List.of(missing));
        assertThrows(IllegalStateException.class, source::getNextPage);
        assertTrue(source.isFinished());
        verify(reader).close();
    }

    @Test
    void wrapsStreamErrors() throws Exception {
        FlightService.BatchReader reader = mock(FlightService.BatchReader.class);
        when(reader.next()).thenThrow(new IOException("boom"));
        FlightPageSource source = new FlightPageSource(reader, List.of(ID));
        assertThrows(UncheckedIOException.class, source::getNextPage);
        assertTrue(source.isFinished());
    }

    @Test
    void providerOpensReaderForSplit() {
        FlightService service = mock(FlightService.class);
        FlightSplit split = new FlightSplit(List.of(), new byte[] {1});
        FlightService.BatchReader reader = mock(FlightService.BatchReader.class);
        when(service.read(split)).thenReturn(reader);
        List<ColumnHandle> columns = List.of(ID);
        assertTrue(new FlightPageSourceProvider(service).createPageSource(FlightTransactionHandle.INSTANCE,
                null, split, null, columns, null) instanceof FlightPageSource);
        verify(service).read(split);
    }
}
