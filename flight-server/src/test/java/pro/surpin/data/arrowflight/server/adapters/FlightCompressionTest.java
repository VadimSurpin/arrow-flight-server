package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Tests optional compression of the Arrow batches a Flight stream sends. */
class FlightCompressionTest {

    private static VectorSchemaRoot repetitiveRoot(RootAllocator allocator, int rows) {
        BigIntVector vector = new BigIntVector("v", allocator);
        vector.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
            vector.setSafe(i, i % 4);
        }
        vector.setValueCount(rows);
        return VectorSchemaRoot.of(vector);
    }

    /** A codec must shrink a repetitive batch and record the codec in the batch header. */
    @Test
    @Tag("unit")
    void unloaderCompressesRepetitiveBatches() {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = repetitiveRoot(allocator, 100_000)) {
            long plain;
            try (ArrowRecordBatch batch = new org.apache.arrow.vector.VectorUnloader(root).getRecordBatch()) {
                plain = batch.computeBodyLength();
            }
            for (String codec : List.of("lz4", "zstd")) {
                try (ArrowRecordBatch batch = FlightCompression.unloader(root, codec).getRecordBatch()) {
                    assertTrue(batch.computeBodyLength() < plain / 4, codec + " " + batch.computeBodyLength());
                    assertTrue(batch.getBodyCompression().getCodec() != CompressionUtil.CodecType.NO_COMPRESSION.getType(),
                            codec);
                }
            }
        }
    }

    @Test
    @Tag("unit")
    void unloaderRejectsUnknownCodecs() {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = repetitiveRoot(allocator, 10)) {
            assertThrows(IllegalArgumentException.class, () -> FlightCompression.unloader(root, "gzip"));
        }
    }

    /** Disabled, or not a real Flight listener (mocks): nothing is replaced and nothing breaks. */
    @Test
    @Tag("unit")
    void applyDoesNothingWhenDisabledOrForForeignListeners() {
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = repetitiveRoot(allocator, 10)) {
            FlightProducer.ServerStreamListener foreign = mock(FlightProducer.ServerStreamListener.class);
            assertFalse(FlightCompression.apply(foreign, root, "none"));
            assertFalse(FlightCompression.apply(foreign, root, null));
            assertFalse(FlightCompression.apply(foreign, root, "zstd"));
        }
    }

    private static List<String> readAll(FlightSqlClient client, String sql) throws Exception {
        FlightInfo info = client.execute(sql);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        StringBuilder row = new StringBuilder();
                        for (int c = 0; c < root.getFieldVectors().size(); c++) {
                            row.append(root.getFieldVectors().get(c).getObject(r)).append('|');
                        }
                        rows.add(row.toString());
                    }
                }
            }
        }
        rows.sort(null);
        return rows;
    }

    /** A real server and client: the same rows come back, and the streams really were compressed. */
    @Test
    @Tag("integration")
    void compressedStreamsReturnTheSameRows() throws Exception {
        String sql = "SELECT id, int_col, double_col, string_col FROM test_schema.test_table";
        List<String> plain;
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().start()) {
            plain = readAll(helper.sqlClient(), sql);
        }
        assertFalse(plain.isEmpty());
        for (String codec : List.of("lz4", "zstd")) {
            System.setProperty("flightCompression", codec);
            try (TestFlightServerHelper helper = TestFlightServerHelper.builder().start()) {
                long before = FlightCompression.appliedStreams();
                List<String> compressed = readAll(helper.sqlClient(), sql);
                assertEquals(plain, compressed, codec);
                assertTrue(FlightCompression.appliedStreams() > before, codec);
            } finally {
                System.clearProperty("flightCompression");
            }
        }
    }
}
