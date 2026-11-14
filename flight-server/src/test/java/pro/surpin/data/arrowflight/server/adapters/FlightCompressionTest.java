package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.junit.jupiter.api.Test;

class FlightCompressionTest {
    private static final int ROWS = 20_000;

    private static VectorSchemaRoot sample(BufferAllocator allocator) {
        BigIntVector id = new BigIntVector("id", allocator);
        DecimalVector price = new DecimalVector("price", allocator, 15, 2);
        VarCharVector comment = new VarCharVector("comment", allocator);
        id.allocateNew(ROWS);
        price.allocateNew(ROWS);
        comment.allocateNew(ROWS);
        for (int i = 0; i < ROWS; i++) {
            id.setSafe(i, i);
            price.setSafe(i, BigDecimal.valueOf(i % 1000, 2));
            if (i % 7 != 0) {
                comment.setSafe(i, ("row comment number " + (i % 50)).getBytes());
            } else {
                comment.setNull(i);
            }
        }
        id.setValueCount(ROWS);
        price.setValueCount(ROWS);
        comment.setValueCount(ROWS);
        return new VectorSchemaRoot(List.of(id, price, comment));
    }

    private static void collect(FieldVector vector, List<ArrowBuf> out) {
        out.addAll(vector.getFieldBuffers());
        for (FieldVector child : vector.getChildrenFromFields()) {
            collect(child, out);
        }
    }

    /** Decodes every buffer of a compressed batch and compares it with the root's buffers. */
    private static void assertSameBuffers(BufferAllocator allocator, VectorSchemaRoot root, ArrowRecordBatch batch) {
        List<ArrowBuf> expected = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            collect(vector, expected);
        }
        assertEquals(expected.size(), batch.getBuffers().size());
        CompressionCodec codec = CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.ZSTD);
        for (int i = 0; i < expected.size(); i++) {
            ArrowBuf compressed = batch.getBuffers().get(i);
            compressed.getReferenceManager().retain();
            try (ArrowBuf plain = codec.decompress(allocator, compressed)) {
                ArrowBuf want = expected.get(i);
                assertEquals(want.writerIndex(), plain.writerIndex(), "length of buffer " + i);
                for (long at = 0; at < want.writerIndex(); at++) {
                    assertEquals(want.getByte(at), plain.getByte(at), "byte " + at + " of buffer " + i);
                }
            }
        }
    }

    @Test
    void parallelBatchDecompressesToTheSameBuffersAndShrinks() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = sample(allocator)) {
            long baseline = allocator.getAllocatedMemory();
            FlightCompression.StreamCompressor compressor = new FlightCompression.StreamCompressor(root, 1, pool);
            long plain;
            try (ArrowRecordBatch batch = new VectorUnloader(root, true, false).getRecordBatch()) {
                plain = batch.computeBodyLength();
            }
            compressor.prepare();
            try (ArrowRecordBatch batch = compressor.unloader().getRecordBatch()) {
                assertEquals(CompressionUtil.CodecType.ZSTD.getType(), batch.getBodyCompression().getCodec());
                assertTrue(batch.computeBodyLength() < plain / 2, "body should shrink on repetitive data");
                assertSameBuffers(allocator, root, batch);
            }
            assertTrue(compressor.preparedHits() > 0, "buffers should have been compressed ahead of the unloader");
            assertEquals(0, compressor.pendingBuffers().size());
            assertEquals(baseline, allocator.getAllocatedMemory(), "no buffer may stay referenced");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void unloaderWorksWithoutPrepare() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = sample(allocator)) {
            long baseline = allocator.getAllocatedMemory();
            FlightCompression.StreamCompressor compressor = new FlightCompression.StreamCompressor(root, 1, pool);
            try (ArrowRecordBatch batch = compressor.unloader().getRecordBatch()) {
                assertSameBuffers(allocator, root, batch);
            }
            assertTrue(compressor.preparedMisses() > 0);
            assertEquals(0, compressor.preparedHits());
            assertEquals(baseline, allocator.getAllocatedMemory());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void discardReleasesBuffersThatWereNeverSent() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = sample(allocator)) {
            long baseline = allocator.getAllocatedMemory();
            FlightCompression.StreamCompressor compressor = new FlightCompression.StreamCompressor(root, 1, pool);
            compressor.prepare();
            assertTrue(compressor.pendingBuffers().size() > 0);
            compressor.discard();
            assertEquals(0, compressor.pendingBuffers().size());
            assertEquals(baseline, allocator.getAllocatedMemory());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void repeatedBatchesKeepReusingTheSameRoot() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = sample(allocator)) {
            long baseline = allocator.getAllocatedMemory();
            FlightCompression.StreamCompressor compressor = new FlightCompression.StreamCompressor(root, 1, pool);
            for (int i = 0; i < 5; i++) {
                compressor.prepare();
                try (ArrowRecordBatch batch = compressor.unloader().getRecordBatch()) {
                    assertSameBuffers(allocator, root, batch);
                }
            }
            assertEquals(baseline, allocator.getAllocatedMemory());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void noneAndBlankLeaveTheListenerAlone() {
        assertNull(FlightCompression.apply(null, null, "none", 1, null));
        assertNull(FlightCompression.apply(null, null, "", 1, null));
        assertFalse(FlightCompression.isEnabled("none"));
        assertTrue(FlightCompression.isEnabled("zstd"));
    }

    /** Serves one compressible stream through a real Flight server and reads it back with a Flight client. */
    @Test
    void realFlightStreamIsCompressedOnTheWireAndReadsBackIdentically() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (BufferAllocator allocator = new RootAllocator();
                BufferAllocator serverAllocator = allocator.newChildAllocator("server", 0, Long.MAX_VALUE)) {
            NoOpFlightProducer producer = new NoOpFlightProducer() {
                @Override
                public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                    try (VectorSchemaRoot root = sample(serverAllocator)) {
                        listener.start(root);
                        FlightCompression.StreamCompressor compressor =
                                FlightCompression.apply(listener, root, "zstd", 1, pool);
                        assertTrue(compressor != null, "the codec must be applied to a real Flight listener");
                        compressor.prepare();
                        listener.putNext();
                        listener.completed();
                    }
                }
            };
            try (FlightServer server = FlightServer.builder(allocator,
                            Location.forGrpcInsecure("localhost", 0), producer).build().start();
                    FlightClient client = FlightClient.builder(allocator,
                            Location.forGrpcInsecure("localhost", server.getPort())).build();
                    FlightStream stream = client.getStream(new Ticket(new byte[] {1}))) {
                assertTrue(stream.next());
                assertEquals(ROWS, stream.getRoot().getRowCount());
                assertEquals(1234L, stream.getRoot().getVector("id").getObject(1234));
                assertEquals(new BigDecimal("0.77"), stream.getRoot().getVector("price").getObject(77));
                assertFalse(stream.next());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
