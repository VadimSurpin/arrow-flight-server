package pro.surpin.data.arrowflight.server.adapters;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional ZSTD compression of the record batches of a Flight data stream.
 *
 * <p>Arrow Flight for Java writes batches without a codec and has no option for one, but a started stream listener
 * keeps the {@link VectorUnloader} that turns each batch into its wire form in a field. That unloader is replaced by
 * one with a codec; the schema message is unaffected and readers (Arrow C++, Java, Python) decompress from the batch
 * header.
 *
 * <p>The stock unloader compresses the buffers of a batch one after another on the calling thread. A
 * {@link StreamCompressor} compresses them in parallel on a shared pool just before the batch is sent
 * ({@link StreamCompressor#prepare()}) and hands the results to the unloader, so one stream can use several cores.
 * LZ4 is not offered: the IPC format needs LZ4 <em>frames</em>, and Arrow's only implementation copies every buffer
 * through Java arrays.
 */
public final class FlightCompression {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightCompression.class);
    private static final String UNLOADER_FIELD = "unloader";
    private static final long MIN_PARALLEL_BYTES = 4096;
    private static final AtomicLong APPLIED = new AtomicLong();
    private static volatile ExecutorService sharedPool;

    private FlightCompression() { }

    /**
     * Number of streams whose batches have been switched to a codec since startup.
     *
     * @return count of compressed streams
     */
    public static long appliedStreams() {
        return APPLIED.get();
    }

    /**
     * Tells whether a configured codec name turns compression on.
     *
     * @param codec configured name
     * @return true for {@code zstd}
     */
    public static boolean isEnabled(String codec) {
        return codec != null && codec.equalsIgnoreCase("zstd");
    }

    /**
     * Applies the configured codec to a started stream.
     *
     * @param listener stream listener, already started with {@code root}
     * @param root the stream's vector root
     * @return the compressor to {@link StreamCompressor#prepare() prepare} before each batch, or null when off
     */
    public static StreamCompressor apply(FlightProducer.ServerStreamListener listener, VectorSchemaRoot root) {
        String codec = ConfigAdapter.getFlightCompression();
        if (!isEnabled(codec)) {
            return null;
        }
        return apply(listener, root, codec, ConfigAdapter.getFlightCompressionLevel(), pool());
    }

    /**
     * Applies a codec to a started stream.
     *
     * @param listener stream listener, already started with {@code root}
     * @param root the stream's vector root
     * @param codec none or zstd
     * @param level ZSTD level; negative levels trade ratio for speed
     * @param pool pool that compresses the buffers of a batch in parallel
     * @return the compressor, or null when the codec is off or the listener cannot take it
     */
    public static StreamCompressor apply(FlightProducer.ServerStreamListener listener, VectorSchemaRoot root,
            String codec, int level, ExecutorService pool) {
        if (!isEnabled(codec)) {
            return null;
        }
        StreamCompressor compressor = new StreamCompressor(root, level, pool);
        for (Class<?> type = listener.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(UNLOADER_FIELD);
                if (!VectorUnloader.class.isAssignableFrom(field.getType())) {
                    continue;
                }
                field.setAccessible(true);
                field.set(listener, compressor.unloader());
                APPLIED.incrementAndGet();
                return compressor;
            } catch (NoSuchFieldException e) {
                // keep looking in the superclass
            } catch (ReflectiveOperationException | RuntimeException e) {
                LOGGER.warn("Flight batch compression is not available: {}", e.toString());
                return null;
            }
        }
        return null;
    }

    private static ExecutorService pool() {
        ExecutorService pool = sharedPool;
        if (pool == null) {
            synchronized (FlightCompression.class) {
                pool = sharedPool;
                if (pool == null) {
                    int threads = ConfigAdapter.getFlightCompressionThreads();
                    pool = Executors.newFixedThreadPool(threads, runnable -> {
                        Thread thread = new Thread(runnable, "flight-compress");
                        thread.setDaemon(true);
                        return thread;
                    });
                    sharedPool = pool;
                    LOGGER.info("Flight batch compression: zstd level {}, {} threads",
                            ConfigAdapter.getFlightCompressionLevel(), threads);
                }
            }
        }
        return pool;
    }

    /** Compresses the buffers of the batch in a stream's root in parallel and feeds them to a {@link VectorUnloader}. */
    public static final class StreamCompressor {
        private final VectorSchemaRoot root;
        private final ExecutorService pool;
        private final CompressionCodec delegate;
        private final VectorUnloader unloader;
        private final Map<ArrowBuf, Future<ArrowBuf>> prepared = new IdentityHashMap<>();
        private final AtomicLong hits = new AtomicLong();
        private final AtomicLong misses = new AtomicLong();

        /**
         * Creates the compressor of one stream.
         *
         * @param root the stream's vector root
         * @param level ZSTD level
         * @param pool pool for parallel compression
         */
        public StreamCompressor(VectorSchemaRoot root, int level, ExecutorService pool) {
            this.root = root;
            this.pool = pool;
            this.delegate = CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.ZSTD, level);
            this.unloader = new VectorUnloader(root, true, new PreparedCodec(), true);
        }

        /**
         * Returns the unloader that produces the compressed batches.
         *
         * @return unloader with this compressor's codec
         */
        public VectorUnloader unloader() {
            return unloader;
        }

        /**
         * Starts compressing the current batch's buffers; call right before the batch is put on the stream.
         * Buffers that were not prepared are compressed inline by the unloader.
         */
        public void prepare() {
            discard();
            for (FieldVector vector : root.getFieldVectors()) {
                submit(vector);
            }
        }

        /** Releases compressed buffers that were prepared but never taken by the unloader (a cancelled stream). */
        public void discard() {
            for (Future<ArrowBuf> future : prepared.values()) {
                try {
                    future.get().close();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException | RuntimeException e) {
                    LOGGER.debug("Discarded a failed Flight batch compression: {}", e.toString());
                }
            }
            prepared.clear();
        }

        /**
         * Buffers compressed ahead of the unloader.
         *
         * @return count of buffers served from a prepared result
         */
        public long preparedHits() {
            return hits.get();
        }

        /**
         * Buffers the unloader had to compress itself.
         *
         * @return count of inline compressions
         */
        public long preparedMisses() {
            return misses.get();
        }

        private void submit(FieldVector vector) {
            BufferAllocator allocator = vector.getAllocator();
            List<ArrowBuf> buffers = vector.getFieldBuffers();
            for (ArrowBuf buf : buffers) {
                if (buf.writerIndex() < MIN_PARALLEL_BYTES) {
                    continue;
                }
                // the codec closes the buffer it compresses, so it needs its own reference
                buf.getReferenceManager().retain();
                prepared.put(buf, pool.submit(() -> delegate.compress(allocator, buf)));
            }
            for (FieldVector child : vector.getChildrenFromFields()) {
                submit(child);
            }
        }

        /** Serves prepared results to the unloader and compresses everything else inline. */
        private final class PreparedCodec implements CompressionCodec {
            @Override
            public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
                Future<ArrowBuf> future = prepared.remove(uncompressedBuffer);
                if (future == null) {
                    misses.incrementAndGet();
                    return delegate.compress(allocator, uncompressedBuffer);
                }
                hits.incrementAndGet();
                // the unloader retained the buffer for this call; the prepared compression holds its own reference
                uncompressedBuffer.getReferenceManager().release();
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while compressing a Flight batch", e);
                } catch (ExecutionException e) {
                    throw new IllegalStateException("Compressing a Flight batch failed", e.getCause());
                }
            }

            @Override
            public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
                return delegate.decompress(allocator, compressedBuffer);
            }

            @Override
            public CompressionUtil.CodecType getCodecType() {
                return delegate.getCodecType();
            }
        }

        List<ArrowBuf> pendingBuffers() {
            return new ArrayList<>(prepared.keySet());
        }
    }
}
