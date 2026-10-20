package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optional compression of the record batches of a Flight data stream.
 *
 * <p>Arrow Flight for Java writes batches without a codec and offers no option for one, but its stream listener keeps
 * the {@link VectorUnloader} that turns each batch into its wire form in a field. After a stream has started that
 * unloader is replaced by one with a codec; the schema message is unaffected, and readers decompress from the batch
 * header. Foreign listeners (tests, mocks) are left alone.
 */
public final class FlightCompression {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightCompression.class);
    private static final String UNLOADER_FIELD = "unloader";
    private static final AtomicLong APPLIED = new AtomicLong();

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
     * Applies the configured codec to a started stream.
     *
     * @param listener stream listener, already started with {@code root}
     * @param root the stream's vector root
     * @return true when the codec was applied
     */
    public static boolean apply(FlightProducer.ServerStreamListener listener, VectorSchemaRoot root) {
        return apply(listener, root, ConfigAdapter.getFlightCompression());
    }

    /**
     * Applies a codec to a started stream.
     *
     * @param listener stream listener, already started with {@code root}
     * @param root the stream's vector root
     * @param codec none, lz4 or zstd
     * @return true when the codec was applied
     */
    public static boolean apply(FlightProducer.ServerStreamListener listener, VectorSchemaRoot root, String codec) {
        if (codec == null || codec.isBlank() || codec.equalsIgnoreCase("none")) {
            return false;
        }
        VectorUnloader compressing = unloader(root, codec);
        for (Class<?> type = listener.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(UNLOADER_FIELD);
                if (!VectorUnloader.class.isAssignableFrom(field.getType())) {
                    continue;
                }
                field.setAccessible(true);
                field.set(listener, compressing);
                APPLIED.incrementAndGet();
                return true;
            } catch (NoSuchFieldException e) {
                // keep looking in the superclass
            } catch (ReflectiveOperationException | RuntimeException e) {
                LOGGER.warn("Flight batch compression is not available: {}", e.toString());
                return false;
            }
        }
        return false;
    }

    /**
     * Builds the unloader that produces compressed batches.
     *
     * @param root vector root
     * @param codec lz4 or zstd
     * @return unloader with the codec
     */
    static VectorUnloader unloader(VectorSchemaRoot root, String codec) {
        CompressionUtil.CodecType type;
        switch (codec.toLowerCase(Locale.ROOT)) {
            case "lz4":
                type = CompressionUtil.CodecType.LZ4_FRAME;
                break;
            case "zstd":
                type = CompressionUtil.CodecType.ZSTD;
                break;
            default:
                throw new IllegalArgumentException("Unsupported Flight compression: " + codec);
        }
        CompressionCodec compressionCodec = CommonsCompressionFactory.INSTANCE.createCodec(type);
        return new VectorUnloader(root, true, compressionCodec, true);
    }
}
