package pro.surpin.data.arrowflight.client.metadata;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;

/** Decodes Flight SQL table schemas independently of the client engine. */
public final class TableSchemaDecoder {
    /** Prevents instantiation of the schema decoder. */
    private TableSchemaDecoder() { }

    /**
     * Decodes an Arrow IPC schema or legacy unframed schema metadata.
     *
     * @param bytes encoded table schema
     * @return Arrow schema
     * @throws IllegalStateException if the server sent no schema or an invalid schema
     */
    public static Schema decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalStateException("Flight server returned a table without a schema");
        }
        try {
            ByteBuffer metadata = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            if (metadata.remaining() >= Integer.BYTES
                    && metadata.getInt(0) == MessageSerializer.IPC_CONTINUATION_TOKEN) {
                if (metadata.remaining() < 2 * Integer.BYTES) {
                    throw new IOException("Incomplete IPC schema prefix");
                }
                int length = metadata.getInt(Integer.BYTES);
                if (length <= 0 || length > bytes.length - 2 * Integer.BYTES) {
                    throw new IOException("Invalid IPC schema length");
                }
                try (ReadChannel channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes)))) {
                    return MessageSerializer.deserializeSchema(channel);
                }
            }
            return MessageSerializer.deserializeSchema(Message.getRootAsMessage(metadata));
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("Flight server returned an invalid table schema", e);
        }
    }
}
