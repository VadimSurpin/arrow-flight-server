package pro.surpin.data.arrowflight.client.metadata;

import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks table schema decoding against the server's Arrow IPC contract. */
@Tag("unit")
class TableSchemaDecoderTest {
    private static final Schema SCHEMA = new Schema(List.of(
            new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("amount", new FieldType(true, new ArrowType.Decimal(18, 2, 128), null,
                    Map.of("description", "Order amount")), null),
            new Field("details", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                    new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)))),
            Map.of("source", "sales.orders"));

    /**
     * Checks framed schemas preserve types, nullability and custom metadata.
     *
     * @throws IOException if schema encoding fails
     */
    @Test
    void decodesIpcSchema() throws IOException {
        assertEquals(SCHEMA, TableSchemaDecoder.decode(ipcSchema()));
    }

    /** Checks compatibility with servers that still send unframed schema metadata. */
    @Test
    void decodesLegacyUnframedSchema() {
        ByteBuffer metadata = MessageSerializer.serializeMetadata(SCHEMA, IpcOption.DEFAULT);
        byte[] bytes = new byte[metadata.remaining()];
        metadata.get(bytes);
        assertEquals(SCHEMA, TableSchemaDecoder.decode(bytes));
    }

    /** Checks missing and malformed metadata produces a clear connector error. */
    @Test
    void rejectsMissingOrMalformedSchema() {
        assertThrows(IllegalStateException.class, () -> TableSchemaDecoder.decode(null));
        for (byte[] bytes : List.of(new byte[0], new byte[]{1, 2, 3}, new byte[16])) {
            assertEquals("Flight server returned an invalid table schema",
                    assertThrows(IllegalStateException.class, () -> TableSchemaDecoder.decode(bytes)).getMessage());
        }
    }

    /**
     * Checks truncated IPC prefixes and payloads are rejected.
     *
     * @throws IOException if schema encoding fails
     */
    @Test
    void rejectsTruncatedIpcSchema() throws IOException {
        byte[] bytes = ipcSchema();
        for (int length : new int[]{Integer.BYTES, 2 * Integer.BYTES - 1, bytes.length - 1}) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThrows(IllegalStateException.class, () -> TableSchemaDecoder.decode(truncated));
        }
    }

    /** Checks invalid IPC lengths are rejected before allocating a metadata buffer. */
    @Test
    void rejectsInvalidIpcLength() {
        for (int length : new int[]{-1, 0, Integer.MAX_VALUE}) {
            byte[] prefix = ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(MessageSerializer.IPC_CONTINUATION_TOKEN).putInt(length).array();
            assertThrows(IllegalStateException.class, () -> TableSchemaDecoder.decode(prefix));
        }
    }

    /** Checks an IPC message must contain a schema. */
    @Test
    void rejectsIpcWithoutSchema() {
        byte[] bytes = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(MessageSerializer.IPC_CONTINUATION_TOKEN).putInt(16).array();
        assertThrows(IllegalStateException.class, () -> TableSchemaDecoder.decode(bytes));
    }

    /**
     * Encodes a schema as a standard Arrow IPC message.
     *
     * @return framed Arrow IPC schema
     * @throws IOException if schema encoding fails
     */
    private static byte[] ipcSchema() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (WriteChannel channel = new WriteChannel(Channels.newChannel(output))) {
            MessageSerializer.serialize(channel, SCHEMA, IpcOption.DEFAULT);
        }
        return output.toByteArray();
    }
}
