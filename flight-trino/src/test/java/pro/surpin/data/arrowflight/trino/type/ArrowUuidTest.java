package pro.surpin.data.arrowflight.trino.type;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks logical UUID metadata and lossless native UUID decoding. */
@Tag("unit")
class ArrowUuidTest {
    /** Checks shortened UUID text is not silently interpreted as canonical storage. */
    @Test
    void refusesShortenedUuidText() {
        try (var allocator = new RootAllocator(); var vector = new VarCharVector("v", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "1-1-1-1-1".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            assertThrows(IllegalArgumentException.class, () -> ArrowToTrinoBlockConverter.toBlock(UUID, vector, 1));
        }
    }
    /** Checks UUID is declared explicitly rather than inferred from arbitrary strings. */
    @Test
    void recognizesLogicalMetadata() {
        var field = new Field("v", new FieldType(true, ArrowType.Utf8.INSTANCE, null,
                Map.of("arrowflight:logical_type", "uuid")), null);
        assertEquals(Optional.of(UUID), ArrowTypes.toTrino(field));
        var invalid = new Field("v", new FieldType(true, ArrowType.Binary.INSTANCE, null,
                Map.of("arrowflight:logical_type", "uuid")), null);
        assertTrue(ArrowTypes.toTrino(invalid).isEmpty());
    }

    /** Checks all UUID bits survive decoding across the signed boundary and NULL. */
    @Test
    void decodesNativeUuidBits() {
        var values = List.of("00000000-0000-0000-0000-000000000000",
                "7fffffff-ffff-ffff-ffff-ffffffffffff", "80000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff");
        try (var allocator = new RootAllocator(); var vector = new VarCharVector("v", allocator)) {
            vector.allocateNew();
            for (int row = 0; row < values.size(); row++) {
                vector.setSafe(row, values.get(row).getBytes(StandardCharsets.UTF_8));
            }
            vector.setNull(values.size());
            vector.setValueCount(values.size() + 1);
            var block = ArrowToTrinoBlockConverter.toBlock(UUID, vector, vector.getValueCount());
            for (int row = 0; row < values.size(); row++) {
                assertEquals(values.get(row), trinoUuidToJavaUuid(UUID.getSlice(block, row)).toString());
            }
            assertTrue(block.isNull(values.size()));
        }
    }
}
