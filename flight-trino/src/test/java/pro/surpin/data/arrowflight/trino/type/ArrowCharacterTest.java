package pro.surpin.data.arrowflight.trino.type;

import io.trino.spi.type.CharType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks CHAR storage trims only trailing ASCII spaces and counts Unicode code points. */
@Tag("unit")
class ArrowCharacterTest {
    /** Checks a supplementary character fits CHAR(1) while two code points do not. */
    @Test
    void preservesCharacterWidth() {
        var type = CharType.createCharType(1);
        try (var allocator = new RootAllocator(); var vector = new VarCharVector("v", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "𝄞  ".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            assertEquals("𝄞", type.getSlice(ArrowToTrinoBlockConverter.toBlock(type, vector, 1), 0).toStringUtf8());
            vector.setSafe(0, "a\u0000".getBytes(StandardCharsets.UTF_8));
            assertThrows(IllegalArgumentException.class, () -> ArrowToTrinoBlockConverter.toBlock(type, vector, 1));
        }
    }
}
