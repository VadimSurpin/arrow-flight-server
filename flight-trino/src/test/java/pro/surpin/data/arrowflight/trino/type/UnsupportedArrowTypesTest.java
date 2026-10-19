package pro.surpin.data.arrowflight.trino.type;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that unsupported Arrow encodings do not break table discovery. */
@Tag("unit")
class UnsupportedArrowTypesTest {
    /** Checks decimal scales outside Trino's supported range. */
    @Test
    void skipsUnrepresentableDecimalScales() {
        assertTrue(ArrowTypes.toTrino(new Field("negative",
                FieldType.nullable(new ArrowType.Decimal(10, -2, 128)), null)).isEmpty());
        assertTrue(ArrowTypes.toTrino(new Field("wide",
                FieldType.nullable(new ArrowType.Decimal(10, 11, 128)), null)).isEmpty());
    }

    /** Checks that dictionary indices are not exposed as actual column values. */
    @Test
    void skipsDictionaryEncodedColumns() {
        Field field = new Field("encoded", new FieldType(true, ArrowType.Utf8.INSTANCE,
                new DictionaryEncoding(1L, false, new ArrowType.Int(32, true))), null);
        assertTrue(ArrowTypes.toTrino(field).isEmpty());
    }
}
