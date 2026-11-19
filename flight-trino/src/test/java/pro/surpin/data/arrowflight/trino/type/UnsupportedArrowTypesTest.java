package pro.surpin.data.arrowflight.trino.type;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that unsupported Arrow encodings are not misrepresented as their storage types. */
@Tag("unit")
class UnsupportedArrowTypesTest {
    /** Checks unrecognized logical extensions are not exposed as plain binary columns. */
    @Test
    void rejectsUnknownLogicalExtensions() {
        var field = new Field("uuid", new FieldType(true, new ArrowType.FixedSizeBinary(16), null,
                java.util.Map.of("ARROW:extension:name", "arrow.uuid")), null);
        assertTrue(ArrowTypes.toTrino(field).isEmpty());
    }
    /** Checks decimal scales outside Trino's supported range. */
    @Test
    void skipsUnrepresentableDecimalScales() {
        assertTrue(ArrowTypes.toTrino(new Field("negative",
                FieldType.nullable(new ArrowType.Decimal(10, -2, 128)), null)).isEmpty());
        assertTrue(ArrowTypes.toTrino(new Field("wide",
                FieldType.nullable(new ArrowType.Decimal(10, 11, 128)), null)).isEmpty());
    }

    /** Checks reads cannot silently narrow a zoned nanosecond timestamp in DuckDB. */
    @Test
    void rejectsNanosecondZonedTimestamps() {
        Field timestamp = new Field("instant", FieldType.nullable(new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.NANOSECOND, "UTC")), null);
        assertTrue(ArrowTypes.toTrino(timestamp).isEmpty());
        Field nested = new Field("details", FieldType.nullable(ArrowType.Struct.INSTANCE), java.util.List.of(timestamp));
        assertTrue(ArrowTypes.toTrino(nested).isEmpty());
    }

    /** Checks that dictionary indices are not exposed as actual column values. */
    @Test
    void skipsDictionaryEncodedColumns() {
        Field field = new Field("encoded", new FieldType(true, ArrowType.Utf8.INSTANCE,
                new DictionaryEncoding(1L, false, new ArrowType.Int(32, true))), null);
        assertTrue(ArrowTypes.toTrino(field).isEmpty());
    }
}
