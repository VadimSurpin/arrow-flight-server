package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies zero-copy scalar access from Flight-owned Arrow vectors.
 */
@Tag("unit")
class FlightArrowColumnVectorTest {

    /** Verifies UTF-8 values remain views over a standard Arrow string buffer. */
    @Test
    void readsVarCharWithoutCopyingDataBuffer() {
        try (RootAllocator allocator = new RootAllocator();
                VarCharVector vector = new VarCharVector("value", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
            vector.setNull(1);
            vector.setValueCount(2);
            FlightArrowColumnVector column = new FlightArrowColumnVector(vector);

            UTF8String value = column.getUTF8String(0);
            vector.getDataBuffer().setByte(0, 'H');

            assertEquals("Hello", value.toString());
            assertNull(column.getUTF8String(1));
        }
    }

    /** Verifies UTF-8 values remain views over a large Arrow string buffer. */
    @Test
    void readsLargeVarCharWithoutCopyingDataBuffer() {
        try (RootAllocator allocator = new RootAllocator();
                LargeVarCharVector vector = new LargeVarCharVector("value", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "flight".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            FlightArrowColumnVector column = new FlightArrowColumnVector(vector);

            UTF8String value = column.getUTF8String(0);
            vector.getDataBuffer().setByte(0, 'F');

            assertEquals("Flight", value.toString());
        }
    }

    /** Verifies a DECIMAL column sent as a 64-bit integer with precision and scale metadata reads as a Spark decimal. */
    @Test
    void readsNarrowedDecimalColumns() {
        Field field = new Field("price", new FieldType(true, new ArrowType.Int(64, true), null,
                Map.of("decimal.precision", "15", "decimal.scale", "2")), null);
        try (RootAllocator allocator = new RootAllocator();
                BigIntVector vector = (BigIntVector) field.createVector(allocator)) {
            vector.allocateNew();
            vector.setSafe(0, 123456);
            vector.setNull(1);
            vector.setValueCount(2);
            FlightArrowColumnVector column = new FlightArrowColumnVector(vector);

            assertEquals(DataTypes.createDecimalType(15, 2), column.dataType());
            assertEquals(new java.math.BigDecimal("1234.56"), column.getDecimal(0, 15, 2).toJavaBigDecimal());
            assertNull(column.getDecimal(1, 15, 2));
            assertTrue(FlightArrowColumnVector.carriesDecimal(new ArrowType.Decimal(15, 2, 128), field));
            assertFalse(FlightArrowColumnVector.carriesDecimal(new ArrowType.Decimal(15, 3, 128), field));
        }
    }
}
