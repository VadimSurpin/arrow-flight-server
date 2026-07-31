package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
