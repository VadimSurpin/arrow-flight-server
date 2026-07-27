package net.surpin.data.arrowflight.client.model;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests failures reported by Arrow value conversion.
 */
@Tag("unit")
class ArrowConversionTest {

    /**
     * Verifies an incompatible Spark type reports an argument error.
     */
    @Test
    void rejectsIncompatibleSparkType() {
        try (BufferAllocator allocator = new RootAllocator();
                BitVector vector = new BitVector("value", allocator)) {
            assertThrows(IllegalArgumentException.class,
                    () -> ArrowConversion.getOrCreate().populate(
                            vector, new InternalRow[0], 0, DataTypes.StringType));
        }
    }
}
