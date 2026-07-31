package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies allocation-free reuse of Spark adapters across Flight record batches.
 */
@Tag("unit")
class FlightColumnarPartitionReaderTest {

    /**
     * Verifies a reused Flight root keeps one batch and one set of column adapters.
     *
     * @throws Exception on mocked stream failure
     */
    @Test
    void reusesBatchAndColumnsForStableFlightRoot() throws Exception {
        Schema schema = new Schema(List.of(new Field(
                "value",
                FieldType.nullable(new ArrowType.Int(32, true)),
                List.of())));
        FlightPartitionReader streamReader = mock(FlightPartitionReader.class);
        when(streamReader.nextBatch()).thenReturn(true, true, false);

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector vector = (IntVector) root.getVector("value");
            vector.allocateNew(3);
            vector.setSafe(0, 10);
            vector.setSafe(1, 20);
            vector.setSafe(2, 30);
            root.setRowCount(3);
            when(streamReader.currentBatch()).thenReturn(root);

            FlightColumnarPartitionReader reader =
                    new FlightColumnarPartitionReader(streamReader, schema);
            reader.next();
            ColumnarBatch firstBatch = reader.get();
            ColumnVector firstColumn = firstBatch.column(0);

            root.setRowCount(1);
            reader.next();

            assertSame(firstBatch, reader.get());
            assertSame(firstColumn, reader.get().column(0));
            assertEquals(1, reader.get().numRows());
            assertEquals(10, reader.get().column(0).getInt(0));

            reader.close();
            verify(streamReader).close();
        }
    }
}
