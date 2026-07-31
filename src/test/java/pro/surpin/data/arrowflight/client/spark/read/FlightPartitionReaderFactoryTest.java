package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests schema-based selection of Spark row and columnar readers.
 */
@Tag("unit")
class FlightPartitionReaderFactoryTest {
    /**
     * Verifies that unknown partition implementations keep the row reader.
     */
    @Test
    void rejectsUnknownInputPartition() {
        FlightPartitionReaderFactory factory = factory();

        assertFalse(factory.supportColumnarReads(new InputPartition() { }));
    }

    /**
     * Verifies that the TPC-H scalar schema enables the columnar reader.
     */
    @Test
    void enablesColumnarReadsForTpchSchema() {
        Schema schema = new Schema(List.of(
                field("l_returnflag", ArrowType.Utf8.INSTANCE),
                field("l_quantity", new ArrowType.Decimal(15, 2, 128)),
                field("l_orderkey", new ArrowType.Int(64, true)),
                field("l_shipdate", new ArrowType.Date(DateUnit.DAY))));

        assertTrue(factory().supportColumnarReads(partition(schema)));
    }

    /**
     * Verifies that unsupported Arrow types keep the row reader.
     */
    @Test
    void rejectsTypesWithoutExactColumnarAdapter() {
        Schema schema = new Schema(List.of(
                field("ti", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                field("iv", new ArrowType.Interval(IntervalUnit.DAY_TIME))));

        assertFalse(factory().supportColumnarReads(partition(schema)));
    }

    /**
     * Creates a factory without opening a Flight connection.
     * @return reader factory
     */
    private static FlightPartitionReaderFactory factory() {
        Configuration configuration = new Configuration(
                "localhost", 32010, "user", "pass", null);
        return new FlightPartitionReaderFactory(configuration);
    }

    /**
     * Creates a query partition for schema selection tests.
     * @param schema Arrow schema
     * @return Flight input partition
     */
    private static FlightInputPartition partition(Schema schema) {
        return new FlightInputPartition.FlightQueryInputPartition(
                schema, "select * from tpch.lineitem");
    }

    /**
     * Creates a nullable Arrow field.
     * @param name field name
     * @param type Arrow type
     * @return Arrow field
     */
    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }
}
