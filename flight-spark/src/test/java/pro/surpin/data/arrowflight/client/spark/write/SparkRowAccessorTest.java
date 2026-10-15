package pro.surpin.data.arrowflight.client.spark.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SparkRowAccessorTest {

    @Test
    void convertsScalarsToJavaValues() {
        StructType schema = new StructType(new StructField[] {
            new StructField("s", DataTypes.StringType, true, Metadata.empty()),
            new StructField("d", DataTypes.DateType, true, Metadata.empty()),
            new StructField("ts", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("dec", DataTypes.createDecimalType(5, 2), true, Metadata.empty()),
            new StructField("i", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("n", DataTypes.IntegerType, true, Metadata.empty())
        });
        InternalRow row = new GenericInternalRow(new Object[] {
            UTF8String.fromString("x"), 1, 2_000_000L, Decimal.apply(new BigDecimal("1.25")), 7, null});
        SparkRowAccessor accessor = new SparkRowAccessor(row, schema);

        assertEquals("x", accessor.get(0));
        assertEquals(LocalDate.ofEpochDay(1), accessor.get(1));
        assertEquals(Instant.ofEpochSecond(2), accessor.get(2));
        assertEquals(new BigDecimal("1.25"), accessor.get(3));
        assertEquals(7, accessor.get(4));
        assertNull(accessor.get(5));
    }

    @Test
    void convertsNestedValues() {
        StructType inner = new StructType(new StructField[] {
            new StructField("a", DataTypes.StringType, true, Metadata.empty())
        });
        StructType schema = new StructType(new StructField[] {
            new StructField("st", inner, true, Metadata.empty()),
            new StructField("xs", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty()),
            new StructField("m", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true,
                    Metadata.empty())
        });
        InternalRow row = new GenericInternalRow(new Object[] {
            new GenericInternalRow(new Object[] {UTF8String.fromString("v")}),
            new GenericArrayData(new Object[] {1, null}),
            ArrayBasedMapData.apply(new Object[] {UTF8String.fromString("k")}, new Object[] {3})
        });
        SparkRowAccessor accessor = new SparkRowAccessor(row, schema);

        assertEquals(Map.of("a", "v"), accessor.get(0));
        assertEquals(java.util.Arrays.asList(1, null), accessor.get(1));
        assertEquals(Map.of("k", 3), accessor.get(2));
        assertInstanceOf(List.class, accessor.get(1));
    }
}
