package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.model.FieldType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SparkTypesTest {

    // ── toSpark: all type mappings ────────────────────────────────────────

    @Test
    void toSparkInt() {
        assertEquals(DataTypes.IntegerType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.INT)));
    }

    @Test
    void toSparkVarchar() {
        assertEquals(DataTypes.StringType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.VARCHAR)));
    }

    @Test
    void toSparkChar() {
        assertEquals(DataTypes.StringType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.CHAR)));
    }

    @Test
    void toSparkTimeAsString() {
        assertEquals(DataTypes.StringType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.TIME)));
    }

    @Test
    void toSparkLong() {
        assertEquals(DataTypes.LongType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.LONG)));
    }

    @Test
    void toSparkBigint() {
        assertEquals(DataTypes.LongType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.BIGINT)));
    }

    @Test
    void toSparkFloat() {
        assertEquals(DataTypes.FloatType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.FLOAT)));
    }

    @Test
    void toSparkDouble() {
        assertEquals(DataTypes.DoubleType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.DOUBLE)));
    }

    @Test
    void toSparkDate() {
        assertEquals(DataTypes.DateType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.DATE)));
    }

    @Test
    void toSparkTimestamp() {
        assertEquals(DataTypes.TimestampType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.TIMESTAMP)));
    }

    @Test
    void toSparkBoolean() {
        assertEquals(DataTypes.BooleanType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.BOOLEAN)));
    }

    @Test
    void toSparkByte() {
        assertEquals(DataTypes.ByteType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.BYTE)));
    }

    @Test
    void toSparkShort() {
        assertEquals(DataTypes.ShortType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.SHORT)));
    }

    @Test
    void toSparkBytes() {
        assertEquals(DataTypes.BinaryType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.BYTES)));
    }

    @Test
    void toSparkDecimal() {
        assertEquals(new org.apache.spark.sql.types.DecimalType(10, 2),
                SparkTypes.toSpark(new FieldType.DecimalType(10, 2)));
    }

    @Test
    void toSparkPeriodYearMonth() {
        assertInstanceOf(org.apache.spark.sql.types.YearMonthIntervalType.class,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.PERIOD_YEAR_MONTH)));
    }

    @Test
    void toSparkDurationDayTime() {
        assertInstanceOf(org.apache.spark.sql.types.DayTimeIntervalType.class,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.DURATION_DAY_TIME)));
    }

    @Test
    void toSparkPeriodDurationMonthDayTime() {
        assertInstanceOf(org.apache.spark.sql.types.StructType.class,
                SparkTypes.toSpark(new FieldType(
                        FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME)));
    }

    @Test
    void toSparkNullType() {
        assertEquals(DataTypes.NullType,
                SparkTypes.toSpark(new FieldType(FieldType.IDs.NULL)));
    }

    @Test
    void toSparkList() {
        FieldType.ListType listType = new FieldType.ListType(
                new FieldType(FieldType.IDs.INT));
        assertEquals(new org.apache.spark.sql.types.ArrayType(DataTypes.IntegerType, true),
                SparkTypes.toSpark(listType));
    }

    @Test
    void toSparkMap() {
        FieldType.MapType mapType = new FieldType.MapType(
                new FieldType(FieldType.IDs.VARCHAR),
                new FieldType(FieldType.IDs.INT));
        assertEquals(new org.apache.spark.sql.types.MapType(
                        DataTypes.StringType, DataTypes.IntegerType, true),
                SparkTypes.toSpark(mapType));
    }

    @Test
    void toSparkStruct() {
        FieldType.StructType structType = new FieldType.StructType(
                Map.of("name", new FieldType(FieldType.IDs.VARCHAR),
                        "age", new FieldType(FieldType.IDs.INT)));
        org.apache.spark.sql.types.DataType sparkType = SparkTypes.toSpark(structType);
        assertInstanceOf(org.apache.spark.sql.types.StructType.class, sparkType);
        assertEquals(2,
                ((org.apache.spark.sql.types.StructType) sparkType).fields().length);
    }
}
