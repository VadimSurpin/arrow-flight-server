package pro.surpin.data.arrowflight.client.model;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldTypeTest {

    // ── fromArrow: primitives ─────────────────────────────────────────────

    @Test
    void fromArrowInt32Signed() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(32, true), List.of());
        assertEquals(FieldType.IDs.INT, ft.getTypeID());
    }

    @Test
    void fromArrowInt32Unsigned() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(32, false), List.of());
        assertEquals(FieldType.IDs.LONG, ft.getTypeID());
    }

    @Test
    void fromArrowInt8Signed() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(8, true), List.of());
        assertEquals(FieldType.IDs.BYTE, ft.getTypeID());
    }

    @Test
    void fromArrowInt8Unsigned() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(8, false), List.of());
        assertEquals(FieldType.IDs.SHORT, ft.getTypeID());
    }

    @Test
    void fromArrowInt16Signed() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(16, true), List.of());
        assertEquals(FieldType.IDs.SHORT, ft.getTypeID());
    }

    @Test
    void fromArrowInt16Unsigned() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(16, false), List.of());
        assertEquals(FieldType.IDs.INT, ft.getTypeID());
    }

    @Test
    void fromArrowInt64Signed() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(64, true), List.of());
        assertEquals(FieldType.IDs.LONG, ft.getTypeID());
    }

    @Test
    void fromArrowInt64Unsigned() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Int(64, false), List.of());
        assertEquals(FieldType.IDs.BIGINT, ft.getTypeID());
    }

    @Test
    void fromArrowBool() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Bool(), List.of());
        assertEquals(FieldType.IDs.BOOLEAN, ft.getTypeID());
    }

    @Test
    void fromArrowUtf8() {
        FieldType ft = FieldType.fromArrow(ArrowType.Utf8.INSTANCE, List.of());
        assertEquals(FieldType.IDs.VARCHAR, ft.getTypeID());
    }

    @Test
    void fromArrowLargeUtf8() {
        FieldType ft = FieldType.fromArrow(ArrowType.LargeUtf8.INSTANCE, List.of());
        assertEquals(FieldType.IDs.VARCHAR, ft.getTypeID());
    }

    @Test
    void fromArrowDecimal() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Decimal(10, 2, 128), List.of());
        assertInstanceOf(FieldType.DecimalType.class, ft);
        FieldType.DecimalType dt = (FieldType.DecimalType) ft;
        assertEquals(10, dt.getPrecision());
        assertEquals(2, dt.getScale());
    }

    @Test
    void fromArrowDate() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Date(DateUnit.DAY), List.of());
        assertEquals(FieldType.IDs.DATE, ft.getTypeID());
    }

    @Test
    void fromArrowTime() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Time(TimeUnit.MILLISECOND, 32), List.of());
        assertEquals(FieldType.IDs.TIME, ft.getTypeID());
    }

    @Test
    void fromArrowTimestamp() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), List.of());
        assertEquals(FieldType.IDs.TIMESTAMP, ft.getTypeID());
    }

    @Test
    void fromArrowFloatHalf() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), List.of());
        assertEquals(FieldType.IDs.FLOAT, ft.getTypeID());
    }

    @Test
    void fromArrowFloatSingle() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), List.of());
        assertEquals(FieldType.IDs.FLOAT, ft.getTypeID());
    }

    @Test
    void fromArrowFloatDouble() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), List.of());
        assertEquals(FieldType.IDs.DOUBLE, ft.getTypeID());
    }

    @Test
    void fromArrowIntervalYearMonth() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Interval(IntervalUnit.YEAR_MONTH), List.of());
        assertEquals(FieldType.IDs.PERIOD_YEAR_MONTH, ft.getTypeID());
    }

    @Test
    void fromArrowIntervalDayTime() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Interval(IntervalUnit.DAY_TIME), List.of());
        assertEquals(FieldType.IDs.DURATION_DAY_TIME, ft.getTypeID());
    }

    @Test
    void fromArrowIntervalMonthDayNano() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), List.of());
        assertEquals(FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME, ft.getTypeID());
    }

    @Test
    void fromArrowDuration() {
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Duration(TimeUnit.MILLISECOND), List.of());
        assertEquals(FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME, ft.getTypeID());
    }

    @Test
    void fromArrowNull() {
        FieldType ft = FieldType.fromArrow(ArrowType.Null.INSTANCE, List.of());
        assertEquals(FieldType.IDs.NULL, ft.getTypeID());
    }

    @Test
    void fromArrowBinary() {
        FieldType ft = FieldType.fromArrow(new ArrowType.Binary(), List.of());
        assertInstanceOf(FieldType.BinaryType.class, ft);
        assertEquals(-1, ((FieldType.BinaryType) ft).getByteWidth());
    }

    @Test
    void fromArrowLargeBinary() {
        FieldType ft = FieldType.fromArrow(new ArrowType.LargeBinary(), List.of());
        assertInstanceOf(FieldType.BinaryType.class, ft);
    }

    @Test
    void fromArrowFixedSizeBinary() {
        FieldType ft = FieldType.fromArrow(new ArrowType.FixedSizeBinary(16), List.of());
        assertInstanceOf(FieldType.BinaryType.class, ft);
        assertEquals(16, ((FieldType.BinaryType) ft).getByteWidth());
    }

    // ── fromArrow: List / FixedSizeList / LargeList ────────────────────────

    @Test
    void fromArrowList() {
        Field child = new Field("element",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        FieldType ft = FieldType.fromArrow(ArrowType.List.INSTANCE, List.of(child));
        assertInstanceOf(FieldType.ListType.class, ft);
        assertEquals(-1, ((FieldType.ListType) ft).getLength());
        assertEquals(FieldType.IDs.INT, ((FieldType.ListType) ft).getChildType().getTypeID());
    }

    @Test
    void fromArrowLargeList() {
        Field child = new Field("element",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        FieldType ft = FieldType.fromArrow(ArrowType.LargeList.INSTANCE, List.of(child));
        assertInstanceOf(FieldType.ListType.class, ft);
    }

    @Test
    void fromArrowFixedSizeList() {
        Field child = new Field("element",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        FieldType ft = FieldType.fromArrow(
                new ArrowType.FixedSizeList(5), List.of(child));
        assertInstanceOf(FieldType.ListType.class, ft);
        assertEquals(5, ((FieldType.ListType) ft).getLength());
    }

    @Test
    void fromArrowListNullChildren() {
        FieldType ft = FieldType.fromArrow(ArrowType.List.INSTANCE, null);
        assertInstanceOf(FieldType.ListType.class, ft);
        assertNull(((FieldType.ListType) ft).getChildType());
    }

    // ── fromArrow: Struct / Union ─────────────────────────────────────────

    @Test
    void fromArrowStruct() {
        Field child = new Field("id",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        FieldType ft = FieldType.fromArrow(ArrowType.Struct.INSTANCE, List.of(child));
        assertInstanceOf(FieldType.StructType.class, ft);
        assertEquals(1, ((FieldType.StructType) ft).getChildrenType().size());
    }

    @Test
    void fromArrowUnion() {
        Field child = new Field("age",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Union(org.apache.arrow.vector.types.UnionMode.Sparse, new int[0]),
                List.of(child));
        assertInstanceOf(FieldType.UnionType.class, ft);
    }

    @Test
    void fromArrowStructNullChildren() {
        FieldType ft = FieldType.fromArrow(ArrowType.Struct.INSTANCE, null);
        assertInstanceOf(FieldType.StructType.class, ft);
        assertTrue(((FieldType.StructType) ft).getChildrenType().isEmpty());
    }

    // ── fromArrow: Map ────────────────────────────────────────────────────

    @Test
    void fromArrowMapTwoChildren() {
        Field keyField = new Field("key",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, ArrowType.Utf8.INSTANCE, null), null);
        Field valueField = new Field("value",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Map(false), List.of(keyField, valueField));
        assertInstanceOf(FieldType.MapType.class, ft);
        assertEquals(FieldType.IDs.VARCHAR,
                ((FieldType.MapType) ft).getKeyType().getTypeID());
        assertEquals(FieldType.IDs.INT,
                ((FieldType.MapType) ft).getValueType().getTypeID());
    }

    @Test
    void fromArrowMapStructChild() {
        Field childKey = new Field("key",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, ArrowType.Utf8.INSTANCE, null), null);
        Field childValue = new Field("value",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, new ArrowType.Int(32, true), null), null);
        Field entriesField = new Field("entries",
                new org.apache.arrow.vector.types.pojo.FieldType(
                        false, ArrowType.Struct.INSTANCE, null),
                List.of(childKey, childValue));
        FieldType ft = FieldType.fromArrow(
                new ArrowType.Map(false), List.of(entriesField));
        assertInstanceOf(FieldType.MapType.class, ft);
    }

    @Test
    void fromArrowMapInvalidChildThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> FieldType.fromArrow(new ArrowType.Map(false), List.of()));
    }

    // ── toSpark: all type mappings ────────────────────────────────────────

    @Test
    void toSparkInt() {
        assertEquals(DataTypes.IntegerType,
                FieldType.toSpark(new FieldType(FieldType.IDs.INT)));
    }

    @Test
    void toSparkVarchar() {
        assertEquals(DataTypes.StringType,
                FieldType.toSpark(new FieldType(FieldType.IDs.VARCHAR)));
    }

    @Test
    void toSparkChar() {
        assertEquals(DataTypes.StringType,
                FieldType.toSpark(new FieldType(FieldType.IDs.CHAR)));
    }

    @Test
    void toSparkTimeAsString() {
        assertEquals(DataTypes.StringType,
                FieldType.toSpark(new FieldType(FieldType.IDs.TIME)));
    }

    @Test
    void toSparkLong() {
        assertEquals(DataTypes.LongType,
                FieldType.toSpark(new FieldType(FieldType.IDs.LONG)));
    }

    @Test
    void toSparkBigint() {
        assertEquals(DataTypes.LongType,
                FieldType.toSpark(new FieldType(FieldType.IDs.BIGINT)));
    }

    @Test
    void toSparkFloat() {
        assertEquals(DataTypes.FloatType,
                FieldType.toSpark(new FieldType(FieldType.IDs.FLOAT)));
    }

    @Test
    void toSparkDouble() {
        assertEquals(DataTypes.DoubleType,
                FieldType.toSpark(new FieldType(FieldType.IDs.DOUBLE)));
    }

    @Test
    void toSparkDate() {
        assertEquals(DataTypes.DateType,
                FieldType.toSpark(new FieldType(FieldType.IDs.DATE)));
    }

    @Test
    void toSparkTimestamp() {
        assertEquals(DataTypes.TimestampType,
                FieldType.toSpark(new FieldType(FieldType.IDs.TIMESTAMP)));
    }

    @Test
    void toSparkBoolean() {
        assertEquals(DataTypes.BooleanType,
                FieldType.toSpark(new FieldType(FieldType.IDs.BOOLEAN)));
    }

    @Test
    void toSparkByte() {
        assertEquals(DataTypes.ByteType,
                FieldType.toSpark(new FieldType(FieldType.IDs.BYTE)));
    }

    @Test
    void toSparkShort() {
        assertEquals(DataTypes.ShortType,
                FieldType.toSpark(new FieldType(FieldType.IDs.SHORT)));
    }

    @Test
    void toSparkBytes() {
        assertEquals(DataTypes.BinaryType,
                FieldType.toSpark(new FieldType(FieldType.IDs.BYTES)));
    }

    @Test
    void toSparkDecimal() {
        assertEquals(new org.apache.spark.sql.types.DecimalType(10, 2),
                FieldType.toSpark(new FieldType.DecimalType(10, 2)));
    }

    @Test
    void toSparkPeriodYearMonth() {
        assertInstanceOf(org.apache.spark.sql.types.YearMonthIntervalType.class,
                FieldType.toSpark(new FieldType(FieldType.IDs.PERIOD_YEAR_MONTH)));
    }

    @Test
    void toSparkDurationDayTime() {
        assertInstanceOf(org.apache.spark.sql.types.DayTimeIntervalType.class,
                FieldType.toSpark(new FieldType(FieldType.IDs.DURATION_DAY_TIME)));
    }

    @Test
    void toSparkPeriodDurationMonthDayTime() {
        assertInstanceOf(org.apache.spark.sql.types.StructType.class,
                FieldType.toSpark(new FieldType(
                        FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME)));
    }

    @Test
    void toSparkNullType() {
        assertEquals(DataTypes.NullType,
                FieldType.toSpark(new FieldType(FieldType.IDs.NULL)));
    }

    @Test
    void toSparkList() {
        FieldType.ListType listType = new FieldType.ListType(
                new FieldType(FieldType.IDs.INT));
        assertEquals(new org.apache.spark.sql.types.ArrayType(DataTypes.IntegerType, true),
                FieldType.toSpark(listType));
    }

    @Test
    void toSparkMap() {
        FieldType.MapType mapType = new FieldType.MapType(
                new FieldType(FieldType.IDs.VARCHAR),
                new FieldType(FieldType.IDs.INT));
        assertEquals(new org.apache.spark.sql.types.MapType(
                        DataTypes.StringType, DataTypes.IntegerType, true),
                FieldType.toSpark(mapType));
    }

    @Test
    void toSparkStruct() {
        FieldType.StructType structType = new FieldType.StructType(
                Map.of("name", new FieldType(FieldType.IDs.VARCHAR),
                        "age", new FieldType(FieldType.IDs.INT)));
        org.apache.spark.sql.types.DataType sparkType = FieldType.toSpark(structType);
        assertInstanceOf(org.apache.spark.sql.types.StructType.class, sparkType);
        assertEquals(2,
                ((org.apache.spark.sql.types.StructType) sparkType).fields().length);
    }

    // ── DecimalType ───────────────────────────────────────────────────────

    @Test
    void decimalTypePrecisionAndScale() {
        FieldType.DecimalType dt = new FieldType.DecimalType(10, 2);
        assertEquals(10, dt.getPrecision());
        assertEquals(2, dt.getScale());
        assertEquals(FieldType.IDs.DECIMAL, dt.getTypeID());
    }

    // ── BinaryType ────────────────────────────────────────────────────────

    @Test
    void binaryTypeVariableWidth() {
        FieldType.BinaryType bt = new FieldType.BinaryType(-1);
        assertEquals(-1, bt.getByteWidth());
    }

    @Test
    void binaryTypeFixedWidth() {
        FieldType.BinaryType bt = new FieldType.BinaryType(16);
        assertEquals(16, bt.getByteWidth());
    }

    // ── ListType constructors ─────────────────────────────────────────────

    @Test
    void listTypeDynamicSize() {
        FieldType.ListType lt = new FieldType.ListType(
                new FieldType(FieldType.IDs.INT));
        assertEquals(-1, lt.getLength());
        assertEquals(FieldType.IDs.INT, lt.getChildType().getTypeID());
    }

    @Test
    void listTypeFixedSize() {
        FieldType.ListType lt = new FieldType.ListType(10,
                new FieldType(FieldType.IDs.VARCHAR));
        assertEquals(10, lt.getLength());
        assertEquals(FieldType.IDs.VARCHAR, lt.getChildType().getTypeID());
    }

    // ── StructType ────────────────────────────────────────────────────────

    @Test
    void structTypeChildren() {
        Map<String, FieldType> children = Map.of("col1",
                new FieldType(FieldType.IDs.INT));
        FieldType.StructType st = new FieldType.StructType(children);
        assertEquals(1, st.getChildrenType().size());
        assertEquals(FieldType.IDs.INT,
                st.getChildrenType().get("col1").getTypeID());
    }

    // ── UnionType extends StructType ──────────────────────────────────────

    @Test
    void unionTypeExtendsStruct() {
        FieldType.UnionType ut = new FieldType.UnionType(
                Map.of("tag", new FieldType(FieldType.IDs.INT)));
        assertInstanceOf(FieldType.StructType.class, ut);
        assertEquals(1, ut.getChildrenType().size());
    }
}
