package net.surpin.data.arrowflight.client.model;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.types.DayTimeIntervalType;

/**
 * Describes the data-type of a Field
 */
public class FieldType implements Serializable {
    /**
     * The ID of each type
     */
    public enum IDs {
        NULL,                           //Null object
        BOOLEAN,                        //Boolean
        BYTE,                           //Byte
        CHAR,                           //Character
        SHORT,                          //Short
        INT,                            //Integer
        LONG,                           //Long
        BIGINT,                         //Big Integer
        FLOAT,                          //Float
        DOUBLE,                         //Double
        DECIMAL,                        //BidDecimal
        VARCHAR,                        //String
        BYTES,                          //byte[]
        DATE,                           //LocalDate
        TIME,                           //LocalTime
        TIMESTAMP,                      //TimeStamp
        PERIOD_YEAR_MONTH,              //Period - year with month
        DURATION_DAY_TIME,              //Period - day with time
        PERIOD_DURATION_MONTH_DAY_TIME, //Period - month with day
        LIST,                           //List<?>
        MAP,                            //Map<String, ?>
        STRUCT                          //Struct
    }

    /**
     * Decimal Type
     */
    public static class DecimalType extends FieldType {
        private final int precision;
        private final int scale;

        /**
         * Constructs a DecimalType with given precision and scale.
         * @param precision decimal precision
         * @param scale decimal scale
         */
        public DecimalType(int precision, int scale) {
            super(IDs.DECIMAL);
            this.precision = precision;
            this.scale = scale;
        }

        /**
         * Gets the decimal precision.
         * @return precision
         */
        public int getPrecision() {
            return this.precision;
        }
        /**
         * Gets the decimal scale.
         * @return scale
         */
        public int getScale() {
            return this.scale;
        }
    }

    /**
     * Binary Type
     */
    public static class BinaryType extends FieldType {
        private final int byteWidth;

        /**
         * Constructs a BinaryType with fixed byte width.
         * @param byteWidth byte width (-1 for variable)
         */
        public BinaryType(int byteWidth) {
            super(IDs.BYTES);
            this.byteWidth = byteWidth;
        }

        /**
         * Gets the byte width of the binary type.
         * @return byte width
         */
        public int getByteWidth() {
            return this.byteWidth;
        }
    }

    /**
     * List Type
     */
    public static class ListType extends FieldType {
        private final int length;
        private final FieldType childType;

        /**
         * Constructs a fixed-size ListType.
         * @param length fixed length (-1 for dynamic)
         * @param childType element type
         */
        public ListType(int length, FieldType childType) {
            super(IDs.LIST);
            this.length = length;
            this.childType = childType;
        }
        /**
         * Constructs a dynamic-size ListType.
         * @param childType element type
         */
        public ListType(FieldType childType) {
            //dynamic size of list
            this(-1, childType);
        }

        /**
         * Gets the fixed length of the list.
         * @return length (-1 if dynamic)
         */
        public int getLength() {
            return this.length;
        }
        /**
         * Gets the element type of the list.
         * @return child field type
         */
        public FieldType getChildType() {
            return this.childType;
        }
    }

    /**
     * May Type
     */
    public static class MapType extends FieldType {
        private final FieldType keyType;
        private final FieldType valueType;

        /**
         * Constructs a MapType with key and value types.
         * @param keyType key field type
         * @param valueType value field type
         */
        public MapType(FieldType keyType, FieldType valueType) {
            super(IDs.MAP);
            this.keyType = keyType;
            this.valueType = valueType;
        }

        /**
         * Gets the key type of the map.
         * @return key field type
         */
        public FieldType getKeyType() {
            return this.keyType;
        }
        /**
         * Gets the value type of the map.
         * @return value field type
         */
        public FieldType getValueType() {
            return this.valueType;
        }
    }

    /**
     * Struct Type
     */
    public static class StructType extends FieldType {
        private final Map<String, FieldType> childrenType;

        /**
         * Constructs a StructType with child fields.
         * @param childrenType map of field name to field type
         */
        public StructType(Map<String, FieldType> childrenType) {
            super(IDs.STRUCT);
            this.childrenType = childrenType;
        }

        /**
         * Gets the child field types.
         * @return map of field name to field type
         */
        public Map<String, FieldType> getChildrenType() {
            return this.childrenType;
        }
    }

    /**
     * Union Type
     */
    public static class UnionType extends StructType {
        /**
         * Constructs a UnionType with child fields.
         * @param childrenType map of field name to field type
         */
        public UnionType(Map<String, FieldType> childrenType) {
            super(childrenType);
        }
    }

    //the type value
    private final IDs typeId;

    private static final String KEY_FIELD_NAME = "key";
    private static final String VALUE_FIELD_NAME = "value";

    /**
     * Construct a FieldType object
     * @param typeId - the id of the type
     */
    public FieldType(IDs typeId) {
        this.typeId = typeId;
    }

    /**
     * Get the Type ID
     * @return - the type ID
     */
    public IDs getTypeID() {
        return this.typeId;
    }

    /**
     * Convert an arrow-type to field-type
     * @param at - the arrow type
     * @param children - any children of the arrow-type
     * @return - the converted field-type
     */
    public static FieldType fromArrow(ArrowType at, java.util.List<org.apache.arrow.vector.types.pojo.Field> children) {
        return switch (at.getTypeID()) {
            case Int -> fromArrowInt((ArrowType.Int) at);
            case Utf8, LargeUtf8 -> new FieldType(IDs.VARCHAR);
            case Decimal -> {
                ArrowType.Decimal d = (ArrowType.Decimal) at;
                yield new DecimalType(d.getPrecision(), d.getScale());
            }
            case Date -> new FieldType(IDs.DATE);
            case Time -> new FieldType(IDs.TIME);
            case Timestamp -> new FieldType(IDs.TIMESTAMP);
            case FloatingPoint -> fromArrowFloatingPoint((ArrowType.FloatingPoint) at);
            case Interval -> fromArrowInterval((ArrowType.Interval) at);
            case Duration -> new FieldType(IDs.PERIOD_DURATION_MONTH_DAY_TIME);
            case Bool -> new FieldType(IDs.BOOLEAN);
            case Struct, Union -> fromArrowStruct(at, children);
            case Map -> fromArrowMap(children);
            case List, LargeList, FixedSizeList -> fromArrowList(at, children);
            case Binary, LargeBinary -> new BinaryType(-1);
            case FixedSizeBinary -> new BinaryType(((ArrowType.FixedSizeBinary) at).getByteWidth());
            default -> new FieldType(IDs.NULL);
        };
    }

    /**
     * Converts an Arrow integer type.
     * @param type Arrow integer type
     * @return converted field type
     */
    private static FieldType fromArrowInt(ArrowType.Int type) {
        return switch (type.getBitWidth()) {
            case 8 -> new FieldType(type.getIsSigned() ? IDs.BYTE : IDs.SHORT);
            case 16 -> new FieldType(type.getIsSigned() ? IDs.SHORT : IDs.INT);
            case 64 -> new FieldType(type.getIsSigned() ? IDs.LONG : IDs.BIGINT);
            default -> new FieldType(type.getIsSigned() ? IDs.INT : IDs.LONG);
        };
    }

    /**
     * Converts an Arrow floating-point type.
     * @param type Arrow floating-point type
     * @return converted field type
     */
    private static FieldType fromArrowFloatingPoint(ArrowType.FloatingPoint type) {
        return switch (type.getPrecision()) {
            case HALF, SINGLE -> new FieldType(IDs.FLOAT);
            default -> new FieldType(IDs.DOUBLE);
        };
    }

    /**
     * Converts an Arrow interval type.
     * @param type Arrow interval type
     * @return converted field type
     */
    private static FieldType fromArrowInterval(ArrowType.Interval type) {
        return switch (type.getUnit()) {
            case YEAR_MONTH -> new FieldType(IDs.PERIOD_YEAR_MONTH);
            case DAY_TIME -> new FieldType(IDs.DURATION_DAY_TIME);
            default -> new FieldType(IDs.PERIOD_DURATION_MONTH_DAY_TIME);
        };
    }

    /**
     * Converts an Arrow struct or union type.
     * @param type Arrow struct or union type
     * @param children child fields
     * @return converted field type
     */
    private static FieldType fromArrowStruct(ArrowType type, java.util.List<org.apache.arrow.vector.types.pojo.Field> children) {
        Map<String, FieldType> childTypes = new java.util.LinkedHashMap<>();
        if (children != null) {
            children.forEach(child -> childTypes.put(child.getName(), fromArrow(child.getType(), child.getChildren())));
        }
        return type.getTypeID() == ArrowType.ArrowTypeID.Struct ? new StructType(childTypes) : new UnionType(childTypes);
    }

    /**
     * Converts an Arrow map type.
     * @param children child fields
     * @return converted map type
     * @throws IllegalArgumentException if the map children do not define key and value types
     */
    private static FieldType fromArrowMap(java.util.List<org.apache.arrow.vector.types.pojo.Field> children) {
        if (children == null) {
            throw new IllegalArgumentException("Invalid map-type.");
        }
        if (children.size() == 1) {
            return fromArrowMapChild(children.getFirst());
        }
        if (children.size() == 2) {
            FieldType keyType = fromArrow(children.get(0).getType(), children.get(0).getChildren());
            FieldType valueType = fromArrow(children.get(1).getType(), children.get(1).getChildren());
            return createMapType(keyType, valueType);
        }
        throw new IllegalArgumentException("Invalid map-type.");
    }

    /**
     * Converts the standard struct child of an Arrow map.
     * @param child map child field
     * @return converted map type
     * @throws IllegalArgumentException if the child does not define key and value fields
     */
    private static FieldType fromArrowMapChild(org.apache.arrow.vector.types.pojo.Field child) {
        FieldType mapChildType = fromArrow(child.getType(), child.getChildren());
        if (mapChildType.getTypeID() != IDs.STRUCT) {
            throw new IllegalArgumentException("Invalid map-type.");
        }
        Map<String, FieldType> childTypes = ((StructType) mapChildType).getChildrenType();
        String[] keys = childTypes.keySet().toArray(new String[0]);
        if (keys.length != 2 || !keys[0].equalsIgnoreCase(KEY_FIELD_NAME) || !keys[1].equalsIgnoreCase(VALUE_FIELD_NAME)) {
            throw new IllegalArgumentException("Invalid map-type.");
        }
        return createMapType(childTypes.get(KEY_FIELD_NAME), childTypes.get(VALUE_FIELD_NAME));
    }

    /**
     * Creates a validated map type.
     * @param keyType key field type
     * @param valueType value field type
     * @return converted map type
     * @throws RuntimeException if either field type is missing
     */
    private static FieldType createMapType(FieldType keyType, FieldType valueType) {
        if (keyType == null || valueType == null) {
            throw new RuntimeException("Invalid map-type.");
        }
        return new MapType(keyType, valueType);
    }

    /**
     * Converts an Arrow list type.
     * @param type Arrow list type
     * @param children child fields
     * @return converted list type
     */
    private static FieldType fromArrowList(ArrowType type, java.util.List<org.apache.arrow.vector.types.pojo.Field> children) {
        FieldType childType = children != null && !children.isEmpty()
                ? fromArrow(children.getFirst().getType(), children.getFirst().getChildren()) : null;
        return type.getTypeID() == ArrowType.ArrowTypeID.FixedSizeList
                ? new ListType(((ArrowType.FixedSizeList) type).getListSize(), childType) : new ListType(childType);
    }

    //Convert to Spark DecimalType
    private static final Function<FieldType, org.apache.spark.sql.types.DecimalType> toDecimalType = t -> {
        DecimalType dt = (DecimalType) t;
        return new org.apache.spark.sql.types.DecimalType(dt.getPrecision(), dt.getScale());
    };
    private static final Function<StructType, org.apache.spark.sql.types.DataType> toStructType = t -> {
        org.apache.spark.sql.types.MapType mt = null;
        java.util.List<Map.Entry<String, FieldType>> entries = new java.util.ArrayList<>(t.getChildrenType().entrySet());
        if (entries.size() == 1 && entries.getFirst().getKey().equals("map") && entries.getFirst().getValue().getTypeID() == IDs.LIST) {
            ListType lt = (ListType) entries.getFirst().getValue();
            if (lt.getChildType().getTypeID() == IDs.STRUCT) {
                StructType st = (StructType) lt.getChildType();
                if (st.getTypeID() == IDs.STRUCT) {
                    java.util.List<Map.Entry<String, FieldType>> children = new java.util.ArrayList<>(st.getChildrenType().entrySet());
                    if (children.size() == 2 && children.get(0).getKey().equals(KEY_FIELD_NAME) && children.get(1).getKey().equals(VALUE_FIELD_NAME)) {
                        mt = new org.apache.spark.sql.types.MapType(FieldType.toSpark(children.get(0).getValue()), FieldType.toSpark(children.get(1).getValue()), true);
                    }
                }
            }
        }
        return (mt != null) ? mt : new org.apache.spark.sql.types.StructType(t.getChildrenType().entrySet().stream().map(e -> new org.apache.spark.sql.types.StructField(e.getKey(), FieldType.toSpark(e.getValue()), true, Metadata.empty())).toArray(org.apache.spark.sql.types.StructField[]::new));
    };
    //convert to Spark ListType
    private static final Function<ListType, org.apache.spark.sql.types.ArrayType> toListType = t -> org.apache.spark.sql.types.ArrayType.apply(FieldType.toSpark(t.getChildType()));
    //convert to Spark MapType
    private static final Function<MapType, org.apache.spark.sql.types.MapType> toMapType = t -> org.apache.spark.sql.types.MapType.apply(FieldType.toSpark(t.getKeyType()), FieldType.toSpark(t.getValueType()));

    /**
     * Converts a FieldType to a Spark DataType.
     * @param ft the field type to convert
     * @return corresponding Spark DataType
     */
    public static org.apache.spark.sql.types.DataType toSpark(FieldType ft) {
        return switch (ft.getTypeID()) {
            case INT -> DataTypes.IntegerType;
            case CHAR, VARCHAR, TIME -> DataTypes.StringType;
            case LONG, BIGINT -> DataTypes.LongType;
            case FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case DECIMAL -> toDecimalType.apply(ft);
            case DATE -> DataTypes.DateType;
            case TIMESTAMP -> DataTypes.TimestampType;
            case BOOLEAN -> DataTypes.BooleanType;
            case BYTE -> DataTypes.ByteType;
            case SHORT -> DataTypes.ShortType;
            case BYTES -> DataTypes.BinaryType;
            case PERIOD_YEAR_MONTH ->
                new YearMonthIntervalType(YearMonthIntervalType.YEAR(), YearMonthIntervalType.MONTH());
            case DURATION_DAY_TIME -> new DayTimeIntervalType(DayTimeIntervalType.DAY(), DayTimeIntervalType.SECOND());
            case PERIOD_DURATION_MONTH_DAY_TIME ->
                new org.apache.spark.sql.types.StructType(Arrays.stream(new org.apache.spark.sql.types.StructField[]{
                    new org.apache.spark.sql.types.StructField("period", new YearMonthIntervalType(YearMonthIntervalType.YEAR(), YearMonthIntervalType.MONTH()), true, Metadata.empty()),
                    new org.apache.spark.sql.types.StructField("duration", new DayTimeIntervalType(DayTimeIntervalType.DAY(), DayTimeIntervalType.SECOND()), true, Metadata.empty())
                }).toArray(org.apache.spark.sql.types.StructField[]::new));
            case LIST -> toListType.apply((ListType) ft);
            case MAP -> toMapType.apply((MapType) ft);
            case STRUCT -> toStructType.apply((StructType) ft);
            default -> DataTypes.NullType;
        };
    }
}
