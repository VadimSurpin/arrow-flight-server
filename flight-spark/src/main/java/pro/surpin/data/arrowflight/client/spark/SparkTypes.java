package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.model.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.YearMonthIntervalType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Maps engine-neutral field types to Spark SQL data types.
 */
public final class SparkTypes {

    /**
     * Prevents instantiation.
     */
    private SparkTypes() {
    }

    /**
     * Converts an Arrow schema to a Spark struct type with nullable fields.
     * @param schema Arrow schema
     * @return Spark struct type
     */
    public static StructType toSpark(Schema schema) {
        return new StructType(Arrays.stream(Field.from(schema))
                .map(f -> new StructField(f.getName(), toSpark(f.getType()), true, Metadata.empty()))
                .toArray(StructField[]::new));
    }

    /**
     * Converts Spark fields to neutral fields; unknown types map to NULL.
     * @param fields Spark fields
     * @return neutral fields
     */
    public static Field[] toFields(StructField[] fields) {
        return Arrays.stream(fields).map(f -> new Field(f.name(), fromSpark(f.dataType()))).toArray(Field[]::new);
    }

    /**
     * Converts a Spark data type to a neutral field type.
     * @param type Spark data type
     * @return field type; unknown types map to NULL
     */
    public static FieldType fromSpark(DataType type) {
        if (type instanceof org.apache.spark.sql.types.DecimalType d) {
            return new FieldType.DecimalType(d.precision(), d.scale());
        }
        if (type instanceof ArrayType a) {
            return new FieldType.ListType(fromSpark(a.elementType()));
        }
        if (type instanceof org.apache.spark.sql.types.MapType m) {
            return new FieldType.MapType(fromSpark(m.keyType()), fromSpark(m.valueType()));
        }
        if (type instanceof StructType s) {
            Map<String, FieldType> children = new LinkedHashMap<>();
            for (StructField f : s.fields()) {
                children.put(f.name(), fromSpark(f.dataType()));
            }
            return new FieldType.StructType(children);
        }
        FieldType.IDs id;
        if (type.equals(DataTypes.ByteType)) {
            id = FieldType.IDs.BYTE;
        } else if (type.equals(DataTypes.ShortType)) {
            id = FieldType.IDs.SHORT;
        } else if (type.equals(DataTypes.IntegerType)) {
            id = FieldType.IDs.INT;
        } else if (type.equals(DataTypes.LongType)) {
            id = FieldType.IDs.LONG;
        } else if (type.equals(DataTypes.FloatType)) {
            id = FieldType.IDs.FLOAT;
        } else if (type.equals(DataTypes.DoubleType)) {
            id = FieldType.IDs.DOUBLE;
        } else if (type.equals(DataTypes.BooleanType)) {
            id = FieldType.IDs.BOOLEAN;
        } else if (type.equals(DataTypes.StringType)) {
            id = FieldType.IDs.VARCHAR;
        } else if (type.equals(DataTypes.BinaryType)) {
            id = FieldType.IDs.BYTES;
        } else if (type.equals(DataTypes.DateType)) {
            id = FieldType.IDs.DATE;
        } else if (type.equals(DataTypes.TimestampType)) {
            id = FieldType.IDs.TIMESTAMP;
        } else {
            id = FieldType.IDs.NULL;
        }
        return new FieldType(id);
    }

    /**
     * Converts a field type to a Spark data type.
     * @param ft field type
     * @return corresponding Spark data type
     */
    public static DataType toSpark(FieldType ft) {
        if (ft instanceof FieldType.DecimalType dt) {
            return toDecimal(dt);
        }
        if (ft instanceof FieldType.ListType lt) {
            return ArrayType.apply(toSpark(lt.getChildType()));
        }
        if (ft instanceof FieldType.MapType mt) {
            return toMap(mt);
        }
        if (ft instanceof FieldType.StructType st) {
            return toStruct(st);
        }
        return switch (ft.getTypeID()) {
            case INT -> DataTypes.IntegerType;
            case CHAR, VARCHAR, TIME -> DataTypes.StringType;
            case LONG, BIGINT -> DataTypes.LongType;
            case FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case DATE -> DataTypes.DateType;
            case TIMESTAMP -> DataTypes.TimestampType;
            case BOOLEAN -> DataTypes.BooleanType;
            case BYTE -> DataTypes.ByteType;
            case SHORT -> DataTypes.ShortType;
            case BYTES -> DataTypes.BinaryType;
            case PERIOD_YEAR_MONTH -> yearMonth();
            case DURATION_DAY_TIME -> dayTime();
            case PERIOD_DURATION_MONTH_DAY_TIME -> new StructType(new StructField[] {
                new StructField("period", yearMonth(), true, Metadata.empty()),
                new StructField("duration", dayTime(), true, Metadata.empty())
            });
            default -> DataTypes.NullType;
        };
    }

    /**
     * Converts a decimal field type.
     * @param dt decimal field type
     * @return Spark decimal type
     */
    private static org.apache.spark.sql.types.DecimalType toDecimal(FieldType.DecimalType dt) {
        return new org.apache.spark.sql.types.DecimalType(dt.getPrecision(), dt.getScale());
    }

    /**
     * Creates the Spark year-month interval type.
     * @return year-month interval type
     */
    private static YearMonthIntervalType yearMonth() {
        return new YearMonthIntervalType(YearMonthIntervalType.YEAR(), YearMonthIntervalType.MONTH());
    }

    /**
     * Creates the Spark day-time interval type.
     * @return day-time interval type
     */
    private static DayTimeIntervalType dayTime() {
        return new DayTimeIntervalType(DayTimeIntervalType.DAY(), DayTimeIntervalType.SECOND());
    }

    /**
     * Converts a map field type.
     * @param t map field type
     * @return Spark map type
     */
    private static org.apache.spark.sql.types.MapType toMap(FieldType.MapType t) {
        return org.apache.spark.sql.types.MapType.apply(toSpark(t.getKeyType()), toSpark(t.getValueType()));
    }

    /**
     * Converts a struct field type.
     * @param t struct field type
     * @return Spark struct or map type
     */
    private static DataType toStruct(FieldType.StructType t) {
        org.apache.spark.sql.types.MapType mt = mapEncodedAsStruct(t);
        if (mt != null) {
            return mt;
        }
        return new StructType(t.getChildrenType().entrySet().stream()
                .map(e -> new StructField(e.getKey(), toSpark(e.getValue()), true, Metadata.empty()))
                .toArray(StructField[]::new));
    }

    /**
     * Detects a struct of the form map: list(struct(key, value)) and maps it to a Spark map.
     * @param t struct type
     * @return Spark map type, or null if the struct does not encode a map
     */
    private static org.apache.spark.sql.types.MapType mapEncodedAsStruct(FieldType.StructType t) {
        List<Map.Entry<String, FieldType>> entries = new ArrayList<>(t.getChildrenType().entrySet());
        if (entries.size() != 1 || !entries.getFirst().getKey().equals("map")) {
            return null;
        }
        if (!(entries.getFirst().getValue() instanceof FieldType.ListType list)
                || !(list.getChildType() instanceof FieldType.StructType entry)) {
            return null;
        }
        List<Map.Entry<String, FieldType>> kv = new ArrayList<>(entry.getChildrenType().entrySet());
        if (kv.size() != 2 || !kv.get(0).getKey().equals(FieldType.KEY_FIELD_NAME)
                || !kv.get(1).getKey().equals(FieldType.VALUE_FIELD_NAME)) {
            return null;
        }
        return new org.apache.spark.sql.types.MapType(toSpark(kv.get(0).getValue()), toSpark(kv.get(1).getValue()), true);
    }
}
