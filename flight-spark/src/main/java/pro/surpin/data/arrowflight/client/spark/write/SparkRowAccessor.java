package pro.surpin.data.arrowflight.client.spark.write;

import pro.surpin.data.arrowflight.client.write.RowAccessor;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Exposes a Spark InternalRow through RowAccessor, converting Spark internal values to plain Java values.
 */
public final class SparkRowAccessor implements RowAccessor {
    private final InternalRow row;
    private final StructType schema;

    /**
     * Wraps a row.
     * @param row Spark row; must stay unchanged while the accessor is used
     * @param schema row schema
     */
    public SparkRowAccessor(InternalRow row, StructType schema) {
        this.row = row;
        this.schema = schema;
    }

    @Override
    public Object get(int column) {
        DataType type = schema.fields()[column].dataType();
        return row.isNullAt(column) ? null : toJava(row.get(column, type), type);
    }

    /**
     * Converts a Spark internal value to the plain Java value expected by RowAccessor.
     * @param value Spark internal value
     * @param type Spark type
     * @return Java value
     */
    static Object toJava(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        if (type.equals(DataTypes.StringType)) {
            return value.toString();
        }
        if (type.equals(DataTypes.DateType)) {
            return LocalDate.ofEpochDay(((Number) value).longValue());
        }
        if (type.equals(DataTypes.TimestampType)) {
            return DateTimeUtils.microsToInstant(((Number) value).longValue());
        }
        if (value instanceof Decimal decimal) {
            return decimal.toJavaBigDecimal();
        }
        if (type instanceof StructType struct && value instanceof InternalRow nested) {
            Map<String, Object> fields = new LinkedHashMap<>();
            StructField[] structFields = struct.fields();
            for (int i = 0; i < structFields.length; i++) {
                DataType fieldType = structFields[i].dataType();
                fields.put(structFields[i].name(),
                        nested.isNullAt(i) ? null : toJava(nested.get(i, fieldType), fieldType));
            }
            return fields;
        }
        if (type instanceof ArrayType array && value instanceof ArrayData data) {
            return toList(data, array.elementType());
        }
        if (type instanceof MapType map && value instanceof MapData data) {
            List<Object> keys = toList(data.keyArray(), map.keyType());
            List<Object> values = toList(data.valueArray(), map.valueType());
            Map<Object, Object> entries = new LinkedHashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                entries.put(keys.get(i), values.get(i));
            }
            return entries;
        }
        return value;
    }

    /**
     * Converts Spark array data to a list.
     * @param data Spark array data
     * @param elementType element type
     * @return list of Java values
     */
    private static List<Object> toList(ArrayData data, DataType elementType) {
        List<Object> list = new ArrayList<>(data.numElements());
        for (int i = 0; i < data.numElements(); i++) {
            list.add(data.isNullAt(i) ? null : toJava(data.get(i, elementType), elementType));
        }
        return list;
    }
}
