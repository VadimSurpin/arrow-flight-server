package net.surpin.data.arrowflight.client.write;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.*;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.time.format.DateTimeFormatter;

/**
 * The write statement for writing data to remote flight service
 */
public class WriteStatement implements Serializable {
    /**
     * Execute function with three arguments
     */
    @FunctionalInterface
    private interface Execute<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }
    //the values variable
    private static final String VAR_VALUES = "${param_Values}";
    private static final String CAST_FORMAT = "cast(%s as %s)";
    private static final String DECIMAL_TYPE = "decimal";
    private static final String VARCHAR_TYPE = "varchar";
    private static final DateTimeFormatter DATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_SECOND_FORMAT =
        DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIME_MILLI_FORMAT =
        DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter TIME_MICRO_FORMAT =
        DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIME_NANO_FORMAT =
        DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter TIMESTAMP_SECOND_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_MILLI_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter TIMESTAMP_MICRO_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIMESTAMP_NANO_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    //the to-object-converter container (transient — re-initialized lazily after deserialization)
    private transient Map<ConverterKey,
        Execute<Object, Field, DataType, String>> converters;

    //data schema
    private final StructType dataSchema;
    //arrow schema
    private final String arrowSchema;
    //the parameter name in the WriteStatement
    private final String[] params;

    //the statement in the format of either merge into or insert into sql statement
    private String stmt;

    /**
     * Identifies a converter by Spark and Arrow types.
     *
     * @param sparkType Spark type name
     * @param arrowType Arrow minor type
     */
    private record ConverterKey(
        String sparkType,
        Types.MinorType arrowType) {
    }

    /**
     * Creates a converter key.
     *
     * @param dataType Spark data type
     * @param arrowType Arrow minor type
     * @return converter key
     */
    private static ConverterKey converterKey(
        DataType dataType,
        Types.MinorType arrowType) {
        return new ConverterKey(
            dataType.getClass().getTypeName().replaceAll("\\$$", ""),
            arrowType);
    }

    /**
     * Registers a converter for the supplied Spark types.
     *
     * @param arrowType Arrow type
     * @param converter value converter
     * @param sparkTypes supported Spark types
     */
    private void register(
        Types.MinorType arrowType,
        Execute<Object, Field, DataType, String> converter,
        Class<?>... sparkTypes) {
        for (Class<?> sparkType : sparkTypes) {
            converters.put(
                new ConverterKey(sparkType.getTypeName(), arrowType),
                converter);
        }
    }

    /** Registers timestamp and date converters for one Arrow timestamp type. */
    private void registerTimestamp(
        Types.MinorType arrowType,
        DateTimeFormatter formatter,
        boolean timezoneAware) {
        register(arrowType,
            (value, field, type) -> formatTimestamp(
                value, field, formatter, mapTimestamp, timezoneAware),
            TimestampType.class);
        register(arrowType,
            (value, field, type) -> formatDateTimestamp(
                value, field, formatter, mapTimestamp, timezoneAware),
            DateType.class);
    }

    /**
     * Deserialize and reinitialize transient converters
     * @param in object input stream
     * @throws IOException on deserialization failure
     * @throws ClassNotFoundException on missing class
     */
    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.converters = new java.util.HashMap<>();
    }

    //the following is the mapping between arrow-types to data-types that the target flight end-point supports
    private final String mapTinyInt;
    private final String mapSmallInt;
    private final String mapInt;
    private final String mapBigInt;
    private final String mapDate;
    private final String mapTime;
    private final String mapTimestamp;
    private final String mapFloat4;
    private final String mapFloat8;
    private final String mapVarchar;
    private final String mapLargeVarchar;
    private final String mapDecimal;
    private final String mapDecimal256;
    private final String mapUInt1;
    private final String mapUInt2;
    private final String mapUInt4;
    private final String mapUInt8;
    private final String mapBit;

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, StructType dataSchema, Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataSchema, arrowSchema, typeMapping);
        this.stmt = String.format("insert into %s(%s) %s", tableName, String.join(",", WriteStatement.quote.apply(this.params, columnQuote)), WriteStatement.VAR_VALUES);
    }

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param mergeByColumns - the name of columns for merging by
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, String[] mergeByColumns, StructType dataSchema, Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataSchema, arrowSchema, typeMapping);

        Map<String, Integer> entries = new java.util.LinkedHashMap<>();
        for (String field: WriteStatement.quote.apply(this.params, columnQuote)) {
            entries.put(field, 0);
        }
        for (String field: WriteStatement.quote.apply(mergeByColumns, columnQuote)) {
            entries.put(field, 1);
        }
        String matchOn = String.join(" and ", entries.entrySet().stream().filter(e -> e.getValue() == 1).map(e -> String.format("t.%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String setUpdate = String.join(",", entries.entrySet().stream().filter(e -> e.getValue() == 0).map(e -> String.format("%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String varInsert = String.join(",", entries.keySet().toArray(new String[0]));
        String valInsert = String.join(",", entries.keySet().stream().map(integer -> String.format("s.%s", integer)).toArray(String[]::new));
        this.stmt = String.format("merge into %s t using (%s) s(%s) on %s when matched then update set %s when not matched then insert (%s) values(%s)", tableName, WriteStatement.VAR_VALUES, varInsert, matchOn, setUpdate, varInsert, valInsert);
    }

    /**
     * Initialize properties with type mapping
     * @param dataSchema the Spark data schema
     * @param arrowSchema the Arrow schema
     * @param typeMapping mapping from Arrow types to target SQL types
     */
    private WriteStatement(StructType dataSchema, Schema arrowSchema, Map<String, String> typeMapping) {
        this.dataSchema = dataSchema;
        this.arrowSchema = arrowSchema.toJson();
        this.params = dataSchema.fieldNames();
        this.converters = new java.util.HashMap<>();

        this.mapTinyInt = typeMapping.getOrDefault("tinyint", "int");
        this.mapSmallInt = typeMapping.getOrDefault("smallint", "int");
        this.mapInt = typeMapping.getOrDefault("int", "int");
        this.mapBigInt = typeMapping.getOrDefault("bigint", "bigint");
        this.mapDate = typeMapping.getOrDefault("date", "date");
        this.mapTime = typeMapping.getOrDefault("time", "time");
        this.mapTimestamp = typeMapping.getOrDefault("timestamp", "timestamp");
        this.mapFloat4 = typeMapping.getOrDefault("float4", "float");
        this.mapFloat8 = typeMapping.getOrDefault("float8", "double");
        this.mapVarchar = typeMapping.getOrDefault(VARCHAR_TYPE, VARCHAR_TYPE);
        this.mapLargeVarchar = typeMapping.getOrDefault("largevarchar", VARCHAR_TYPE);
        this.mapDecimal = typeMapping.getOrDefault(DECIMAL_TYPE, DECIMAL_TYPE);
        this.mapDecimal256 = typeMapping.getOrDefault("decimal256", DECIMAL_TYPE);
        this.mapUInt1 = typeMapping.getOrDefault("uint1", "int");
        this.mapUInt2 = typeMapping.getOrDefault("uint2", "int");
        this.mapUInt4 = typeMapping.getOrDefault("uint4", "int");
        this.mapUInt8 = typeMapping.getOrDefault("uint8", "int");
        this.mapBit = typeMapping.getOrDefault("bit", "boolean");
    }

    private void initialize() {
        register(Types.MinorType.DATEDAY,
            (value, field, type) -> formatTimestampDate(value, mapDate),
            TimestampType.class);
        register(Types.MinorType.DATEDAY,
            (value, field, type) -> formatDate(value, mapDate),
            DateType.class);
        register(Types.MinorType.DATEMILLI,
            (value, field, type) -> formatTimestampDate(value, mapDate),
            TimestampType.class);
        register(Types.MinorType.DATEMILLI,
            (value, field, type) -> formatDate(value, mapDate),
            DateType.class);
        registerTimestamp(Types.MinorType.TIMESTAMPNANOTZ,
            TIMESTAMP_NANO_FORMAT, true);
        registerTimestamp(Types.MinorType.TIMESTAMPMICROTZ,
            TIMESTAMP_MICRO_FORMAT, true);
        registerTimestamp(Types.MinorType.TIMESTAMPSECTZ,
            TIMESTAMP_SECOND_FORMAT, true);
        registerTimestamp(Types.MinorType.TIMESTAMPMILLITZ,
            TIMESTAMP_MILLI_FORMAT, true);
        registerTimestamp(Types.MinorType.TIMESTAMPNANO,
            TIMESTAMP_NANO_FORMAT, false);
        registerTimestamp(Types.MinorType.TIMESTAMPMICRO,
            TIMESTAMP_MICRO_FORMAT, false);
        registerTimestamp(Types.MinorType.TIMESTAMPSEC,
            TIMESTAMP_SECOND_FORMAT, false);
        registerTimestamp(Types.MinorType.TIMESTAMPMILLI,
            TIMESTAMP_MILLI_FORMAT, false);
        register(Types.MinorType.TIMEMICRO,
            (value, field, type) -> formatStringTime(value, TIME_MICRO_FORMAT, mapTime),
            StringType.class);
        register(Types.MinorType.TIMEMICRO,
            (value, field, type) -> formatTimestampTime(value, TIME_MICRO_FORMAT, mapTime),
            TimestampType.class);
        register(Types.MinorType.TIMESEC,
            (value, field, type) -> formatStringTime(value, TIME_SECOND_FORMAT, mapTime),
            StringType.class);
        register(Types.MinorType.TIMESEC,
            (value, field, type) -> formatTimestampTime(value, TIME_SECOND_FORMAT, mapTime),
            TimestampType.class);
        register(Types.MinorType.TIMENANO,
            (value, field, type) -> formatStringTime(value, TIME_NANO_FORMAT, mapTime),
            StringType.class);
        register(Types.MinorType.TIMENANO,
            (value, field, type) -> formatTimestampTime(value, TIME_NANO_FORMAT, mapTime),
            TimestampType.class);
        register(Types.MinorType.TIMEMILLI,
            (value, field, type) -> formatStringTime(value, TIME_MILLI_FORMAT, mapTime),
            StringType.class);
        register(Types.MinorType.TIMEMILLI,
            (value, field, type) -> formatTimestampTime(value, TIME_MILLI_FORMAT, mapTime),
            TimestampType.class);

        register(
            Types.MinorType.BIGINT,
            (value, field, type) ->
                castNumber(value, mapBigInt),
            ByteType.class,
            ShortType.class,
            IntegerType.class,
            LongType.class);

        register(
            Types.MinorType.FLOAT8,
            (value, field, type) ->
                castNumber(value, mapFloat8),
            ByteType.class,
            ShortType.class,
            IntegerType.class,
            LongType.class,
            FloatType.class,
            DoubleType.class);

        register(
            Types.MinorType.INT,
            (value, field, type) -> castNumber(value, mapInt),
            ByteType.class, ShortType.class, IntegerType.class);
        register(
            Types.MinorType.SMALLINT,
            (value, field, type) -> castNumber(value, mapSmallInt),
            ByteType.class, ShortType.class);
        register(
            Types.MinorType.TINYINT,
            (value, field, type) -> castNumber(value, mapTinyInt),
            ByteType.class);
        register(
            Types.MinorType.UINT1,
            (value, field, type) -> castNumber(value, mapUInt1),
            ByteType.class, ShortType.class);
        register(
            Types.MinorType.UINT2,
            (value, field, type) -> castNumber(value, mapUInt2),
            ByteType.class, ShortType.class);
        register(
            Types.MinorType.UINT4,
            (value, field, type) -> castNumber(value, mapUInt4),
            ByteType.class, ShortType.class, IntegerType.class);
        register(
            Types.MinorType.UINT8,
            (value, field, type) -> castNumber(value, mapUInt8),
            ByteType.class, ShortType.class, IntegerType.class, LongType.class);
        register(
            Types.MinorType.FLOAT4,
            (value, field, type) -> castNumber(value, mapFloat4),
            ByteType.class, ShortType.class, IntegerType.class,
            LongType.class, FloatType.class, DoubleType.class);
        register(
            Types.MinorType.DECIMAL,
            (value, field, type) -> castNumber(value, mapDecimal),
            ByteType.class, ShortType.class, IntegerType.class,
            LongType.class, FloatType.class, DoubleType.class, DecimalType.class);
        register(
            Types.MinorType.DECIMAL256,
            (value, field, type) -> castNumber(value, mapDecimal256),
            ByteType.class, ShortType.class, IntegerType.class,
            LongType.class, FloatType.class, DoubleType.class, DecimalType.class);

        register(
            Types.MinorType.VARCHAR,
            (value, field, type) ->
                primitiveToVarchar(value, mapVarchar),
            StringType.class,
            BooleanType.class,
            ByteType.class,
            ShortType.class,
            IntegerType.class,
            LongType.class,
            FloatType.class,
            DoubleType.class,
            DateType.class,
            TimestampType.class);

        register(
            Types.MinorType.LARGEVARCHAR,
            (value, field, type) ->
                primitiveToVarchar(value, mapLargeVarchar),
            StringType.class, BooleanType.class, ByteType.class, ShortType.class,
            IntegerType.class, LongType.class, FloatType.class, DoubleType.class,
            DateType.class, TimestampType.class, DecimalType.class);
        register(
            Types.MinorType.VARCHAR,
            (value, field, type) -> complexToVarchar(value, type, mapVarchar),
            MapType.class, ArrayType.class, StructType.class);
        register(
            Types.MinorType.LARGEVARCHAR,
            (value, field, type) -> complexToVarchar(value, type, mapLargeVarchar),
            MapType.class, ArrayType.class, StructType.class);
        register(
            Types.MinorType.BIT,
            (value, field, type) -> booleanToBit(value, mapBit),
            BooleanType.class);
        register(
            Types.MinorType.BIT,
            (value, field, type) -> stringToBit(value, mapBit),
            StringType.class);
        register(
            Types.MinorType.BIT,
            (value, field, type) -> numberToBit(value, mapBit),
            ByteType.class, ShortType.class, IntegerType.class, LongType.class,
            FloatType.class, DoubleType.class, DecimalType.class);
    }

    /**
     * Converts a numeric value to a typed SQL literal.
     *
     * @param value source value
     * @param targetType target SQL type
     * @return SQL literal
     */
    static String castNumber(
        Object value,
        String targetType) {
        return String.format(
            CAST_FORMAT,
            value == null ? "null" : value,
            targetType);
    }

    /** Converts a primitive value to a SQL string literal. */
    static String primitiveToVarchar(Object value, String targetType) {
        if (value == null) {
            return String.format(CAST_FORMAT, "null", targetType);
        }
        return String.format("'%s'", value.toString().replace("'", "''"));
    }

    /** Converts a complex Spark value to a SQL string literal. */
    private static String complexToVarchar(
        Object value,
        DataType dataType,
        String targetType) {
        if (value == null) {
            return String.format(CAST_FORMAT, "null", targetType);
        }
        return String.format("'%s'", toJson.apply(value, dataType).replace("'", "''"));
    }

    /** Converts a boolean value to a typed SQL literal. */
    static String booleanToBit(Object value, String targetType) {
        return castNumber(value, targetType);
    }

    /** Converts a numeric value to a typed boolean SQL literal. */
    static String numberToBit(Object value, String targetType) {
        Object literal = value == null
            ? null
            : ((Number) value).doubleValue() != 0;
        return castNumber(literal, targetType);
    }

    /** Converts a string value to a typed boolean SQL literal. */
    static String stringToBit(Object value, String targetType) {
        Object literal = value == null
            ? null
            : Boolean.parseBoolean(value.toString());
        return castNumber(literal, targetType);
    }

    /** Formats a textual time value as a typed SQL literal. */
    private static String formatStringTime(
        Object value,
        DateTimeFormatter formatter,
        String targetType) {
        String formatted = value == null
            ? "null"
            : LocalTime.parse(value.toString()).format(formatter);
        return castQuoted(formatted, targetType);
    }

    /** Formats a Spark timestamp value as a typed time SQL literal. */
    private static String formatTimestampTime(
        Object value,
        DateTimeFormatter formatter,
        String targetType) {
        String formatted = value == null
            ? "null"
            : DateTimeUtils.microsToLocalDateTime(longValue(value)).format(formatter);
        return castQuoted(formatted, targetType);
    }

    /** Formats a Spark timestamp value as a typed date SQL literal. */
    private static String formatTimestampDate(Object value, String targetType) {
        String formatted = value == null
            ? "null"
            : DateTimeUtils.microsToLocalDateTime(longValue(value)).format(DATE_FORMAT);
        return castQuoted(formatted, targetType);
    }

    /** Formats a Spark date value as a typed date SQL literal. */
    private static String formatDate(Object value, String targetType) {
        String formatted = value == null
            ? "null"
            : DateTimeUtils.daysToLocalDate(intValue(value)).format(DATE_FORMAT);
        return castQuoted(formatted, targetType);
    }

    /** Formats a Spark timestamp value as a typed timestamp SQL literal. */
    private static String formatTimestamp(
        Object value,
        Field field,
        DateTimeFormatter formatter,
        String targetType,
        boolean timezoneAware) {
        if (value == null) {
            return castQuoted("null", targetType);
        }
        long micros = longValue(value);
        if (timezoneAware) {
            micros = microsToMicrosTz(micros, field);
        }
        String formatted = DateTimeUtils.microsToLocalDateTime(micros).format(formatter);
        return castQuoted(formatted, targetType);
    }

    /** Formats a Spark date value as a typed timestamp SQL literal. */
    private static String formatDateTimestamp(
        Object value,
        Field field,
        DateTimeFormatter formatter,
        String targetType,
        boolean timezoneAware) {
        if (value == null) {
            return castQuoted("null", targetType);
        }
        int days = intValue(value);
        String formatted = timezoneAware
            ? DateTimeUtils.microsToLocalDateTime(
                daysToMicrosTz(days, field)).format(formatter)
            : DateTimeUtils.daysToLocalDate(days).atStartOfDay().format(formatter);
        return castQuoted(formatted, targetType);
    }

    /** Wraps a quoted value in a typed SQL cast. */
    private static String castQuoted(String value, String targetType) {
        return String.format("cast('%s' as %s)", value, targetType);
    }

    /** Converts an object to a long value. */
    private static long longValue(Object value) {
        return value instanceof Number number
            ? number.longValue()
            : Long.parseLong(value.toString());
    }

    /** Converts an object to an integer value. */
    private static int intValue(Object value) {
        return value instanceof Number number
            ? number.intValue()
            : Integer.parseInt(value.toString());
    }

    /**
     * Get the data schema
     * @return - the data schema
     */
    public StructType getDataSchema() {
        return this.dataSchema;
    }

    /**
     * Get the arrow-schema
     * @return - arrow schema
     * @throws IOException - thrown when the arrow-schema is invalid
     */
    public Schema getArrowSchema() throws IOException {
        return Schema.fromJSON(this.arrowSchema);
    }

    /**
     * Get the statement
     * @return - the merge into or insert into statement
     */
    public String getStatement() {
        return this.stmt.replace(WriteStatement.VAR_VALUES, String.format("values(%s)", String.join(",", Arrays.stream(this.params).map(param -> "?").toArray(String[]::new))));
    }

    /**
     * Fill the statment with data
     * @param rows - the rows of data
     * @param arrowFields - the fields of output
     * @return - a statement with data
     */
    public String fillStatement(InternalRow[] rows, Field[] arrowFields) {
        Function<String, Optional<Field>> find = (name) -> Arrays.stream(arrowFields).filter(x -> x.getName().equalsIgnoreCase(name)).findFirst();
        StructField[] dataFields = this.dataSchema.fields();
        Object[] columns = IntStream.range(0, dataFields.length).mapToObj(idx -> {
            Optional<Field> arrowField = find.apply(dataFields[idx].name());
            if (arrowField.isEmpty()) {
                throw new RuntimeException("The arrow field is not available.");
            }
            return this.fillColumns(rows, idx, dataFields[idx].dataType(), arrowField.get());
        }).toArray(Object[]::new);

        String[] values = IntStream.range(0, rows.length).mapToObj(i -> String.format("(%s)", String.join(",", Arrays.stream(columns).map(column -> ((String[]) column)[i]).toArray(String[]::new)))).toArray(String[]::new);
        return this.stmt.replace(WriteStatement.VAR_VALUES, String.format("values%s", String.join(",", values)));
    }

    /**
     * Convert values of one column to SQL strings
     * @param rows source rows
     * @param idxColumn column index
     * @param dataType Spark data type
     * @param arrowField Arrow field definition
     * @return array of SQL string values
     */
    private String[] fillColumns(InternalRow[] rows, int idxColumn, DataType dataType, Field arrowField) {
        ConverterKey key = converterKey(
            dataType,
            Types.getMinorTypeForArrowType(arrowField.getType()));
        if (!this.converters.containsKey(key)) {
            this.initialize();
        }
        Execute<Object, Field, DataType, String> converter = this.converters.get(key);
        if (converter == null) {
            throw new IllegalArgumentException(
                "Unsupported Spark/Arrow type combination: " + key);
        }
        return Arrays.stream(rows)
            .map(row -> converter.apply(row.get(idxColumn, dataType), arrowField, dataType))
            .toArray(String[]::new);
    }

    /** Converts timestamp microseconds to the Arrow field timezone. */
    private static long microsToMicrosTz(long micros, Field arrowField) {
        return DateTimeUtils.convertTz(
            micros,
            ZoneId.systemDefault(),
            ZoneId.of(((ArrowType.Timestamp) arrowField.getFieldType().getType()).getTimezone()));
    }

    /** Converts date days to microseconds in the Arrow field timezone. */
    private static long daysToMicrosTz(int days, Field arrowField) {
        return DateTimeUtils.daysToMicros(
            days,
            ZoneId.of(((ArrowType.Timestamp) arrowField.getFieldType().getType()).getTimezone()));
    }
    //to-json
    private static final BiFunction<Object, DataType, String> toJson = (o, dt) -> {
        StringBuilder sb = new StringBuilder();
        try {
            if (dt instanceof StructType structType && o instanceof UnsafeRow data) {
                StructField[] fields = structType.fields();
                sb.append(String.format("{ %s }", String.join(", ", IntStream.range(0, fields.length).mapToObj(idx -> String.format("\"%s\": %s", fields[idx].name(), WriteStatement.toJson.apply(data.get(idx, fields[idx].dataType()), fields[idx].dataType()))).toArray(String[]::new))));
            } else if (dt instanceof MapType mt && o instanceof UnsafeMapData data) {
                UnsafeArrayData keys = data.keyArray();
                UnsafeArrayData values = data.valueArray();
                sb.append(String.format("{ \"map\": [%s] }", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> String.format("{ \"key\": %s, \"value\": %s }", WriteStatement.toJson.apply(keys.get(idx, mt.keyType()), mt.keyType()), WriteStatement.toJson.apply(values.get(idx, mt.valueType()), mt.valueType()))).toArray(String[]::new))));
            } else if (dt instanceof ArrayType at && o instanceof UnsafeArrayData data) {
                sb.append(String.format("[%s]", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> WriteStatement.toJson.apply(data.get(idx, at.elementType()), at.elementType())).toArray(String[]::new))));
            } else {
                sb.append(String.format("\"%s\"", o));
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(WriteStatement.class).warn(e.getMessage() + Arrays.toString(e.getStackTrace()));
        }
        return sb.toString().replace("'", "''");
    };
    //quote all fields in the collection
    private static final BiFunction<String[], String, String[]> quote = (fields, quoteChar) -> Arrays.stream(fields).map(field -> String.format("%s%s%s", quoteChar, field, quoteChar)).toArray(String[]::new);
}
