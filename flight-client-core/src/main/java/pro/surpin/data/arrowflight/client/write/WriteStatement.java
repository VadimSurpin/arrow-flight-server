package pro.surpin.data.arrowflight.client.write;

import pro.surpin.data.arrowflight.client.model.FieldType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Engine-neutral write statement: an INSERT or MERGE template whose values are rendered
 * as typed SQL literals from rows exposed through RowAccessor.
 */
public class WriteStatement implements Serializable {

    /**
     * Converts one value to a SQL literal.
     */
    @FunctionalInterface
    private interface Converter {
        /**
         * Converts a value.
         * @param value neutral value, or null
         * @param field target Arrow field
         * @param type source field type
         * @return SQL literal
         */
        String apply(Object value, Field field, FieldType type);
    }

    /**
     * Identifies a converter by source type and target Arrow type.
     * @param source source type id
     * @param arrowType target Arrow minor type
     */
    private record ConverterKey(FieldType.IDs source, Types.MinorType arrowType) {
    }

    private static final String VAR_VALUES = "${param_Values}";
    private static final String CAST_FORMAT = "cast(%s as %s)";
    private static final String DECIMAL_TYPE = "decimal";
    private static final String VARCHAR_TYPE = "varchar";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_SECOND_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIME_MILLI_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter TIME_MICRO_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIME_NANO_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter TIMESTAMP_SECOND_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_MILLI_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter TIMESTAMP_MICRO_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIMESTAMP_NANO_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    private static final FieldType.IDs[] INTEGRAL = {
        FieldType.IDs.BYTE, FieldType.IDs.SHORT, FieldType.IDs.INT, FieldType.IDs.LONG};
    private static final FieldType.IDs[] NUMERIC = {
        FieldType.IDs.BYTE, FieldType.IDs.SHORT, FieldType.IDs.INT, FieldType.IDs.LONG,
        FieldType.IDs.FLOAT, FieldType.IDs.DOUBLE};

    //the to-literal converters (transient: rebuilt lazily after deserialization)
    private transient Map<ConverterKey, Converter> converters;

    private final pro.surpin.data.arrowflight.client.model.Field[] dataFields;
    private final String arrowSchema;
    private final String[] params;
    private final String stmt;

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
     * Construct an INSERT statement.
     * @param tableName - the name of the table
     * @param dataFields - the written columns and their source types
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     * @param typeMapping - Arrow type name to server SQL type overrides
     */
    public WriteStatement(String tableName, pro.surpin.data.arrowflight.client.model.Field[] dataFields,
                          Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataFields, arrowSchema, typeMapping, insert(tableName, dataFields, columnQuote));
    }

    /**
     * Construct a MERGE statement.
     * @param tableName - the name of the table
     * @param mergeByColumns - the name of columns for merging by
     * @param dataFields - the written columns and their source types
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     * @param typeMapping - Arrow type name to server SQL type overrides
     */
    public WriteStatement(String tableName, String[] mergeByColumns,
                          pro.surpin.data.arrowflight.client.model.Field[] dataFields,
                          Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataFields, arrowSchema, typeMapping, merge(tableName, mergeByColumns, dataFields, columnQuote));
    }

    /**
     * Initialize properties with type mapping.
     * @param dataFields the written columns
     * @param arrowSchema the Arrow schema
     * @param typeMapping mapping from Arrow types to target SQL types
     * @param stmt statement template
     */
    private WriteStatement(pro.surpin.data.arrowflight.client.model.Field[] dataFields, Schema arrowSchema,
                           Map<String, String> typeMapping, String stmt) {
        this.dataFields = dataFields.clone();
        this.arrowSchema = arrowSchema.toJson();
        this.params = names(dataFields);
        this.stmt = stmt;

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

    /**
     * Builds the INSERT template.
     * @param tableName target table
     * @param dataFields written columns
     * @param columnQuote identifier quote
     * @return template
     */
    private static String insert(String tableName, pro.surpin.data.arrowflight.client.model.Field[] dataFields,
                                 String columnQuote) {
        return String.format("insert into %s(%s) %s", tableName,
                String.join(",", quote(names(dataFields), columnQuote)), VAR_VALUES);
    }

    /**
     * Builds the MERGE template.
     * @param tableName target table
     * @param mergeByColumns merge key columns
     * @param dataFields written columns
     * @param columnQuote identifier quote
     * @return template
     */
    private static String merge(String tableName, String[] mergeByColumns,
                                pro.surpin.data.arrowflight.client.model.Field[] dataFields, String columnQuote) {
        Map<String, Integer> entries = new LinkedHashMap<>();
        for (String field : quote(names(dataFields), columnQuote)) {
            entries.put(field, 0);
        }
        for (String field : quote(mergeByColumns, columnQuote)) {
            entries.put(field, 1);
        }
        String matchOn = entries.entrySet().stream().filter(e -> e.getValue() == 1)
                .map(e -> String.format("t.%s = s.%s", e.getKey(), e.getKey())).collect(Collectors.joining(" and "));
        String setUpdate = entries.entrySet().stream().filter(e -> e.getValue() == 0)
                .map(e -> String.format("%s = s.%s", e.getKey(), e.getKey())).collect(Collectors.joining(","));
        String varInsert = String.join(",", entries.keySet());
        String valInsert = entries.keySet().stream().map(k -> "s." + k).collect(Collectors.joining(","));
        return String.format("merge into %s t using (%s) s(%s) on %s when matched then update set %s "
                + "when not matched then insert (%s) values(%s)",
                tableName, VAR_VALUES, varInsert, matchOn, setUpdate, varInsert, valInsert);
    }

    /**
     * Rebuilds transient state after deserialization.
     * @param in object input stream
     * @throws IOException on deserialization failure
     * @throws ClassNotFoundException on missing class
     */
    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.converters = null;
    }

    /**
     * Returns the written columns.
     * @return data fields
     */
    public pro.surpin.data.arrowflight.client.model.Field[] getDataFields() {
        return this.dataFields.clone();
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
     * Get the statement with one positional parameter per column.
     * @return - the merge into or insert into statement
     */
    public String getStatement() {
        return this.stmt.replace(VAR_VALUES, String.format("values(%s)",
                String.join(",", Arrays.stream(this.params).map(param -> "?").toArray(String[]::new))));
    }

    /**
     * Fill the statement with literal values.
     * @param rows - the rows of data
     * @param arrowFields - the target Arrow fields, matched to data fields by name ignoring case
     * @return - a statement with data
     * @throws IllegalArgumentException if a column has no Arrow field or its type pair is unsupported
     */
    public String fillStatement(List<? extends RowAccessor> rows, Field[] arrowFields) {
        String[][] columns = new String[dataFields.length][];
        for (int idx = 0; idx < dataFields.length; idx++) {
            String name = dataFields[idx].getName();
            Optional<Field> arrowField = Arrays.stream(arrowFields)
                    .filter(x -> x.getName().equalsIgnoreCase(name)).findFirst();
            if (arrowField.isEmpty()) {
                throw new IllegalArgumentException("The arrow field is not available.");
            }
            columns[idx] = fillColumn(rows, idx, dataFields[idx].getType(), arrowField.get());
        }
        String[] values = new String[rows.size()];
        for (int i = 0; i < values.length; i++) {
            String[] row = new String[columns.length];
            for (int c = 0; c < columns.length; c++) {
                row[c] = columns[c][i];
            }
            values[i] = String.format("(%s)", String.join(",", row));
        }
        return this.stmt.replace(VAR_VALUES, String.format("values%s", String.join(",", values)));
    }

    /**
     * Converts values of one column to SQL literals.
     * @param rows source rows
     * @param idxColumn column index
     * @param type source type
     * @param arrowField target Arrow field
     * @return SQL literals
     */
    private String[] fillColumn(List<? extends RowAccessor> rows, int idxColumn, FieldType type, Field arrowField) {
        ConverterKey key = new ConverterKey(type.getTypeID(), Types.getMinorTypeForArrowType(arrowField.getType()));
        Converter converter = converters().get(key);
        if (converter == null) {
            throw new IllegalArgumentException("Unsupported source/Arrow type combination: " + key);
        }
        return rows.stream().map(row -> converter.apply(row.get(idxColumn), arrowField, type)).toArray(String[]::new);
    }

    /**
     * Returns the converter registry, building it on first use.
     * @return converters
     */
    private Map<ConverterKey, Converter> converters() {
        if (this.converters == null) {
            this.converters = buildConverters();
        }
        return this.converters;
    }

    /**
     * Builds the converter registry for the configured type mapping.
     * @return converters
     */
    private Map<ConverterKey, Converter> buildConverters() {
        Map<ConverterKey, Converter> map = new HashMap<>();
        for (Types.MinorType dateType : new Types.MinorType[] {Types.MinorType.DATEDAY, Types.MinorType.DATEMILLI}) {
            register(map, dateType, (v, f, t) -> formatTimestampDate((Instant) v, mapDate), FieldType.IDs.TIMESTAMP);
            register(map, dateType, (v, f, t) -> formatDate((LocalDate) v, mapDate), FieldType.IDs.DATE);
        }
        registerTimestamp(map, Types.MinorType.TIMESTAMPNANOTZ, TIMESTAMP_NANO_FORMAT, true);
        registerTimestamp(map, Types.MinorType.TIMESTAMPMICROTZ, TIMESTAMP_MICRO_FORMAT, true);
        registerTimestamp(map, Types.MinorType.TIMESTAMPSECTZ, TIMESTAMP_SECOND_FORMAT, true);
        registerTimestamp(map, Types.MinorType.TIMESTAMPMILLITZ, TIMESTAMP_MILLI_FORMAT, true);
        registerTimestamp(map, Types.MinorType.TIMESTAMPNANO, TIMESTAMP_NANO_FORMAT, false);
        registerTimestamp(map, Types.MinorType.TIMESTAMPMICRO, TIMESTAMP_MICRO_FORMAT, false);
        registerTimestamp(map, Types.MinorType.TIMESTAMPSEC, TIMESTAMP_SECOND_FORMAT, false);
        registerTimestamp(map, Types.MinorType.TIMESTAMPMILLI, TIMESTAMP_MILLI_FORMAT, false);
        registerTime(map, Types.MinorType.TIMESEC, TIME_SECOND_FORMAT);
        registerTime(map, Types.MinorType.TIMEMILLI, TIME_MILLI_FORMAT);
        registerTime(map, Types.MinorType.TIMEMICRO, TIME_MICRO_FORMAT);
        registerTime(map, Types.MinorType.TIMENANO, TIME_NANO_FORMAT);

        register(map, Types.MinorType.BIGINT, (v, f, t) -> castNumber(v, mapBigInt), INTEGRAL);
        register(map, Types.MinorType.FLOAT8, (v, f, t) -> castNumber(v, mapFloat8), NUMERIC);
        register(map, Types.MinorType.INT, (v, f, t) -> castNumber(v, mapInt),
                FieldType.IDs.BYTE, FieldType.IDs.SHORT, FieldType.IDs.INT);
        register(map, Types.MinorType.SMALLINT, (v, f, t) -> castNumber(v, mapSmallInt),
                FieldType.IDs.BYTE, FieldType.IDs.SHORT);
        register(map, Types.MinorType.TINYINT, (v, f, t) -> castNumber(v, mapTinyInt), FieldType.IDs.BYTE);
        register(map, Types.MinorType.UINT1, (v, f, t) -> castNumber(v, mapUInt1),
                FieldType.IDs.BYTE, FieldType.IDs.SHORT);
        register(map, Types.MinorType.UINT2, (v, f, t) -> castNumber(v, mapUInt2),
                FieldType.IDs.BYTE, FieldType.IDs.SHORT);
        register(map, Types.MinorType.UINT4, (v, f, t) -> castNumber(v, mapUInt4),
                FieldType.IDs.BYTE, FieldType.IDs.SHORT, FieldType.IDs.INT);
        register(map, Types.MinorType.UINT8, (v, f, t) -> castNumber(v, mapUInt8), INTEGRAL);
        register(map, Types.MinorType.FLOAT4, (v, f, t) -> castNumber(v, mapFloat4), NUMERIC);
        register(map, Types.MinorType.DECIMAL, (v, f, t) -> castNumber(v, mapDecimal), NUMERIC);
        register(map, Types.MinorType.DECIMAL, (v, f, t) -> castNumber(v, mapDecimal), FieldType.IDs.DECIMAL);
        register(map, Types.MinorType.DECIMAL256, (v, f, t) -> castNumber(v, mapDecimal256), NUMERIC);
        register(map, Types.MinorType.DECIMAL256, (v, f, t) -> castNumber(v, mapDecimal256), FieldType.IDs.DECIMAL);

        FieldType.IDs[] varcharSources = {
            FieldType.IDs.VARCHAR, FieldType.IDs.BOOLEAN, FieldType.IDs.BYTE, FieldType.IDs.SHORT,
            FieldType.IDs.INT, FieldType.IDs.LONG, FieldType.IDs.FLOAT, FieldType.IDs.DOUBLE,
            FieldType.IDs.DATE, FieldType.IDs.TIMESTAMP};
        register(map, Types.MinorType.VARCHAR, (v, f, t) -> primitiveToVarchar(v, mapVarchar), varcharSources);
        register(map, Types.MinorType.LARGEVARCHAR, (v, f, t) -> primitiveToVarchar(v, mapLargeVarchar), varcharSources);
        register(map, Types.MinorType.LARGEVARCHAR, (v, f, t) -> primitiveToVarchar(v, mapLargeVarchar),
                FieldType.IDs.DECIMAL);
        FieldType.IDs[] complex = {FieldType.IDs.MAP, FieldType.IDs.LIST, FieldType.IDs.STRUCT};
        register(map, Types.MinorType.VARCHAR, (v, f, t) -> complexToVarchar(v, t, mapVarchar), complex);
        register(map, Types.MinorType.LARGEVARCHAR, (v, f, t) -> complexToVarchar(v, t, mapLargeVarchar), complex);

        register(map, Types.MinorType.BIT, (v, f, t) -> booleanToBit(v, mapBit), FieldType.IDs.BOOLEAN);
        register(map, Types.MinorType.BIT, (v, f, t) -> stringToBit(v, mapBit), FieldType.IDs.VARCHAR);
        register(map, Types.MinorType.BIT, (v, f, t) -> numberToBit(v, mapBit), NUMERIC);
        register(map, Types.MinorType.BIT, (v, f, t) -> numberToBit(v, mapBit), FieldType.IDs.DECIMAL);
        return map;
    }

    /**
     * Registers a converter for several source types.
     * @param map registry
     * @param arrowType target Arrow type
     * @param converter converter
     * @param sources source type ids
     */
    private static void register(Map<ConverterKey, Converter> map, Types.MinorType arrowType,
                                 Converter converter, FieldType.IDs... sources) {
        for (FieldType.IDs source : sources) {
            map.put(new ConverterKey(source, arrowType), converter);
        }
    }

    /**
     * Registers timestamp and date converters for one Arrow timestamp type.
     * @param map registry
     * @param arrowType Arrow timestamp type
     * @param formatter output format
     * @param timezoneAware whether the Arrow type carries a time zone
     */
    private void registerTimestamp(Map<ConverterKey, Converter> map, Types.MinorType arrowType,
                                   DateTimeFormatter formatter, boolean timezoneAware) {
        register(map, arrowType,
                (v, f, t) -> formatTimestamp((Instant) v, f, formatter, mapTimestamp, timezoneAware),
                FieldType.IDs.TIMESTAMP);
        register(map, arrowType,
                (v, f, t) -> formatDateTimestamp((LocalDate) v, f, formatter, mapTimestamp, timezoneAware),
                FieldType.IDs.DATE);
    }

    /**
     * Registers string and timestamp converters for one Arrow time type.
     * @param map registry
     * @param arrowType Arrow time type
     * @param formatter output format
     */
    private void registerTime(Map<ConverterKey, Converter> map, Types.MinorType arrowType, DateTimeFormatter formatter) {
        register(map, arrowType, (v, f, t) -> formatStringTime(v, formatter, mapTime), FieldType.IDs.VARCHAR);
        register(map, arrowType, (v, f, t) -> formatTimestampTime((Instant) v, formatter, mapTime),
                FieldType.IDs.TIMESTAMP);
    }

    /**
     * Converts a numeric value to a typed SQL literal.
     * @param value source value
     * @param targetType target SQL type
     * @return SQL literal
     */
    static String castNumber(Object value, String targetType) {
        return String.format(CAST_FORMAT, value == null ? "null" : value, targetType);
    }

    /**
     * Converts a primitive value to a SQL string literal.
     * @param value source value
     * @param targetType target SQL type used for null
     * @return SQL literal
     */
    static String primitiveToVarchar(Object value, String targetType) {
        if (value == null) {
            return String.format(CAST_FORMAT, "null", targetType);
        }
        return quoteString(value.toString());
    }

    /**
     * Converts a complex value to a JSON string literal.
     * @param value list, map or struct value
     * @param type source type
     * @param targetType target SQL type used for null
     * @return SQL literal
     */
    private static String complexToVarchar(Object value, FieldType type, String targetType) {
        if (value == null) {
            return String.format(CAST_FORMAT, "null", targetType);
        }
        return quoteString(toJson(value, type));
    }

    /**
     * Converts a boolean value to a typed SQL literal.
     * @param value source value
     * @param targetType target SQL type
     * @return SQL literal
     */
    static String booleanToBit(Object value, String targetType) {
        return castNumber(value, targetType);
    }

    /**
     * Converts a numeric value to a typed boolean SQL literal.
     * @param value source value
     * @param targetType target SQL type
     * @return SQL literal
     */
    static String numberToBit(Object value, String targetType) {
        Object literal = value == null ? null : ((Number) value).doubleValue() != 0;
        return castNumber(literal, targetType);
    }

    /**
     * Converts a string value to a typed boolean SQL literal.
     * @param value source value
     * @param targetType target SQL type
     * @return SQL literal
     */
    static String stringToBit(Object value, String targetType) {
        Object literal = value == null ? null : Boolean.parseBoolean(value.toString());
        return castNumber(literal, targetType);
    }

    /**
     * Formats a textual time value.
     * @param value time text such as 01:02:03
     * @param formatter output format
     * @param targetType target SQL type
     * @return SQL literal
     */
    private static String formatStringTime(Object value, DateTimeFormatter formatter, String targetType) {
        return value == null ? castNull(targetType)
                : castQuoted(LocalTime.parse(value.toString()).format(formatter), targetType);
    }

    /**
     * Formats the UTC time-of-day of a timestamp.
     * @param value timestamp
     * @param formatter output format
     * @param targetType target SQL type
     * @return SQL literal
     */
    private static String formatTimestampTime(Instant value, DateTimeFormatter formatter, String targetType) {
        return value == null ? castNull(targetType) : castQuoted(utc(value).format(formatter), targetType);
    }

    /**
     * Formats the UTC date of a timestamp.
     * @param value timestamp
     * @param targetType target SQL type
     * @return SQL literal
     */
    private static String formatTimestampDate(Instant value, String targetType) {
        return value == null ? castNull(targetType) : castQuoted(utc(value).format(DATE_FORMAT), targetType);
    }

    /**
     * Formats a date.
     * @param value date
     * @param targetType target SQL type
     * @return SQL literal
     */
    private static String formatDate(LocalDate value, String targetType) {
        return value == null ? castNull(targetType) : castQuoted(value.format(DATE_FORMAT), targetType);
    }

    /**
     * Formats a timestamp; for zoned Arrow types the UTC wall clock is read in the JVM zone
     * and shifted to the Arrow field zone.
     * @param value timestamp
     * @param field target Arrow field
     * @param formatter output format
     * @param targetType target SQL type
     * @param timezoneAware whether the Arrow type carries a time zone
     * @return SQL literal
     */
    private static String formatTimestamp(Instant value, Field field, DateTimeFormatter formatter,
                                          String targetType, boolean timezoneAware) {
        if (value == null) {
            return castNull(targetType);
        }
        LocalDateTime wall = utc(value);
        if (timezoneAware) {
            wall = wall.atZone(ZoneId.systemDefault()).withZoneSameInstant(fieldZone(field)).toLocalDateTime();
        }
        return castQuoted(wall.format(formatter), targetType);
    }

    /**
     * Formats a date as a timestamp at the start of the day.
     * @param value date
     * @param field target Arrow field
     * @param formatter output format
     * @param targetType target SQL type
     * @param timezoneAware whether the Arrow type carries a time zone
     * @return SQL literal
     */
    private static String formatDateTimestamp(LocalDate value, Field field, DateTimeFormatter formatter,
                                              String targetType, boolean timezoneAware) {
        if (value == null) {
            return castNull(targetType);
        }
        LocalDateTime start = timezoneAware
                ? utc(value.atStartOfDay(fieldZone(field)).toInstant())
                : value.atStartOfDay();
        return castQuoted(start.format(formatter), targetType);
    }

    /**
     * Returns the UTC wall clock of an instant.
     * @param value instant
     * @return local date-time in UTC
     */
    private static LocalDateTime utc(Instant value) {
        return LocalDateTime.ofInstant(value, ZoneOffset.UTC);
    }

    /**
     * Returns the time zone of an Arrow timestamp field.
     * @param field Arrow field
     * @return zone
     */
    private static ZoneId fieldZone(Field field) {
        return ZoneId.of(((ArrowType.Timestamp) field.getFieldType().getType()).getTimezone());
    }

    /**
     * Wraps a quoted value in a typed SQL cast.
     * @param value literal text
     * @param targetType target SQL type
     * @return SQL literal
     */
    private static String castQuoted(String value, String targetType) {
        return String.format("cast('%s' as %s)", value, targetType);
    }

    /**
     * Returns a typed SQL NULL.
     * @param targetType target SQL type
     * @return SQL literal
     */
    private static String castNull(String targetType) {
        return String.format(CAST_FORMAT, "null", targetType);
    }

    /**
     * Quotes a string literal.
     * @param value text
     * @return quoted literal
     */
    private static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Renders a complex value as JSON-like text.
     * @param value value
     * @param type value type
     * @return text
     */
    private static String toJson(Object value, FieldType type) {
        if (type instanceof FieldType.StructType st && value instanceof Map<?, ?> struct) {
            return "{ " + st.getChildrenType().entrySet().stream()
                    .map(e -> String.format("\"%s\": %s", e.getKey(), toJson(struct.get(e.getKey()), e.getValue())))
                    .collect(Collectors.joining(", ")) + " }";
        }
        if (type instanceof FieldType.MapType mt && value instanceof Map<?, ?> entries) {
            return "{ \"map\": [" + entries.entrySet().stream()
                    .map(e -> String.format("{ \"key\": %s, \"value\": %s }",
                            toJson(e.getKey(), mt.getKeyType()), toJson(e.getValue(), mt.getValueType())))
                    .collect(Collectors.joining(", ")) + "] }";
        }
        if (type instanceof FieldType.ListType lt && value instanceof List<?> list) {
            return "[" + list.stream().map(item -> toJson(item, lt.getChildType()))
                    .collect(Collectors.joining(", ")) + "]";
        }
        return String.format("\"%s\"", value);
    }

    /**
     * Returns field names.
     * @param fields fields
     * @return names
     */
    private static String[] names(pro.surpin.data.arrowflight.client.model.Field[] fields) {
        return Arrays.stream(fields).map(pro.surpin.data.arrowflight.client.model.Field::getName).toArray(String[]::new);
    }

    /**
     * Quotes identifiers.
     * @param fields identifiers
     * @param quoteChar quote string
     * @return quoted identifiers
     */
    private static String[] quote(String[] fields, String quoteChar) {
        return Arrays.stream(fields).map(field -> String.format("%s%s%s", quoteChar, field, quoteChar))
                .toArray(String[]::new);
    }
}
