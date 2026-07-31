package pro.surpin.data.arrowflight.server.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.SchemaConverter;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;

import static org.apache.arrow.vector.ipc.message.MessageSerializer.serializeMetadata;
import static java.util.Collections.singletonList;

/**
 * Handles schema/table discovery and builds Flight SQL metadata responses.
 * Wraps ParquetAdapter schema methods and Arrow vector building.
 */
public final class MetadataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataService.class);

    public static final String CATALOG_NAME = "PARQUET_ARROW_FLIGHT_CATALOG";
    public static final String TABLE_TYPE = "TABLE";
    private static final String COLUMN_TABLE_CATALOG = "TABLE_CATALOG";
    private static final String VECTOR_CATALOG_NAME = "catalog_name";
    private static final String COLUMN_TABLE_SCHEMA = "TABLE_SCHEM";
    private static final String COLUMN_TABLE_TYPE = "TABLE_TYPE";

    private final ParquetAdapter parquetAdapter;

    /**
     * Creates MetadataService.
     *
     * @param parquetAdapter Parquet metadata adapter
     */
    public MetadataService(ParquetAdapter parquetAdapter) {
        this.parquetAdapter = parquetAdapter;
    }

    /**
     * Lists schemas matching an optional filter pattern.
     *
     * @param filterPattern SQL LIKE pattern, null for all
     * @return map of schema name to path
     * @throws IOException on HDFS read failure
     */
    public Map<String, org.apache.hadoop.fs.Path> getSchemas(String filterPattern) throws IOException {
        return parquetAdapter.getSchemas(filterPattern);
    }

    /**
     * Lists tables in a schema matching an optional filter pattern.
     *
     * @param schema        schema name
     * @param filterPattern SQL LIKE pattern, null for all
     * @return map of table name to path
     */
    public Map<String, org.apache.hadoop.fs.Path> getTables(String schema, String filterPattern) {
        return parquetAdapter.getTables(schema, filterPattern);
    }

    /**
     * Returns the Arrow schema for a table, optionally filtered to specific columns.
     *
     * @param schema  schema name
     * @param table   table name
     * @param columns columns to include, null for all
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table, List<String> columns) {
        return parquetAdapter.getTableSchema(schema, table, columns);
    }

    /**
     * Returns the Arrow schema for a SQL query result.
     * Handles regular SELECT, aggregation, and JOIN queries.
     *
     * @param query SQL query
     * @return Arrow schema
     */
    public Schema getQuerySchema(String query) {
        long t = LogUtil.mark();
        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        Schema schema;
        if (pq.isJoin) {
            schema = buildJoinSchema(pq);
        } else {
            schema = pq.hasAggregation
                    ? buildAggregationSchema(pq)
                    : parquetAdapter.getTableSchema(pq.schema, pq.table, pq.columns);
        }
        LogUtil.logTiming(t, "schema.getQuerySchema", "join=" + pq.isJoin + " agg=" + pq.hasAggregation);
        return schema;
    }

    /**
     * Converts an Arrow schema to DuckDB-compatible DDL.
     *
     * @param tableSchema schema name
     * @param tableName   table name
     * @param schema      Arrow schema
     * @return DDL string
     */
    public String arrowSchemaToDDL(String tableSchema, String tableName, Schema schema) {
        return parquetAdapter.arrowSchemaToDDL(tableSchema, tableName, schema);
    }

    /**
     * Returns the DDL cache for all tables.
     *
     * @return map of schema to table to DDL
     */
    public Map<String, Map<String, String>> tableDdlCache() {
        return parquetAdapter.tableDdlCache();
    }

    // ── Flight SQL metadata response builders ─────────────────────────────

    /**
     * Builds a VectorSchemaRoot for GetCatalogs response.
     *
     * @param allocator Arrow allocator
     * @return catalog VSR
     */
    public VectorSchemaRoot getCatalogsRoot(BufferAllocator allocator) {
        List<Map<String, Object>> data = List.of(
                Map.of(COLUMN_TABLE_CATALOG, CATALOG_NAME));
        return getRoot(data, allocator, VECTOR_CATALOG_NAME, COLUMN_TABLE_CATALOG);
    }

    /**
     * Builds a VectorSchemaRoot for GetSchemas response.
     *
     * @param schemas   schema names
     * @param allocator Arrow allocator
     * @return schemas VSR
     */
    public VectorSchemaRoot getSchemasRoot(Collection<String> schemas, BufferAllocator allocator) {
        final VarCharVector catalogsVector = new VarCharVector(VECTOR_CATALOG_NAME, allocator);
        final VarCharVector schemasVector = new VarCharVector("db_schema_name",
                FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);

        final List<FieldVector> vectors = ImmutableList.of(catalogsVector, schemasVector);
        vectors.forEach(FieldVector::allocateNew);

        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogsVector, COLUMN_TABLE_CATALOG,
                schemasVector, COLUMN_TABLE_SCHEMA);

        List<? extends Map<String, ?>> data = schemas.stream().map(schema -> Map.of(
                COLUMN_TABLE_CATALOG, CATALOG_NAME,
                COLUMN_TABLE_SCHEMA, schema)).toList();

        saveToVectors(vectorToColumnName, data);
        final int rows = vectors.stream()
                .map(FieldVector::getValueCount)
                .findAny()
                .orElseThrow(IllegalStateException::new);
        vectors.forEach(vector -> vector.setValueCount(rows));
        return new VectorSchemaRoot(vectors);
    }

    /**
     * Builds a VectorSchemaRoot for GetTableTypes response.
     *
     * @param allocator Arrow allocator
     * @return table types VSR
     */
    public VectorSchemaRoot getTableTypesRoot(BufferAllocator allocator) {
        List<Map<String, Object>> data = List.of(
                Map.of(COLUMN_TABLE_TYPE, TABLE_TYPE));
        return getRoot(data, allocator, "table_type", COLUMN_TABLE_TYPE);
    }

    /**
     * Builds a VectorSchemaRoot for GetTables response.
     *
     * @param tables             map of schema to table name to Arrow schema
     * @param allocator          Arrow allocator
     * @param includeSchema      whether to include serialized Arrow schema
     * @param schemaFilterPattern optional schema LIKE filter
     * @param tableFilterPattern  optional table LIKE filter
     * @return tables VSR
     */
    public VectorSchemaRoot getTablesRoot(
            Map<String, Map<String, Schema>> tables,
            BufferAllocator allocator,
            boolean includeSchema,
            String schemaFilterPattern,
            String tableFilterPattern) {
        Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");

        Predicate<String> schemaNamePredicate = createLikePredicate(schemaFilterPattern);
        Predicate<String> tableNamePredicate = createLikePredicate(tableFilterPattern);

        final VarCharVector catalogNameVector = new VarCharVector(VECTOR_CATALOG_NAME, allocator);
        final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
        final VarCharVector tableNameVector = new VarCharVector("table_name",
                FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        final VarCharVector tableTypeVector = new VarCharVector("table_type",
                FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        final VarBinaryVector tableSchemaVector = includeSchema
                ? new VarBinaryVector("table_schema",
                        FieldType.notNullable(Types.MinorType.VARBINARY.getType()), allocator)
                : null;

        final List<FieldVector> vectors = new ArrayList<>(4);
        vectors.add(catalogNameVector);
        vectors.add(schemaNameVector);
        vectors.add(tableNameVector);
        vectors.add(tableTypeVector);
        if (tableSchemaVector != null) {
            vectors.add(tableSchemaVector);
        }
        vectors.forEach(FieldVector::allocateNew);

        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogNameVector, "TABLE_CAT",
                schemaNameVector, COLUMN_TABLE_SCHEMA,
                tableNameVector, "TABLE_NAME",
                tableTypeVector, COLUMN_TABLE_TYPE);

        List<Map<String, ?>> data = new LinkedList<>();
        for (Map.Entry<String, Map<String, Schema>> schemaEntry : tables.entrySet()) {
            String schemaName = schemaEntry.getKey();
            if (!schemaNamePredicate.test(schemaName)) {
                continue;
            }
            for (Map.Entry<String, Schema> tableEntry : schemaEntry.getValue().entrySet()) {
                String tableName = tableEntry.getKey();
                if (!tableNamePredicate.test(tableName)) {
                    continue;
                }
                data.add(Map.of(
                        "TABLE_CAT", CATALOG_NAME,
                        COLUMN_TABLE_SCHEMA, schemaName,
                        "TABLE_NAME", tableName,
                        COLUMN_TABLE_TYPE, TABLE_TYPE));
                if (tableSchemaVector != null) {
                    java.nio.ByteBuffer buf = serializeMetadata(
                            tableEntry.getValue(), IpcOption.DEFAULT);
                    byte[] schemaBytes = new byte[buf.remaining()];
                    buf.get(schemaBytes);
                    saveToVector(schemaBytes, tableSchemaVector, data.size() - 1);
                }
            }
        }
        saveToVectors(vectorToColumnName, data);
        final int rows = vectors.stream()
                .map(FieldVector::getValueCount)
                .findAny()
                .orElseThrow(IllegalStateException::new);
        vectors.forEach(vector -> vector.setValueCount(rows));
        return new VectorSchemaRoot(vectors);
    }

    /**
     * Builds a VectorSchemaRoot for GetXdbcTypeInfo response.
     *
     * @param request   Flight SQL type info request
     * @param allocator Arrow allocator
     * @return type info VSR
     */
    public VectorSchemaRoot getTypeInfoRoot(
            FlightSql.CommandGetXdbcTypeInfo request, BufferAllocator allocator) {
        Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");

        VectorSchemaRoot root = VectorSchemaRoot.create(
                FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA, allocator);

        Map<FieldVector, String> mapper = new HashMap<>();
        mapper.put(root.getVector("type_name"), "TYPE_NAME");
        mapper.put(root.getVector("data_type"), "DATA_TYPE");
        mapper.put(root.getVector("column_size"), "PRECISION");
        mapper.put(root.getVector("literal_prefix"), "LITERAL_PREFIX");
        mapper.put(root.getVector("literal_suffix"), "LITERAL_SUFFIX");
        mapper.put(root.getVector("create_params"), "CREATE_PARAMS");
        mapper.put(root.getVector("nullable"), "NULLABLE");
        mapper.put(root.getVector("case_sensitive"), "CASE_SENSITIVE");
        mapper.put(root.getVector("searchable"), "SEARCHABLE");
        mapper.put(root.getVector("unsigned_attribute"), "UNSIGNED_ATTRIBUTE");
        mapper.put(root.getVector("fixed_prec_scale"), "FIXED_PREC_SCALE");
        mapper.put(root.getVector("auto_increment"), "AUTO_INCREMENT");
        mapper.put(root.getVector("local_type_name"), "LOCAL_TYPE_NAME");
        mapper.put(root.getVector("minimum_scale"), "MINIMUM_SCALE");
        mapper.put(root.getVector("maximum_scale"), "MAXIMUM_SCALE");
        mapper.put(root.getVector("sql_data_type"), "SQL_DATA_TYPE");
        mapper.put(root.getVector("datetime_subcode"), "SQL_DATETIME_SUB");
        mapper.put(root.getVector("num_prec_radix"), "NUM_PREC_RADIX");

        List<? extends Map<String, ?>> types = SchemaConverter.buildTypeInfo().stream()
                .filter(type -> !request.hasDataType() || type.dataType() == request.getDataType())
                .map(SchemaConverter.TypeInfo::toMap).toList();

        saveToVectors(mapper, types);
        return root;
    }

    /**
     * Creates a LIKE predicate from a SQL pattern.
     *
     * @param sqlPattern SQL LIKE pattern, null matches all
     * @return predicate
     */
    public static Predicate<String> createLikePredicate(String sqlPattern) {
        if (sqlPattern == null) {
            return s -> true;
        }
        String regex = "^" + Pattern.quote(sqlPattern)
                .replace("%", "\\E.*\\Q")
                .replace("_", "\\E.\\Q") + "$";
        return Pattern.compile(regex).asPredicate();
    }

    // ── private helpers ───────────────────────────────────────────────────

    /**
     * Builds Arrow schema for a JOIN query result.
     *
     * @param pq parsed join query
     * @return Arrow schema
     */
    @SuppressWarnings("java:S3776")
    private Schema buildJoinSchema(ParquetQueryParser pq) {
        long t = LogUtil.mark();
        Map<String, Schema> aliasSchemas = new LinkedHashMap<>();
        for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
            aliasSchemas.put(jt.alias(), parquetAdapter.getTableSchema(jt.schema(), jt.table()));
        }
        List<Field> resultFields = new ArrayList<>();
        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            String col = expr.inputColumn;
            int dot = col.indexOf('.');
            if (dot > 0) {
                String alias = col.substring(0, dot);
                String colName = col.substring(dot + 1);
                Schema ts = aliasSchemas.get(alias);
                if (ts != null) {
                    Field found = ts.getFields().stream()
                            .filter(f -> f.getName().equalsIgnoreCase(colName))
                            .findFirst().orElse(null);
                    if (found != null) {
                        resultFields.add(new Field(expr.outputName,
                                FieldType.nullable(found.getType()), null));
                        continue;
                    }
                }
            }
            for (Schema ts : aliasSchemas.values()) {
                Field found = ts.getFields().stream()
                        .filter(f -> f.getName().equalsIgnoreCase(col))
                        .findFirst().orElse(null);
                if (found != null) {
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(found.getType()), null));
                    break;
                }
            }
            if (resultFields.size() < pq.selectExprs.indexOf(expr) + 1) {
                resultFields.add(new Field(expr.outputName,
                        FieldType.nullable(new ArrowType.Utf8()), null));
            }
        }
        LogUtil.logTiming(t, "schema.buildJoin", "tables=" + pq.joinTables.size());
        return new org.apache.arrow.vector.types.pojo.Schema(resultFields);
    }

    /**
     * Builds Arrow schema for an aggregation query result.
     *
     * @param pq parsed aggregation query
     * @return Arrow schema
     */
    public Schema buildAggregationSchema(ParquetQueryParser pq) {
        long t = LogUtil.mark();
        org.apache.arrow.vector.types.pojo.Schema tableSchema =
                parquetAdapter.getTableSchema(pq.schema, pq.table);
        Map<String, Field> colFieldMap = tableSchema.getFields().stream()
                .collect(Collectors.toMap(Field::getName, f -> f, (a, b) -> a));

        List<Field> resultFields = new ArrayList<>();
        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            switch (expr.func) {
                case COLUMN -> {
                    Field src = resolveColumn(colFieldMap, expr.inputColumn);
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(src.getType()), null));
                }
                case COUNT_STAR, COUNT ->
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(new ArrowType.Int(64, true)), null));
                case SUM -> {
                    ArrowType sumType = expr.decimalScale == null
                            ? new ArrowType.FloatingPoint(
                                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
                            : new ArrowType.Decimal(38, expr.decimalScale, 128);
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(sumType), null));
                }
                case MIN, MAX -> {
                    Field src = resolveColumn(colFieldMap, expr.inputColumn);
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(src.getType()), null));
                }
                default -> { }
            }
        }
        LogUtil.logTiming(t, "schema.buildAgg", "exprs=" + pq.selectExprs.size());
        return new org.apache.arrow.vector.types.pojo.Schema(resultFields);
    }

    /**
     * Resolves a column field by name with case-insensitive fallback.
     *
     * @param colFieldMap  column name to field map
     * @param inputColumn  column name
     * @return resolved Arrow field
     */
    private static Field resolveColumn(Map<String, Field> colFieldMap, String inputColumn) {
        Field src = colFieldMap.get(inputColumn);
        if (src != null) {
            return src;
        }
        for (Map.Entry<String, Field> entry : colFieldMap.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(inputColumn)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException(
                "Column '" + inputColumn + "' not found. Available: " + colFieldMap.keySet());
    }

    // ── vector helpers ────────────────────────────────────────────────────

    /**
     * Creates a single-column VectorSchemaRoot from data.
     *
     * @param data            row data
     * @param allocator       Arrow allocator
     * @param fieldVectorName vector name
     * @param columnName      map key for data extraction
     * @return populated VSR
     */
    private static VectorSchemaRoot getRoot(
            List<? extends Map<String, ?>> data, BufferAllocator allocator,
            String fieldVectorName, String columnName) {
        final VarCharVector dataVector = new VarCharVector(fieldVectorName,
                FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        saveToVectors(ImmutableMap.of(dataVector, columnName), data);
        dataVector.setValueCount(dataVector.getValueCount());
        return new VectorSchemaRoot(singletonList(dataVector));
    }

    /**
     * Writes data to vectors with no row filtering.
     *
     * @param vectorToColumnName vector to column name mapping
     * @param data               row data
     */
    private static <T extends FieldVector> void saveToVectors(
            final Map<T, String> vectorToColumnName,
            List<? extends Map<String, ?>> data) {
        saveToVectors(vectorToColumnName, data, none -> true);
    }

    /**
     * Writes data to vectors with optional row filtering.
     *
     * @param vectorToColumnName vector to column name mapping
     * @param data               row data
     * @param rowPredicate       filter for rows to include
     * @return number of rows written
     */
    @SuppressWarnings({"StringSplitter", "java:S3776"})
    private static <T extends FieldVector> int saveToVectors(
            Map<T, String> vectorToColumnName,
            List<? extends Map<String, ?>> data,
            Predicate<Map<String, ?>> rowPredicate) {
        Objects.requireNonNull(vectorToColumnName);
        Objects.requireNonNull(data);
        final Set<Map.Entry<T, String>> entrySet = vectorToColumnName.entrySet();
        int rows = 0;

        for (Map<String, ?> row : data) {
            if (!rowPredicate.test(row)) {
                continue;
            }
            for (final Map.Entry<T, String> vectorToColumn : entrySet) {
                final T vector = vectorToColumn.getKey();
                final String columnName = vectorToColumn.getValue();
                if (vector instanceof VarCharVector) {
                    saveToVector((String) row.get(columnName), (VarCharVector) vector, rows);
                } else if (vector instanceof IntVector) {
                    saveToVector((Integer) row.get(columnName), (IntVector) vector, rows);
                } else if (vector instanceof UInt1Vector) {
                    saveToVector((Byte) row.get(columnName), (UInt1Vector) vector, rows);
                } else if (vector instanceof org.apache.arrow.vector.BitVector) {
                    saveToVector((Byte) row.get(columnName), (org.apache.arrow.vector.BitVector) vector, rows);
                } else if (vector instanceof ListVector) {
                    String createParamsValues = (String) row.get(columnName);
                    UnionListWriter writer = ((ListVector) vector).getWriter();
                    BufferAllocator vecAllocator = vector.getAllocator();
                    final org.apache.arrow.memory.ArrowBuf buf = vecAllocator.buffer(1024);
                    writer.setPosition(rows);
                    writer.startList();
                    if (createParamsValues != null) {
                        String[] split = createParamsValues.split(",");
                        for (String s : split) {
                            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                            com.google.common.base.Preconditions.checkState(
                                    bytes.length < 1024,
                                    "Amount of bytes is greater than ArrowBuf supports");
                            buf.setBytes(0, bytes);
                            writer.varChar().writeVarChar(0, bytes.length, buf);
                        }
                    }
                    buf.close();
                    writer.endList();
                } else {
                    throw CallStatus.INVALID_ARGUMENT
                            .withDescription("Provided vector not supported")
                            .toRuntimeException();
                }
            }
            rows++;
        }
        for (final Map.Entry<T, String> vectorToColumn : entrySet) {
            vectorToColumn.getKey().setValueCount(rows);
        }
        return rows;
    }

    /**
     * Writes a Byte to a UInt1Vector at the given index.
     *
     * @param data   value
     * @param vector target vector
     * @param index  row index
     */
    private static void saveToVector(Byte data, UInt1Vector vector, int index) {
        vectorConsumer(data, vector,
                fv -> fv.setNull(index),
                (theData, fv) -> fv.setSafe(index, theData));
    }

    /**
     * Writes a Byte to a BitVector at the given index.
     *
     * @param data   value
     * @param vector target vector
     * @param index  row index
     */
    private static void saveToVector(Byte data, org.apache.arrow.vector.BitVector vector, int index) {
        vectorConsumer(data, vector,
                fv -> fv.setNull(index),
                (theData, fv) -> fv.setSafe(index, theData));
    }

    /**
     * Writes a String to a VarCharVector at the given index.
     *
     * @param data   value
     * @param vector target vector
     * @param index  row index
     */
    private static void saveToVector(String data, VarCharVector vector, int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector,
                fv -> fv.setNull(index),
                (theData, fv) -> fv.setSafe(index, new Text(theData)));
    }

    /**
     * Writes an Integer to an IntVector at the given index.
     *
     * @param data   value
     * @param vector target vector
     * @param index  row index
     */
    private static void saveToVector(Integer data, IntVector vector, int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector,
                fv -> fv.setNull(index),
                (theData, fv) -> fv.setSafe(index, theData));
    }

    /**
     * Writes a byte array to a VarBinaryVector at the given index.
     *
     * @param data   value
     * @param vector target vector
     * @param index  row index
     */
    private static void saveToVector(byte[] data, VarBinaryVector vector, int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector,
                fv -> fv.setNull(index),
                (theData, fv) -> fv.setSafe(index, theData));
    }

    /**
     * Validates vector and index preconditions.
     *
     * @param vector field vector
     * @param index  row index
     */
    private static void preconditionCheckSaveToVector(FieldVector vector, int index) {
        Objects.requireNonNull(vector, "vector cannot be null.");
        com.google.common.base.Preconditions.checkState(index >= 0, "Index must be a positive number!");
    }

    /**
     * Applies null-safe vector write using the appropriate consumer.
     *
     * @param data               value
     * @param vector             target vector
     * @param consumerIfNullable null handler
     * @param defaultConsumer    non-null handler
     */
    private static <T, V extends FieldVector> void vectorConsumer(
            T data, V vector,
            Consumer<V> consumerIfNullable,
            BiConsumer<T, V> defaultConsumer) {
        if (Objects.isNull(data)) {
            consumerIfNullable.accept(vector);
            return;
        }
        defaultConsumer.accept(data, vector);
    }
}
