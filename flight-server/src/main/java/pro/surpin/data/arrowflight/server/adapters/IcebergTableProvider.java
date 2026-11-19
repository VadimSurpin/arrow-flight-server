package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import pro.surpin.data.arrowflight.server.TableProvider;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Resolves one Flight SQL schema against an Iceberg Hadoop catalog. */
public final class IcebergTableProvider implements TableProvider, Closeable {

    private final HadoopCatalog catalog;
    private final String sqlSchema;
    private final Namespace namespace;

    /**
     * Binds a catalog namespace to a Flight SQL schema.
     *
     * @param catalog owned Hadoop catalog
     * @param sqlSchema schema name exposed to Flight SQL clients
     * @param namespace Iceberg namespace mapped to the SQL schema
     */
    IcebergTableProvider(HadoopCatalog catalog, String sqlSchema, String namespace) {
        this.catalog = catalog;
        this.sqlSchema = ParquetAdapter.validateName(sqlSchema);
        this.namespace = Namespace.of(ParquetAdapter.validateName(namespace));
    }

    /**
     * Opens the configured Iceberg catalog when a warehouse is enabled.
     *
     * @param configuration server Hadoop configuration
     * @param environment environment holding Iceberg settings
     * @return the catalog binding, or null when disabled
     */
    public static IcebergTableProvider fromEnvironment(
            Configuration configuration, Map<String, String> environment) {
        String warehouse = environment.get("FLIGHT_ICEBERG_WAREHOUSE");
        if (warehouse == null || warehouse.isBlank()) {
            return null;
        }
        URI uri = URI.create(warehouse);
        if (!"hdfs".equalsIgnoreCase(uri.getScheme()) || uri.getPath() == null
                || !uri.getPath().startsWith("/")) {
            throw new IllegalArgumentException("FLIGHT_ICEBERG_WAREHOUSE must be an absolute HDFS URI");
        }
        String schema = ParquetAdapter.validateName(environment.getOrDefault("FLIGHT_ICEBERG_SCHEMA", "iceberg"));
        String namespace = ParquetAdapter.validateName(environment.getOrDefault("FLIGHT_ICEBERG_NAMESPACE", schema));
        return new IcebergTableProvider(new HadoopCatalog(new Configuration(configuration), warehouse),
                schema, namespace);
    }

    /**
     * Returns the schema name exposed to SQL clients.
     *
     * @return configured SQL schema
     */
    public String sqlSchema() {
        return sqlSchema;
    }

    /**
     * Lists table names within the configured Iceberg namespace.
     *
     * @return sorted table names
     */
    public List<String> listTables() {
        try {
            return catalog.listTables(namespace).stream()
                    .map(org.apache.iceberg.catalog.TableIdentifier::name).sorted().toList();
        } catch (NoSuchNamespaceException e) {
            return List.of();
        }
    }

    @Override
    public TableScanPlan planScan(TableIdentifier table) throws IOException {
        if (!sqlSchema.equalsIgnoreCase(table.schema())) {
            throw new IllegalArgumentException("Schema is outside the configured Iceberg namespace");
        }
        var identifier = org.apache.iceberg.catalog.TableIdentifier.of(namespace,
                ParquetAdapter.validateName(Objects.requireNonNull(table.table(), "table name")));
        TableMetadata metadata;
        try {
            HasTableOperations loadedTable = (HasTableOperations) catalog.loadTable(identifier);
            metadata = loadedTable.operations().current();
        } catch (NoSuchTableException e) {
            FileNotFoundException missing = new FileNotFoundException(
                    "Iceberg table not found: " + table.schema() + "." + table.table());
            missing.initCause(e);
            throw missing;
        }
        if (metadata.formatVersion() != 2) {
            throw new UnsupportedOperationException("Iceberg planning supports format version 2");
        }
        Schema schema = new Schema(metadata.schema().columns().stream().map(IcebergTableProvider::field).toList());
        Snapshot snapshot = metadata.currentSnapshot();
        ScanSource source = snapshot == null ? new EmptyScan()
                : new IcebergSnapshot(URI.create(metadata.metadataFileLocation()), snapshot.snapshotId());
        return new TableScanPlan(schema, source);
    }

    /**
     * Converts an Iceberg field into its Arrow representation.
     *
     * @param source Iceberg field definition
     * @return Arrow field
     */
    private static Field field(Types.NestedField source) {
        Type type = source.type();
        List<Field> children = switch (type.typeId()) {
            case STRUCT, LIST -> type.asNestedType().fields().stream().map(IcebergTableProvider::field).toList();
            case MAP -> List.of(new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    type.asMapType().fields().stream().map(IcebergTableProvider::field).toList()));
            default -> List.of();
        };
        return new Field(source.name(), new FieldType(source.isOptional(), arrowType(type), null), children);
    }

    /**
     * Returns the Arrow type for an Iceberg v2 type.
     *
     * @param type Iceberg column type
     * @return equivalent Arrow type
     */
    private static ArrowType arrowType(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            case INTEGER -> new ArrowType.Int(32, true);
            case LONG -> new ArrowType.Int(64, true);
            case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DATE -> new ArrowType.Date(DateUnit.DAY);
            case TIME -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            case TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MICROSECOND,
                    ((Types.TimestampType) type).shouldAdjustToUTC() ? "UTC" : null);
            case STRING -> ArrowType.Utf8.INSTANCE;
            case BINARY -> ArrowType.Binary.INSTANCE;
            case FIXED -> new ArrowType.FixedSizeBinary(((Types.FixedType) type).length());
            case UUID -> new ArrowType.FixedSizeBinary(16);
            case DECIMAL -> {
                Types.DecimalType decimal = (Types.DecimalType) type;
                yield new ArrowType.Decimal(decimal.precision(), decimal.scale(), 128);
            }
            case STRUCT -> ArrowType.Struct.INSTANCE;
            case LIST -> ArrowType.List.INSTANCE;
            case MAP -> new ArrowType.Map(false);
            default -> throw new UnsupportedOperationException("Unsupported Iceberg column type: " + type);
        };
    }

    @Override
    public void close() throws IOException {
        catalog.close();
    }
}
