package net.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.adapter.arrow.ArrowFieldTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import net.surpin.data.arrowflight.server.LogUtil;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import static net.surpin.data.arrowflight.server.adapters.HostUtils.LOOPBACK_HOSTS;

/**
 * Reads Parquet metadata and file listings from HDFS.
 * Maintains caches for schemas, tables, and DDL strings.
 */
public class ParquetAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetAdapter.class);
    private static final String PARQUET_EXTENSION = ".parquet";
    private static final String TABLE_TIMING_PREFIX = "table=";
    private static final long METADATA_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final FileSystem fileSystem;
    private final String dataDirectory;
    private final String localhost;
    private final JavaTypeFactoryImpl typeFactory;

    private final Map<String, Path> tableSchemaCache;
    private final Map<String, Map<String, Path>> tableCache;
    private final Map<String, Map<String, String>> tableDdlCache = new HashMap<>();
    private final Map<TableKey, CachedTableSchema> arrowSchemaCache =
            new ConcurrentHashMap<>();
    private final Map<String, CachedParquetMetadata> parquetMetadataCache =
            new ConcurrentHashMap<>();

    /**
     * Identifies one table in the Arrow schema cache.
     *
     * @param schema schema name
     * @param table table name
     */
    private record TableKey(String schema, String table) {
    }

    /**
     * Stores a full Arrow schema with its file-inventory fingerprint.
     *
     * @param schema full Arrow schema
     * @param fingerprint table file-inventory fingerprint
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CachedTableSchema(
            Schema schema, String fingerprint, long expiresAtNanos) {
    }

    /**
     * Stores Parquet footer metadata with its file fingerprint.
     *
     * @param schema Parquet schema
     * @param rowCount Parquet footer row count
     * @param length file length
     * @param modificationTime file modification time
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CachedParquetMetadata(MessageType schema, long rowCount,
            long length, long modificationTime, long expiresAtNanos) {
    }

    /**
     * Creates a ParquetAdapter for the given Hadoop filesystem and data directory.
     *
     * @param appConfig  server configuration
     * @param fileSystem Hadoop FileSystem instance
     */
    public ParquetAdapter(AppConfig appConfig, FileSystem fileSystem) {
        this(appConfig, fileSystem, "localhost");
    }

    /**
     * Creates a ParquetAdapter with an explicit localhost override.
     *
     * @param appConfig  server configuration
     * @param fileSystem Hadoop FileSystem instance
     * @param localhost  local hostname for block locality resolution
     */
    public ParquetAdapter(AppConfig appConfig, FileSystem fileSystem, String localhost) {
        this.fileSystem = fileSystem;
        this.dataDirectory = appConfig.dataDir();
        this.localhost = localhost;
        this.typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        try {
            this.tableSchemaCache = Collections.unmodifiableMap(scanSchemas());
            this.tableCache = Collections.unmodifiableMap(scanTables());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        initCatalogReader();
    }

    /**
     * Lists all schemas (top-level directories) matching an optional filter pattern.
     *
     * @param filterExpression SQL LIKE pattern, null for all
     * @return map of schema name to path
     * @throws IOException on HDFS read failure
     */
    public Map<String, Path> getSchemas(String filterExpression) throws IOException {
        java.util.function.Predicate<String> schemaPredicate = createLikePredicate(filterExpression);
        return tableSchemaCache.entrySet().stream()
                .filter(entry -> filterExpression == null || schemaPredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Lists all tables in a schema matching an optional filter pattern.
     *
     * @param schema           schema name
     * @param filterExpression SQL LIKE pattern, null for all
     * @return map of table name to path
     */
    public Map<String, Path> getTables(String schema, String filterExpression) {
        Map<String, Path> tables = tableCache.getOrDefault(schema, Collections.emptyMap());
        java.util.function.Predicate<String> tablePredicate = createLikePredicate(filterExpression);
        return tables.entrySet().stream()
                .filter(entry -> filterExpression == null || tablePredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns the Arrow schema for a table.
     *
     * @param schema schema name
     * @param table  table name
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table) {
        return getTableSchema(schema, table, null);
    }

    /**
     * Returns the Arrow schema for a table, optionally filtering to specific columns.
     *
     * @param schema  schema name
     * @param table   table name
     * @param columns columns to include, null or empty for all
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table, List<String> columns) {
        long t = LogUtil.mark();
        validateName(schema);
        validateName(table);
        TableKey key = new TableKey(schema, table);
        CachedTableSchema cached = arrowSchemaCache.compute(key,
                (ignored, current) -> refreshTableSchema(key, current));
        Schema fullSchema = cached.schema();
        if (columns == null || columns.isEmpty()) {
            LogUtil.logTiming(t, "schema.cache",
                    TABLE_TIMING_PREFIX + schema + "." + table
                            + " fields=" + fullSchema.getFields().size());
            return fullSchema;
        }
        List<Field> projected = fullSchema.getFields().stream()
                .filter(field -> columns.contains(field.getName()))
                .toList();
        LogUtil.logTiming(t, "schema.cacheProjection",
                TABLE_TIMING_PREFIX + schema + "." + table
                        + " fields=" + projected.size());
        return new Schema(projected, fullSchema.getCustomMetadata());
    }

    /**
     * Estimates row count from cached Parquet footers for assigned files.
     *
     * @param files assigned relative file paths and sizes
     * @return summed footer row count, or negative one when unavailable
     */
    public long estimateRowCount(Map<String, FileAssignment> files) {
        long result = 0L;
        try {
            for (Map.Entry<String, FileAssignment> file : files.entrySet()) {
                CachedParquetMetadata metadata =
                        parquetMetadata(file.getKey(), file.getValue().size());
                result = Math.addExact(result, metadata.rowCount());
            }
            return result;
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Unable to estimate Parquet row count: {}", e.getMessage());
            return -1L;
        }
    }

    /**
     * Refreshes a table schema after its short metadata TTL.
     *
     * @param key table key
     * @param current current cached value
     * @return refreshed cached schema
     */
    private CachedTableSchema refreshTableSchema(
            TableKey key, CachedTableSchema current) {
        long now = System.nanoTime();
        if (current != null && current.expiresAtNanos() > now) {
            return current;
        }
        try {
            Path tablePath = new Path(
                    dataDirectory, key.schema() + "/" + key.table());
            LOGGER.debug("node={} parquet=schemaReadStart table={}.{} path={}",
                    LogUtil.node(), key.schema(), key.table(), tablePath);
            List<LocatedFileStatus> files = parquetFiles(tablePath);
            String fingerprint = inventoryFingerprint(files);
            if (current != null && current.fingerprint().equals(fingerprint)) {
                return new CachedTableSchema(
                        current.schema(), fingerprint, now + METADATA_TTL_NANOS);
            }
            if (files.isEmpty()) {
                LOGGER.warn("node={} parquet=schemaNotFound table={}.{} path={}",
                        LogUtil.node(), key.schema(), key.table(), tablePath);
                return new CachedTableSchema(
                        new Schema(Collections.emptyList(), null),
                        fingerprint, now + METADATA_TTL_NANOS);
            }
            LocatedFileStatus first = files.get(0);
            String relativePath = relativePath(first.getPath());
            long footerStart = LogUtil.mark();
            CachedParquetMetadata metadata = readParquetMetadata(
                    relativePath, first.getPath(), first.getLen(),
                    first.getModificationTime(), now);
            Schema arrowSchema = SchemaConverter.convert(metadata.schema(), ignored -> true);
            LogUtil.logTiming(footerStart, "schema.readFooter",
                    TABLE_TIMING_PREFIX + key.schema() + "." + key.table()
                            + " fields=" + metadata.schema().getFieldCount());
            return new CachedTableSchema(
                    arrowSchema, fingerprint, now + METADATA_TTL_NANOS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Lists sorted Parquet files below a table directory.
     *
     * @param tablePath table directory
     * @return sorted Parquet file statuses
     * @throws IOException on file-system access failure
     */
    private List<LocatedFileStatus> parquetFiles(Path tablePath) throws IOException {
        List<LocatedFileStatus> result = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(tablePath, true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            if (file.isFile()
                    && file.getPath().getName().toLowerCase()
                    .endsWith(PARQUET_EXTENSION)) {
                result.add(file);
            }
        }
        result.sort(Comparator.comparing(file -> file.getPath().toString()));
        return result;
    }

    /**
     * Builds a stable fingerprint from table file inventory.
     *
     * @param files sorted Parquet file statuses
     * @return inventory fingerprint
     */
    private static String inventoryFingerprint(List<LocatedFileStatus> files) {
        StringBuilder result = new StringBuilder();
        for (LocatedFileStatus file : files) {
            result.append(file.getPath()).append(':')
                    .append(file.getLen()).append(':')
                    .append(file.getModificationTime()).append(';');
        }
        return result.toString();
    }

    /**
     * Resolves cached footer metadata for one relative Parquet path.
     *
     * @param relativePath relative path below the data directory
     * @param expectedLength length published in cluster inventory
     * @return cached footer metadata
     * @throws IOException on file-system access failure
     */
    private CachedParquetMetadata parquetMetadata(
            String relativePath, long expectedLength) throws IOException {
        long now = System.nanoTime();
        CachedParquetMetadata current = parquetMetadataCache.get(relativePath);
        if (current != null && current.expiresAtNanos() > now
                && current.length() == expectedLength) {
            return current;
        }
        Path path = new Path(dataDirectory, relativePath);
        FileStatus status = fileSystem.getFileStatus(path);
        if (status.getLen() != expectedLength) {
            throw new IOException("Parquet inventory size changed for " + relativePath);
        }
        return readParquetMetadata(relativePath, path, status.getLen(),
                status.getModificationTime(), now);
    }

    /**
     * Reads and caches schema and row count from one Parquet footer.
     *
     * @param relativePath cache key below the data directory
     * @param path absolute Hadoop path
     * @param length file length
     * @param modificationTime file modification time
     * @param now current monotonic time
     * @return cached footer metadata
     * @throws IOException on footer read failure
     */
    private CachedParquetMetadata readParquetMetadata(String relativePath,
            Path path, long length, long modificationTime, long now)
            throws IOException {
        CachedParquetMetadata current = parquetMetadataCache.get(relativePath);
        if (current != null && current.length() == length
                && current.modificationTime() == modificationTime) {
            CachedParquetMetadata refreshed = new CachedParquetMetadata(
                    current.schema(), current.rowCount(), length,
                    modificationTime, now + METADATA_TTL_NANOS);
            parquetMetadataCache.put(relativePath, refreshed);
            return refreshed;
        }
        try (ParquetFileReader reader = ParquetFileReader.open(
                new org.apache.parquet.io.InputFile() {
                    @Override
                    public long getLength() {
                        return length;
                    }

                    @Override
                    public org.apache.parquet.io.SeekableInputStream newStream()
                            throws IOException {
                        return org.apache.parquet.hadoop.util.HadoopStreams.wrap(
                                fileSystem.open(path));
                    }
                })) {
            MessageType parquetSchema =
                    reader.getFooter().getFileMetaData().getSchema();
            long rowCount = reader.getFooter().getBlocks().stream()
                    .mapToLong(block -> block.getRowCount()).sum();
            CachedParquetMetadata metadata = new CachedParquetMetadata(
                    parquetSchema, rowCount, length, modificationTime,
                    now + METADATA_TTL_NANOS);
            parquetMetadataCache.put(relativePath, metadata);
            return metadata;
        }
    }

    /**
     * Converts an absolute Hadoop path to a path below the configured data root.
     *
     * @param path absolute file path
     * @return relative path
     * @throws IOException when the data root cannot be resolved
     */
    private String relativePath(Path path) throws IOException {
        URI root = fileSystem.getFileStatus(new Path(dataDirectory)).getPath().toUri();
        return root.relativize(path.toUri()).toString();
    }

    /**
     * Converts an Arrow schema to a DuckDB-compatible DDL statement.
     *
     * @param tableSchema schema name
     * @param tableName   table name
     * @param schema      Arrow schema
     * @return DDL string
     */
    public String arrowSchemaToDDL(String tableSchema, String tableName, Schema schema) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(schema);

        StringBuilder result = new StringBuilder("CREATE TABLE ");
        if (tableSchema != null) {
            result.append(tableSchema).append(".");
        }
        result.append(tableName).append("(\n");

        List<Field> fields = schema.getFields();
        IntStream.range(0, fields.size()).forEach(i -> {
            Field field = fields.get(i);
            RelDataType relDataType;
            if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                relDataType = ArrowFieldTypeFactory.toType(new ArrowType.Int(64, true), typeFactory);
            } else {
                relDataType = ArrowFieldTypeFactory.toType(field.getType(), typeFactory);
            }
            if (i > 0) {
                result.append(",\n");
            }
            result.append("\t\"").append(field.getName()).append("\" ").append(relDataType);
        });
        result.append(")");
        return result.toString();
    }

    /** Initializes cached DDL definitions for all discovered tables. */
    public void initCatalogReader() {
        Objects.requireNonNull(tableSchemaCache, "Initialize schema cache first");
        Objects.requireNonNull(tableCache, "Initialize table cache first");

        StringBuilder ddlBuilder = new StringBuilder();
        List<String> strippedDdls = new ArrayList<>();
        tableCache.forEach((schemaName, tablesMap) -> {
            tableDdlCache.putIfAbsent(schemaName, new HashMap<>());
            tablesMap.forEach((tableName, path) -> {
                Schema schema = getTableSchema(schemaName, tableName);
                String ddl = arrowSchemaToDDL(schemaName, tableName, schema);
                tableDdlCache.get(schemaName).put(tableName, ddl);
                ddlBuilder.append(ddl).append(";\n");
                strippedDdls.add(ddl.replace(schemaName + ".", ""));
            });
        });

        LOGGER.info("Parsed DDL: {}", ddlBuilder.toString());
    }

    /**
     * Returns the DDL cache (schema → table → DDL string).
     *
     * @return DDL map
     */
    public Map<String, Map<String, String>> tableDdlCache() {
        return tableDdlCache;
    }

    /**
     * Resolves file locations for a SQL query across all relevant Parquet files.
     *
     * @param query SQL query
     * @return map of relative file path to FileAssignment with locality info
     * @throws IOException on HDFS read failure
     */
    @SuppressWarnings("java:S3776") // Locality calculation mirrors the Hadoop block hierarchy.
    public Map<String, FileAssignment> locationsForQuery(String query) throws IOException {
        long t = LogUtil.mark();
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        Map<String, FileAssignment> result = new HashMap<>();
        URI dataDirectoryURI = fileSystem.getFileStatus(new Path(dataDirectory)).getPath().toUri();

        int fileCount = 0;
        long totalBytes = 0;

        if (parsedQuery.isJoin) {
            for (ParquetQueryParser.JoinTable jt : parsedQuery.joinTables) {
                validateName(jt.schema());
                validateName(jt.table());
                Path parquetPath = new Path(dataDirectory, jt.schema() + "/" + jt.table());
                LOGGER.debug("node={} parquet=discover table={}.{} path={}",
                        LogUtil.node(), jt.schema(), jt.table(), parquetPath);
                RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(parquetPath, true);
                while (filesIter.hasNext()) {
                    LocatedFileStatus file = filesIter.next();
                    if (file.isDirectory() || !file.getPath().getName().toLowerCase().endsWith(PARQUET_EXTENSION)) {
                        continue;
                    }
                    String relativePath = dataDirectoryURI.relativize(file.getPath().toUri()).toString();
                    result.putIfAbsent(relativePath, new FileAssignment(file.getLen(), fileLocality(file).keySet()));
                    fileCount++;
                    totalBytes += file.getLen();
                }
                LogUtil.logTiming(t, "schema.discoverJoinTable", TABLE_TIMING_PREFIX + jt.schema() + "." + jt.table() + " files=" + fileCount + " bytes=" + totalBytes);
            }
            return result;
        }

        validateName(parsedQuery.schema);
        validateName(parsedQuery.table);
        Path parquetPath = new Path(dataDirectory, parsedQuery.schema + "/" + parsedQuery.table);
        LOGGER.debug("node={} parquet=discover table={}.{} path={}",
                LogUtil.node(), parsedQuery.schema, parsedQuery.table, parquetPath);
        RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(parquetPath, true);
        while (filesIter.hasNext()) {
            LocatedFileStatus file = filesIter.next();
            if (file.isDirectory() || !file.getPath().getName().toLowerCase().endsWith(PARQUET_EXTENSION)) {
                continue;
            }
            String relativePath = dataDirectoryURI.relativize(file.getPath().toUri()).toString();
            result.put(relativePath, new FileAssignment(file.getLen(), fileLocality(file).keySet()));
            fileCount++;
            totalBytes += file.getLen();
        }
        LogUtil.logTiming(t, "schema.discover", TABLE_TIMING_PREFIX + (parsedQuery.schema != null ? parsedQuery.schema + "." : "") + (parsedQuery.table != null ? parsedQuery.table : "") + " files=" + fileCount + " bytes=" + totalBytes);
        return result;
    }

    /**
     * Lists Parquet files with at least one block on this server's colocated storage host.
     * Paths are relative to the shared data root. For local filesystems, loopback block
     * locations are mapped to the configured storage host.
     *
     * @return relative Parquet path to file size
     * @throws IOException on file-system access failure
     */
    public Map<String, Long> localFileInventory() throws IOException {
        long t = LogUtil.mark();
        Map<String, Long> result = new LinkedHashMap<>();
        Path root = new Path(this.dataDirectory);
        if (!this.fileSystem.exists(root)) {
            LOGGER.warn("node={} parquet=inventoryDataDirNotFound path={}",
                    LogUtil.node(), this.dataDirectory);
            return result;
        }

        URI rootUri = this.fileSystem.getFileStatus(root).getPath().toUri();
        RemoteIterator<LocatedFileStatus> files = this.fileSystem.listFiles(root, true);
        int scanned = 0;
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            scanned++;
            if (file.isFile()
                    && file.getPath().getName().toLowerCase().endsWith(PARQUET_EXTENSION)
                    && hasLocalBlock(fileLocality(file).keySet(), localhost)) {
                String relativePath = rootUri.relativize(file.getPath().toUri()).toString();
                result.put(relativePath, file.getLen());
            }
        }
        long totalBytes = result.values().stream().mapToLong(Long::longValue).sum();
        LogUtil.logTiming(t, "parquet.localInventory", "localFiles=" + result.size() + " scanned=" + scanned + " totalBytes=" + totalBytes);
        return result;
    }

    static boolean hasLocalBlock(java.util.Set<String> blockHosts, String localHost) {
        String normalizedLocalHost = HostUtils.normalize(localHost);
        return blockHosts.stream()
                .map(HostUtils::normalize)
                .anyMatch(normalizedLocalHost::equals);
    }

    /**
     * Returns block locality for a file, mapping host to block count.
     *
     * @param locatedFileStatus file status
     * @return ordered map of host to block count
     */
    public LinkedHashMap<String, Long> fileLocality(LocatedFileStatus locatedFileStatus) {
        Objects.requireNonNull(locatedFileStatus);
        return Arrays.stream(locatedFileStatus.getBlockLocations())
                .flatMap(bl -> {
                    try {
                        return Stream.of(bl.getHosts())
                                .map(host -> LOOPBACK_HOSTS.contains(host) ? localhost : host);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.groupingBy(h -> h, Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * Returns the data directory path.
     *
     * @return data directory path string
     */
    public String dataDirectory() {
        return dataDirectory;
    }

    /**
     * Validates that a schema or table name contains only safe characters.
     * Prevents path traversal via names like ".." or "./etc".
     */
    public static String validateName(String name) {
        if (name == null) {
            return null;
        }
        if (!name.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Invalid name: '" + name
                    + "'. Must match [a-zA-Z_][a-zA-Z0-9_]*");
        }
        return name;
    }

    /**
     * Returns the Hadoop filesystem instance.
     *
     * @return FileSystem
     */
    public FileSystem fileSystem() {
        return fileSystem;
    }

    /**
     * Scans data directory for schema directories and caches them.
     *
     * @throws IOException on HDFS read failure
     */
    private Map<String, Path> scanSchemas() throws IOException {
        LOGGER.info("Initializing schema cache for data directory: {}", dataDirectory);
        Path dirPath = new Path(dataDirectory);

        if (!fileSystem.exists(dirPath)) {
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return Collections.emptyMap();
        }

        Map<String, Path> result = Arrays.stream(fileSystem.listStatus(dirPath))
                .filter(FileStatus::isDirectory)
                .collect(Collectors.toMap(status -> status.getPath().getName(), FileStatus::getPath));

        LOGGER.info("Collected schemas: {}", result);
        return result;
    }

    /**
     * Scans each schema directory for table directories and caches them.
     *
     * @throws IOException on HDFS read failure
     */
    private Map<String, Map<String, Path>> scanTables() throws IOException {
        LOGGER.info("Initializing table cache for data directory: {}", dataDirectory);
        Path schemaPath = new Path(dataDirectory);

        if (!fileSystem.exists(schemaPath)) {
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return Collections.emptyMap();
        }

        Map<String, Map<String, Path>> result = new HashMap<>();
        for (Map.Entry<String, Path> schema : tableSchemaCache.entrySet()) {
            LOGGER.info("Collecting tables for schema: {} at path {}", schema.getKey(), schema.getValue());
            Map<String, Path> tables = Arrays.stream(fileSystem.listStatus(schema.getValue()))
                    .filter(FileStatus::isDirectory)
                    .collect(Collectors.toMap(status -> status.getPath().getName(), FileStatus::getPath));
            result.put(schema.getKey(), tables);
        }

        LOGGER.info("Collected tables: {}", result);
        return result;
    }

    /**
     * Creates a predicate from a SQL LIKE pattern.
     *
     * @param pattern SQL LIKE pattern
     * @return predicate
     */
    private static java.util.function.Predicate<String> createLikePredicate(String pattern) {
        if (pattern == null || pattern.isEmpty() || pattern.equals("%")) {
            return s -> true;
        }
        String regex = pattern
                .replace("\\", "\\\\")
                .replace(".", "\\.")
                .replace("%", ".*")
                .replace("_", ".");
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex,
                java.util.regex.Pattern.CASE_INSENSITIVE);
        return s -> p.matcher(s).matches();
    }
}
