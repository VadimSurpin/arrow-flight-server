package pro.surpin.data.arrowflight.server.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;

/**
 * Table statistics for a query optimizer: row count, per-column bounds, null counts and sizes come from the Parquet
 * footers (a few kilobytes per file, cached per table version); the number of distinct values is not in a footer and
 * is estimated by a background {@code approx_count_distinct} scan, so a first answer never waits for it and later
 * answers carry it.
 *
 * <p>Bounds are numbers: integers and floats as they are, decimals as their value, dates as the epoch day and
 * timestamps as epoch seconds. Strings have none.
 */
public final class TableStatisticsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStatisticsService.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern COLUMN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final long FOOTER_TTL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
    private static final long AWAIT_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(60);

    /** Runs a single-value SQL query on the engine that scans the data. */
    @FunctionalInterface
    public interface DistinctCounter {
        /**
         * Runs a query returning one integer.
         *
         * @param sql query text
         * @return the integer
         * @throws Exception when the query fails
         */
        long count(String sql) throws Exception;
    }

    /**
     * Statistics of one column.
     *
     * @param min smallest value, or null when unknown (strings, missing footer statistics)
     * @param max largest value, or null when unknown
     * @param nulls number of nulls, or -1 when unknown
     * @param bytes uncompressed size of the column
     * @param ndv estimated number of distinct values, or null while the estimate is pending
     */
    public record ColumnStatistics(Double min, Double max, long nulls, long bytes, Long ndv) { }

    /**
     * Statistics of one table.
     *
     * @param rows row count
     * @param columns statistics of the requested columns that exist
     */
    public record TableStatistics(long rows, Map<String, ColumnStatistics> columns) { }

    private record Footer(String fingerprint, long expiresAtNanos, long rows, List<String> files,
            Map<String, ColumnAggregate> columns) { }

    private static final class ColumnAggregate {
        private Double min;
        private Double max;
        private boolean boundsKnown = true;
        private long nulls;
        private boolean nullsKnown = true;
        private long bytes;
    }

    private final FileSystem fileSystem;
    private final Path dataDirectory;
    private final DistinctCounter counter;
    private final ExecutorService background;
    private final Map<String, Footer> footers = new ConcurrentHashMap<>();
    private final Map<String, Long> distinct = new ConcurrentHashMap<>();
    private final Set<String> pending = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong footerScans = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Creates the service.
     *
     * @param fileSystem file system holding the data directory
     * @param dataDirectory root with {@code schema/table/*.parquet}
     * @param counter engine for the distinct-count estimate
     * @param background executor that runs the estimates, one at a time is enough
     */
    public TableStatisticsService(FileSystem fileSystem, Path dataDirectory, DistinctCounter counter,
            ExecutorService background) {
        this.fileSystem = fileSystem;
        this.dataDirectory = dataDirectory;
        this.counter = counter;
        this.background = background;
    }

    /**
     * Statistics of a table.
     *
     * @param schema schema name
     * @param table table name
     * @param columns columns to report; unknown names are skipped
     * @return statistics
     * @throws IOException when the table cannot be read
     */
    public TableStatistics statistics(String schema, String table, List<String> columns) throws IOException {
        ParquetAdapter.validateName(schema);
        ParquetAdapter.validateName(table);
        for (String column : columns) {
            if (!COLUMN.matcher(column).matches()) {
                throw new IllegalArgumentException("Unsupported column name: " + column);
            }
        }
        String key = schema + "." + table;
        Footer footer = footer(key, schema, table);
        Map<String, ColumnStatistics> result = new LinkedHashMap<>();
        for (String column : columns) {
            ColumnAggregate aggregate = footer.columns().get(column);
            if (aggregate == null) {
                continue;
            }
            String distinctKey = key + "|" + footer.fingerprint() + "|" + column;
            Long ndv = distinct.get(distinctKey);
            if (ndv == null) {
                estimateDistinct(distinctKey, footer, column);
            }
            result.put(column, new ColumnStatistics(
                    aggregate.boundsKnown ? aggregate.min : null, aggregate.boundsKnown ? aggregate.max : null,
                    aggregate.nullsKnown ? aggregate.nulls : -1, aggregate.bytes, ndv));
        }
        return new TableStatistics(footer.rows(), result);
    }

    /**
     * Number of times the footers of a table were read and aggregated.
     *
     * @return scan count
     */
    public long footerScans() {
        return footerScans.get();
    }

    /**
     * Waits until no distinct-count estimate is running; for tests and warm-up.
     *
     * @throws InterruptedException when interrupted
     */
    public void awaitIdle() throws InterruptedException {
        long deadline = System.nanoTime() + AWAIT_NANOS;
        while (!pending.isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Distinct-count estimates did not finish: " + pending);
            }
            Thread.sleep(20);
        }
    }

    /**
     * Encodes statistics for the wire.
     *
     * @param statistics statistics
     * @return JSON text
     */
    public static String toJson(TableStatistics statistics) {
        try {
            return JSON.writeValueAsString(statistics);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Decodes statistics from the wire.
     *
     * @param json JSON text
     * @return statistics
     */
    public static TableStatistics fromJson(String json) {
        try {
            return JSON.readValue(json, TableStatistics.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed table statistics", e);
        }
    }

    private void estimateDistinct(String distinctKey, Footer footer, String column) {
        if (!pending.add(distinctKey)) {
            return;
        }
        List<String> paths = footer.files();
        try {
            background.execute(() -> {
                try {
                    String list = String.join(", ", paths.stream().map(p -> "'" + p.replace("'", "''") + "'").toList());
                    long value = counter.count("SELECT approx_count_distinct(\"" + column + "\") FROM read_parquet(["
                            + list + "])");
                    distinct.put(distinctKey, value);
                } catch (Exception | LinkageError e) {
                    LOGGER.warn("Distinct-count estimate of {} failed: {}", distinctKey, e.toString());
                } finally {
                    pending.remove(distinctKey);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            pending.remove(distinctKey);
        }
    }

    private Footer footer(String key, String schema, String table) throws IOException {
        long now = System.nanoTime();
        Footer cached = footers.get(key);
        if (cached != null && cached.expiresAtNanos() > now) {
            return cached;
        }
        Path tablePath = new Path(new Path(dataDirectory, schema), table);
        if (!fileSystem.exists(tablePath)) {
            throw new FileNotFoundException("Unknown table " + key);
        }
        List<LocatedFileStatus> files = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(tablePath, true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            if (file.isFile() && file.getPath().getName().toLowerCase().endsWith(".parquet")) {
                files.add(file);
            }
        }
        files.sort(Comparator.comparing(file -> file.getPath().toString()));
        if (files.isEmpty()) {
            throw new FileNotFoundException("No Parquet files in " + key);
        }
        StringBuilder fingerprint = new StringBuilder();
        for (LocatedFileStatus file : files) {
            fingerprint.append(file.getPath()).append(':').append(file.getLen()).append(':')
                    .append(file.getModificationTime()).append(';');
        }
        if (cached != null && cached.fingerprint().equals(fingerprint.toString())) {
            Footer renewed = new Footer(cached.fingerprint(), now + FOOTER_TTL_NANOS, cached.rows(), cached.files(),
                    cached.columns());
            footers.put(key, renewed);
            return renewed;
        }
        long t = LogUtil.mark();
        Footer fresh = readFooters(files, fingerprint.toString(), now);
        footerScans.incrementAndGet();
        LogUtil.logTiming(t, "statistics.footers", "table=" + key + " files=" + files.size());
        footers.put(key, fresh);
        return fresh;
    }

    private Footer readFooters(List<LocatedFileStatus> files, String fingerprint, long now) throws IOException {
        Map<String, ColumnAggregate> columns = new LinkedHashMap<>();
        long rows = 0;
        for (LocatedFileStatus file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(file.getPath(), fileSystem.getConf()))) {
                for (BlockMetaData block : reader.getFooter().getBlocks()) {
                    rows += block.getRowCount();
                    for (ColumnChunkMetaData chunk : block.getColumns()) {
                        if (chunk.getPath().size() != 1) {
                            continue;
                        }
                        merge(columns.computeIfAbsent(chunk.getPath().toDotString(), n -> new ColumnAggregate()),
                                chunk);
                    }
                }
            }
        }
        List<String> paths = files.stream().map(f -> ExecutionService.plainDuckDbPath(f.getPath())).toList();
        return new Footer(fingerprint, now + FOOTER_TTL_NANOS, rows, paths, columns);
    }

    private static void merge(ColumnAggregate aggregate, ColumnChunkMetaData chunk) {
        aggregate.bytes += chunk.getTotalUncompressedSize();
        Statistics<?> statistics = chunk.getStatistics();
        if (statistics == null || statistics.isEmpty() || !statistics.isNumNullsSet()) {
            aggregate.nullsKnown = false;
        } else {
            aggregate.nulls += statistics.getNumNulls();
        }
        if (!aggregate.boundsKnown) {
            return;
        }
        if (statistics == null || statistics.isEmpty()) {
            aggregate.boundsKnown = false;
            return;
        }
        if (!statistics.hasNonNullValue()) {
            return;
        }
        Double low = number(statistics.genericGetMin(), chunk.getPrimitiveType());
        Double high = number(statistics.genericGetMax(), chunk.getPrimitiveType());
        if (low == null || high == null) {
            aggregate.boundsKnown = false;
            aggregate.min = null;
            aggregate.max = null;
            return;
        }
        aggregate.min = aggregate.min == null ? low : Math.min(aggregate.min, low);
        aggregate.max = aggregate.max == null ? high : Math.max(aggregate.max, high);
    }

    private static Double number(Comparable<?> value, PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (value instanceof Integer || value instanceof Long) {
            long raw = ((Number) value).longValue();
            if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                return BigDecimal.valueOf(raw, decimal.getScale()).doubleValue();
            }
            if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
                switch (timestamp.getUnit()) {
                    case MILLIS:
                        return raw / 1_000.0;
                    case MICROS:
                        return raw / 1_000_000.0;
                    default:
                        return raw / 1_000_000_000.0;
                }
            }
            return (double) raw;
        }
        if (value instanceof Float || value instanceof Double) {
            double number = ((Number) value).doubleValue();
            return Double.isNaN(number) || Double.isInfinite(number) ? null : number;
        }
        if (value instanceof Binary binary && logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return new BigDecimal(new BigInteger(binary.getBytes()), decimal.getScale()).doubleValue();
        }
        return null;
    }
}
