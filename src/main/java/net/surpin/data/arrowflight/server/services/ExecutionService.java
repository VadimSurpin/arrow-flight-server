package net.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.ExecutionPath;
import net.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import net.surpin.data.arrowflight.server.LogUtil;

import java.math.BigDecimal;

/**
 * Orchestrates query execution through DuckDB.
 */
public final class ExecutionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionService.class);
    private static final String FILES_TIMING_PREFIX = "files=";

    private final ParquetAdapter parquetAdapter;
    private final DuckDbAdapter duckDbAdapter;
    private final MetadataService metadataService;
    private final ExecutorService ioPool;
    private final AppConfig appConfig;

    /**
     * Creates ExecutionService.
     *
     * @param parquetAdapter  Parquet metadata adapter
     * @param duckDbAdapter   DuckDB adapter
     * @param metadataService schema/metadata service
     * @param appConfig       server configuration
     * @param ioPool          shared I/O thread pool
     */
    public ExecutionService(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool) {
        this.parquetAdapter = parquetAdapter;
        this.duckDbAdapter = duckDbAdapter;
        this.metadataService = metadataService;
        this.appConfig = appConfig;
        this.ioPool = ioPool;
    }

    /**
     * Reads specific Parquet files and sends Arrow batches through the listener.
     *
     * @param allocator      Arrow buffer allocator
     * @param query          SQL query
     * @param fileUris       relative file paths
     * @param listener       Flight stream listener
     * @param startListener  whether to call listener.start()
     * @throws Exception on execution failure
     */
    public void readParquet(BufferAllocator allocator, String query, String[] fileUris,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        readParquet(allocator, query, fileUris, listener, startListener,
                new ExecutionPathTracker());
    }

    /**
     * Reads Parquet files while recording the actual runtime-selected execution path.
     *
     * @param allocator      Arrow buffer allocator
     * @param query          SQL query
     * @param fileUris       relative file paths
     * @param listener       Flight stream listener
     * @param startListener  whether to call listener.start()
     * @param pathTracker    runtime execution-path tracker
     * @throws Exception on execution failure
     */
    public void readParquet(BufferAllocator allocator, String query, String[] fileUris,
            FlightProducer.ServerStreamListener listener, boolean startListener,
            ExecutionPathTracker pathTracker) throws Exception {
        long tParse = LogUtil.mark();
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        LogUtil.logTiming(tParse, "parseQuery");

        if (parsedQuery.isJoin) {
            pathTracker.select(ExecutionPath.DUCKDB_JOIN);
            executeJoin(allocator, parsedQuery, fileUris, listener, startListener);
            return;
        }

        if (fileUris == null) {
            long tDiscover = LogUtil.mark();
            fileUris = parquetAdapter.locationsForQuery(query)
                    .keySet().toArray(new String[0]);
            LogUtil.logTiming(tDiscover, "files.discover", FILES_TIMING_PREFIX + fileUris.length);
        }

        List<Path> parquetFiles = resolveParquetFiles(parsedQuery, fileUris);
        if (parquetFiles.isEmpty()) {
            pathTracker.unknown("no-parquet-files");
            LOGGER.warn("No Parquet files to read for query: {}", query);
            return;
        }

        long tResolve = LogUtil.mark();
        List<String> resolvedUris = resolveUris(fileUris);
        LogUtil.logTiming(tResolve, "files.resolveUris", FILES_TIMING_PREFIX + resolvedUris.size());

        if (parsedQuery.hasAggregation) {
            LOGGER.debug("qid={} node={} execution=engine engine=DuckDB(aggregation) hasGroupBy={} files={}",
                    LogUtil.qid(), LogUtil.node(), !parsedQuery.groupByColumnNames.isEmpty(),
                    parquetFiles.size());
            executeAggregation(allocator, parsedQuery, parquetFiles, resolvedUris,
                    fileUris, listener, startListener, pathTracker);
            return;
        }

        pathTracker.select(ExecutionPath.DUCKDB_SCAN);
        LOGGER.debug("qid={} node={} execution=engine engine=DuckDB files={}",
                LogUtil.qid(), LogUtil.node(), resolvedUris.size());
        String duckSql = DuckDbAdapter.buildSelectSql(parsedQuery,
                DuckDbAdapter.readParquetFromClause(ducksDbPaths(resolvedUris)));
        duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
    }

    /**
     * Executes aggregation queries via DuckDB or footer-stats fast paths.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param parquetFiles  resolved Parquet file paths
     * @param resolvedUris  resolved file URIs
     * @param fileUris      relative file paths
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @param pathTracker   runtime execution-path tracker
     * @throws Exception on execution failure
     */
    @SuppressWarnings("java:S3776") // Aggregation dispatch intentionally keeps all aggregate states together.
    private void executeAggregation(BufferAllocator allocator, ParquetQueryParser pq,
            List<Path> parquetFiles, List<String> resolvedUris, String[] fileUris,
            FlightProducer.ServerStreamListener listener,
            boolean startListener, ExecutionPathTracker pathTracker) throws Exception {
        long t = LogUtil.mark();

        if (parquetFiles.isEmpty()) {
            pathTracker.unknown("no-parquet-files");
            emitRowsAsArrow(allocator, pq, Collections.emptyList(), listener, startListener);
            return;
        }

        boolean noGroupByNoFilter = pq.groupByColumnNames.isEmpty()
                && (pq.filter == null || pq.filter.isBlank())
                && !pq.selectExprs.isEmpty();

        // Fast path A: COUNT(*) only — read row counts from Parquet footer
        if (noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(
                        e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)) {
            pathTracker.select(ExecutionPath.FOOTER_COUNT);
            List<Future<Long>> futs = new ArrayList<>(parquetFiles.size());
            for (Path file : parquetFiles) {
                futs.add(ioPool.submit(() -> DuckDbAdapter.footerRowCount(
                        parquetAdapter.fileSystem(), file)));
            }
            long total = 0;
            for (Future<Long> f : futs) {
                total += f.get();
            }
            LogUtil.logTiming(t, "engine:agg.footerCount", FILES_TIMING_PREFIX + parquetFiles.size() + " total=" + total);
            int n = pq.selectExprs.size();
            Object[] row = new Object[n];
            Arrays.fill(row, total);
            emitRowsAsArrow(allocator, pq, Collections.singletonList(row), listener, startListener);
            return;
        }

        // Fast path B: MIN/MAX/COUNT(col) from Parquet footer statistics
        boolean statsEligible = noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)
                && pq.selectExprs.stream().anyMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT);
        if (statsEligible) {
            pathTracker.select(ExecutionPath.FOOTER_STATS);
            List<Future<Optional<Object[]>>> futs = new ArrayList<>(parquetFiles.size());
            for (Path file : parquetFiles) {
                futs.add(ioPool.submit(() -> DuckDbAdapter.footerStats(
                        parquetAdapter.fileSystem(), file, pq)));
            }
            Object[] merged = null;
            boolean allHaveStats = true;
            for (Future<Optional<Object[]>> f : futs) {
                Optional<Object[]> opt = f.get();
                if (opt.isEmpty()) {
                    allHaveStats = false;
                    break;
                }
                if (merged == null) {
                    merged = opt.get().clone();
                } else {
                    mergeAggCols(pq.selectExprs, merged, opt.get(), 0);
                }
            }
            if (allHaveStats) {
                LogUtil.logTiming(t, "engine:agg.footerStats", FILES_TIMING_PREFIX + parquetFiles.size());
                List<Object[]> rows = merged != null
                        ? Collections.singletonList(merged) : Collections.emptyList();
                emitRowsAsArrow(allocator, pq, rows, listener, startListener);
                return;
            }
            LogUtil.logTiming(t, "engine:agg.footerStatsFallback", FILES_TIMING_PREFIX + parquetFiles.size());
            pathTracker.fallbackTo(
                    ExecutionPath.DUCKDB_AGGREGATION, "missing-parquet-footer-statistics");
        } else {
            pathTracker.select(ExecutionPath.DUCKDB_AGGREGATION);
        }

        String duckSql = DuckDbAdapter.buildDuckSqlWithFilter(pq,
                DuckDbAdapter.readParquetFromClause(ducksDbPaths(resolvedUris)), false);
        duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
    }

    // ── DuckDB join execution ────────────────────────────────────────────

    /**
     * Executes JOIN queries by registering temp views in DuckDB and streaming the result.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query with join tables
     * @param fileUris      relative file paths
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    private void executeJoin(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        long t = LogUtil.mark();
        Connection conn = duckDbAdapter.connection();
        List<String> registeredAliases = new ArrayList<>();
        try {
            Map<String, List<String>> tableFiles = new LinkedHashMap<>();
            for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
                String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                tableFiles.computeIfAbsent(key, k -> {
                    try {
                        return resolveTableFiles(k);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
                String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                List<String> duckDbPaths = tableFiles.get(key);
                if (duckDbPaths.isEmpty()) {
                    throw new IOException("No Parquet files found for table: " + key);
                }
                long tView = LogUtil.mark();
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE OR REPLACE TEMP VIEW "
                            + DuckDbAdapter.quoteIdentifier(jt.alias())
                            + " AS SELECT * FROM "
                            + DuckDbAdapter.readParquetFromClause(duckDbPaths));
                }
                LogUtil.logTiming(tView, "engine:join.createView", "alias=" + jt.alias() + " " + FILES_TIMING_PREFIX + duckDbPaths.size());
                registeredAliases.add(jt.alias());
            }

            duckDbAdapter.streamSql(allocator, pq.duckDbSql, listener, startListener);
        } finally {
            long tDrop = LogUtil.mark();
            try (Statement stmt = conn.createStatement()) {
                for (String alias : registeredAliases) {
                    try {
                        stmt.execute("DROP VIEW IF EXISTS "
                                + DuckDbAdapter.quoteIdentifier(alias));
                    } catch (Exception ignored) {
                    }
                }
            }
            LogUtil.logTiming(tDrop, "engine:join.dropViews", "aliases=" + registeredAliases.size());
        }
        LogUtil.logTiming(t, "engine:join", "tables=" + pq.joinTables.size());
    }

    // ── private helpers ──────────────────────────────────────────────────

    /**
     * Resolves relative file URIs to absolute Parquet paths.
     *
     * @param pq       parsed query
     * @param fileUris relative file paths
     * @return resolved Parquet paths
     * @throws IOException on HDFS read failure
     */
    private List<Path> resolveParquetFiles(ParquetQueryParser pq, String[] fileUris)
            throws IOException {
        long t = LogUtil.mark();
        List<Path> files = new ArrayList<>();
        for (String uri : fileUris) {
            files.add(new Path(parquetAdapter.dataDirectory(), uri));
        }
        LogUtil.logTiming(t, "files.resolvePaths", FILES_TIMING_PREFIX + files.size());
        return files;
    }

    /**
     * Resolves relative file URIs to absolute URIs for DuckDB consumption.
     *
     * @param fileUris relative file paths
     * @return resolved URIs
     * @throws IOException on HDFS read failure
     */
    private List<String> resolveUris(String[] fileUris) throws IOException {
        long t = LogUtil.mark();
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            Path path = new Path(parquetAdapter.dataDirectory(), rel);
            uris.add(parquetAdapter.fileSystem().makeQualified(path).toUri().toString());
        }
        LogUtil.logTiming(t, "files.resolveUris", FILES_TIMING_PREFIX + uris.size());
        return uris;
    }

    /**
     * Strips file: scheme from URIs for DuckDB consumption.
     *
     * @param uris fully-qualified URIs
     * @return DuckDB-compatible paths
     */
    static List<String> ducksDbPaths(List<String> uris) {
        return uris.stream().map(u -> {
            if (u.startsWith("file:")) {
                int colon = u.indexOf(':');
                // file:///path → /path,  file:/path → /path
                String stripped = u.substring(colon + 1);
                while (stripped.startsWith("/")) {
                    stripped = stripped.substring(1);
                }
                return "/" + stripped;
            }
            return u;
        }).toList();
    }

    /**
     * Recursively lists Parquet files for a schema.table key.
     *
     * @param key schema.table or table name
     * @return DuckDB-compatible file paths
     * @throws IOException on HDFS read failure
     */
    private List<String> resolveTableFiles(String key) throws IOException {
        long t = LogUtil.mark();
        int dot = key.indexOf('.');
        String schema = dot > 0 ? key.substring(0, dot) : null;
        String table = dot > 0 ? key.substring(dot + 1) : key;
        Path dir = schema != null
                ? new Path(parquetAdapter.dataDirectory(), schema + "/" + table)
                : new Path(parquetAdapter.dataDirectory(), table);
        List<String> uris = new ArrayList<>();
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it =
                parquetAdapter.fileSystem().listFiles(dir, true);
        while (it.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus f = it.next();
            if (f.isFile() && f.getPath().getName().endsWith(".parquet")) {
                uris.add(plainDuckDbPath(f.getPath()));
            }
        }
        LogUtil.logTiming(t, "engine:join.resolveTableFiles", "table=" + key + " " + FILES_TIMING_PREFIX + uris.size());
        return uris;
    }

    /**
     * Converts Hadoop path to DuckDB-quoted path string.
     *
     * @param p Hadoop path
     * @return quoted DuckDB path
     */
    static String duckDbPath(org.apache.hadoop.fs.Path p) {
        java.net.URI uri = p.toUri();
        String path = "file".equals(uri.getScheme())
                ? uri.getPath()
                : uri.toString();
        return "'" + path.replace("'", "''") + "'";
    }

    /**
     * Converts Hadoop path to unquoted DuckDB path string.
     *
     * @param p Hadoop path
     * @return unquoted DuckDB path
     */
    static String plainDuckDbPath(org.apache.hadoop.fs.Path p) {
        java.net.URI uri = p.toUri();
        return "file".equals(uri.getScheme())
                ? uri.getPath()
                : uri.toString();
    }

    // ── emit ─────────────────────────────────────────────────────────────

    /**
     * Emits aggregation result rows as Arrow batches through the listener.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query
     * @param rows          result rows
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws InterruptedException if interrupted during send
     */
    @SuppressWarnings("java:S3776") // Vector population must branch on each supported Arrow vector type.
    private void emitRowsAsArrow(BufferAllocator allocator, ParquetQueryParser pq,
            List<Object[]> rows, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws InterruptedException {
        long t = LogUtil.mark();
        Schema aggSchema = pq.selectExprs.isEmpty()
                ? metadataService.getQuerySchema(buildSelectExprQuery(pq))
                : metadataService.buildAggregationSchema(pq);
        int numGbCols = pq.groupByColumnNames.size();

        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : aggSchema.getFields()) {
            FieldVector v = field.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(rows.size());
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(rows.size() * 16);
            }
            vectors.add(v);
        }

        try (VectorSchemaRoot root = new VectorSchemaRoot(aggSchema.getFields(), vectors)) {
            root.setRowCount(rows.size());
            int vecIdx = 0;
            int aggIdx = 0;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                FieldVector vec = vectors.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    for (int r = 0; r < rows.size(); r++) {
                        setVectorValue(vec, r, rows.get(r)[gbPos]);
                    }
                } else {
                    int pos = numGbCols + aggIdx++;
                    for (int r = 0; r < rows.size(); r++) {
                        setVectorValue(vec, r, rows.get(r)[pos]);
                    }
                }
            }
            if (startListener) {
                listener.start(root);
            }
            if (!rows.isEmpty()) {
                if (DuckDbAdapter.awaitListenerReady(
                        listener, appConfig.flightListenerReadyTimeoutMillis())) {
                    listener.putNext();
                }
            }
        }
        LogUtil.logTiming(t, "engine:agg.emitRows", "rows=" + rows.size());
    }

    /**
     * Builds a SELECT * query for schema inference.
     *
     * @param pq parsed query
     * @return SELECT * query string
     */
    private String buildSelectExprQuery(ParquetQueryParser pq) {
        if (pq.schema != null && pq.table != null) {
            return "SELECT * FROM " + pq.schema + "." + pq.table;
        }
        throw new IllegalArgumentException("Cannot build schema query: missing schema/table");
    }



    /**
     * Merges aggregation columns from one row into another.
     *
     * @param exprs     select expressions
     * @param into      target accumulators
     * @param from      source row
     * @param numGbCols number of group-by columns
     */
    static void mergeAggCols(List<ParquetQueryParser.SelectExpr> exprs,
            Object[] into, Object[] from, int numGbCols) {
        int aggIdx = 0;
        for (ParquetQueryParser.SelectExpr expr : exprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                continue;
            }
            int pos = numGbCols + aggIdx++;
            switch (expr.func) {
                case COUNT_STAR, COUNT -> into[pos] = addLongs(into[pos], from[pos]);
                case SUM -> into[pos] = addNumbers(into[pos], from[pos]);
                case MIN -> into[pos] = minOf(into[pos], from[pos]);
                case MAX -> into[pos] = maxOf(into[pos], from[pos]);
                default -> { }
            }
        }
    }

    /**
     * Extracts a long value from a FieldVector at the given index.
     *
     * @param vec   field vector
     * @param index row index
     * @return long value
     */
    static long toLong(FieldVector vec, int index) {
        if (vec instanceof BigIntVector v) {
            return v.get(index);
        }
        if (vec instanceof IntVector v) {
            return v.get(index);
        }
        if (vec instanceof SmallIntVector v) {
            return v.get(index);
        }
        if (vec instanceof TinyIntVector v) {
            return v.get(index);
        }
        return ((Number) vec.getObject(index)).longValue();
    }

    /**
     * Extracts a double value from a FieldVector at the given index.
     *
     * @param vec   field vector
     * @param index row index
     * @return double value
     */
    static double toDouble(FieldVector vec, int index) {
        if (vec instanceof Float8Vector v) {
            return v.get(index);
        }
        if (vec instanceof Float4Vector v) {
            return v.get(index);
        }
        return ((Number) vec.getObject(index)).doubleValue();
    }

    /**
     * Returns the minimum of two comparable values, null-safe.
     *
     * @param a first value
     * @param b second value
     * @return minimum value
     */
    @SuppressWarnings("unchecked")
    static Object minOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b;
    }

    /**
     * Returns the maximum of two comparable values, null-safe.
     *
     * @param a first value
     * @param b second value
     * @return maximum value
     */
    @SuppressWarnings("unchecked")
    static Object maxOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b;
    }

    /**
     * Adds two long accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Long
     */
    static Object addLongs(Object a, Object b) {
        if (a == null) {
            return b == null ? 0L : ((Number) b).longValue();
        }
        if (b == null) {
            return ((Number) a).longValue();
        }
        return ((Number) a).longValue() + ((Number) b).longValue();
    }

    /**
     * Adds two double accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Double
     */
    static Object addDoubles(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : ((Number) b).doubleValue();
        }
        if (b == null) {
            return ((Number) a).doubleValue();
        }
        return ((Number) a).doubleValue() + ((Number) b).doubleValue();
    }

    /**
     * Adds numeric aggregate values while preserving decimal precision.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return decimal sum when either value is decimal, otherwise a double sum
     */
    static Object addNumbers(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : b;
        }
        if (b == null) {
            return a;
        }
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            return toBigDecimal(a).add(toBigDecimal(b));
        }
        return addDoubles(a, b);
    }

    /**
     * Converts a numeric aggregate value to BigDecimal without losing integral precision.
     *
     * @param value numeric value
     * @return decimal representation
     */
    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    /**
     * Sets a value in a FieldVector at the given index, handling all supported types.
     *
     * @param vec   field vector
     * @param index row index
     * @param value value to set
     */
    @SuppressWarnings("unchecked")
    static void setVectorValue(FieldVector vec, int index, Object value) {
        if (value == null) {
            vec.setNull(index);
            return;
        }
        if (vec instanceof BigIntVector) {
            ((BigIntVector) vec).setSafe(index, ((Number) value).longValue());
        } else if (vec instanceof IntVector) {
            ((IntVector) vec).setSafe(index, ((Number) value).intValue());
        } else if (vec instanceof SmallIntVector) {
            ((SmallIntVector) vec).setSafe(index, ((Number) value).shortValue());
        } else if (vec instanceof TinyIntVector) {
            ((TinyIntVector) vec).setSafe(index, ((Number) value).byteValue());
        } else if (vec instanceof DecimalVector) {
            ((DecimalVector) vec).setSafe(index, toBigDecimal(value));
        } else if (vec instanceof Float8Vector) {
            ((Float8Vector) vec).setSafe(index, ((Number) value).doubleValue());
        } else if (vec instanceof Float4Vector) {
            ((Float4Vector) vec).setSafe(index, ((Number) value).floatValue());
        } else if (vec instanceof BitVector) {
            ((BitVector) vec).setSafe(index, ((Boolean) value) ? 1 : 0);
        } else if (vec instanceof VarCharVector) {
            byte[] bytes = value instanceof Text
                    ? ((Text) value).getBytes()
                    : value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(index, bytes);
        }
    }

    /**
     * Partitions items into numGroups using round-robin distribution.
     *
     * @param items     items to partition
     * @param numGroups number of groups
     * @return partitioned lists
     */
    static <T> List<List<T>> partitionIntoGroups(List<T> items, int numGroups) {
        List<List<T>> groups = new ArrayList<>(numGroups);
        for (int i = 0; i < numGroups; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < items.size(); i++) {
            groups.get(i % numGroups).add(items.get(i));
        }
        return groups;
    }
}
