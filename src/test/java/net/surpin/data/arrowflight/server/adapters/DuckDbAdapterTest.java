package net.surpin.data.arrowflight.server.adapters;

import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests DuckDB SQL helpers, connection setup, and Flight backpressure handling. */
@Tag("unit")
class DuckDbAdapterTest {

    @Test
    void sqlStringLiteralNormal() {
        assertEquals("'hello'", DuckDbAdapter.sqlStringLiteral("hello"));
    }

    @Test
    void sqlStringLiteralEscapesSingleQuote() {
        assertEquals("'it''s'", DuckDbAdapter.sqlStringLiteral("it's"));
    }

    @Test
    void sqlStringLiteralEmpty() {
        assertEquals("''", DuckDbAdapter.sqlStringLiteral(""));
    }

    @Test
    void quoteIdentifierNormal() {
        assertEquals("\"table_name\"", DuckDbAdapter.quoteIdentifier("table_name"));
    }

    @Test
    void quoteIdentifierEscapesDoubleQuote() {
        assertEquals("\"col\"\"name\"", DuckDbAdapter.quoteIdentifier("col\"name"));
    }

    @Test
    void readParquetFromClauseSingleFile() {
        String result = DuckDbAdapter.readParquetFromClause(List.of("/data/file.parquet"));
        assertEquals("read_parquet(['/data/file.parquet'])", result);
    }

    @Test
    void readParquetFromClauseMultipleFiles() {
        String result = DuckDbAdapter.readParquetFromClause(
                List.of("/data/a.parquet", "/data/b.parquet"));
        assertEquals("read_parquet(['/data/a.parquet', '/data/b.parquet'])", result);
    }

    @Test
    void readParquetFromClauseEmpty() {
        String result = DuckDbAdapter.readParquetFromClause(List.of());
        assertEquals("read_parquet([])", result);
    }

    /** Verifies readiness changes cannot be lost before handler registration. */
    @Test
    void listenerReadinessChangeDoesNotWaitForTimeout() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            ready.set(true);
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertDoesNotThrow(
                        () -> DuckDbAdapter.awaitListenerReady(listener, 200)));
        verify(listener).setOnReadyHandler(any(Runnable.class));
    }

    /** Verifies polling observes readiness when Arrow never invokes its callback. */
    @Test
    void listenerReadinessWithoutCallbackIsPolled() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        CountDownLatch handlerRegistered = new CountDownLatch(1);
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            handlerRegistered.countDown();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> result = executor.submit(() -> {
                DuckDbAdapter.awaitListenerReady(listener, 500);
                return null;
            });
            assertTrue(handlerRegistered.await(1, TimeUnit.SECONDS));
            ready.set(true);
            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies a readiness signal received before waiting remains observable. */
    @Test
    void listenerReadinessSignalBeforeWaitIsRetained() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        AtomicInteger readinessChecks = new AtomicInteger();
        when(listener.isReady()).thenAnswer(invocation ->
                readinessChecks.getAndIncrement() > 0 && ready.get());
        doAnswer(invocation -> {
            ready.set(true);
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertDoesNotThrow(
                        () -> DuckDbAdapter.awaitListenerReady(listener, 200)));
    }

    /** Verifies a spurious wake-up cannot release a non-ready listener. */
    @Test
    void listenerSpuriousWakeupKeepsWaiting() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        AtomicReference<Runnable> readyHandler = new AtomicReference<>();
        CountDownLatch handlerRegistered = new CountDownLatch(1);
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            readyHandler.set(invocation.getArgument(0));
            handlerRegistered.countDown();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> result = executor.submit(() -> {
                DuckDbAdapter.awaitListenerReady(listener, 500);
                return null;
            });
            assertTrue(handlerRegistered.await(1, TimeUnit.SECONDS));
            readyHandler.get().run();
            assertThrows(TimeoutException.class,
                    () -> result.get(50, TimeUnit.MILLISECONDS));

            ready.set(true);
            readyHandler.get().run();
            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies cancellation releases a signalled waiter without sending data. */
    @Test
    void listenerCancellationStopsWaiting() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicReference<Runnable> cancelHandler = new AtomicReference<>();
        CountDownLatch cancelHandlerRegistered = new CountDownLatch(1);
        when(listener.isCancelled()).thenAnswer(invocation -> cancelled.get());
        doAnswer(invocation -> {
            cancelHandler.set(invocation.getArgument(0));
            cancelHandlerRegistered.countDown();
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> result = executor.submit(() -> {
                DuckDbAdapter.awaitListenerReady(listener, 500);
                return null;
            });
            assertTrue(cancelHandlerRegistered.await(1, TimeUnit.SECONDS));
            cancelled.set(true);
            cancelHandler.get().run();
            java.util.concurrent.ExecutionException exception = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> result.get(1, TimeUnit.SECONDS));
            FlightRuntimeException cause = assertInstanceOf(
                    FlightRuntimeException.class, exception.getCause());
            assertEquals(FlightStatusCode.CANCELLED, cause.status().code());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies an absent readiness signal is bounded by configured timeout. */
    @Test
    void listenerWithoutSignalTimesOut() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        FlightRuntimeException exception = assertTimeoutPreemptively(
                Duration.ofSeconds(1), () -> assertThrows(FlightRuntimeException.class,
                        () -> DuckDbAdapter.awaitListenerReady(listener, 25)));
        assertEquals(FlightStatusCode.TIMED_OUT, exception.status().code());
    }

    /** Verifies non-positive readiness timeouts fail before waiting. */
    @Test
    void listenerRejectsNonPositiveTimeout() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        assertThrows(IllegalArgumentException.class,
                () -> DuckDbAdapter.awaitListenerReady(listener, 0));
    }

    @Test
    void ignoresHdfsOptionsWhenExtensionIsNotConfigured() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 4096, 1, 131072, 1, 1, 1,
                null, false, null, null,
                "true", "/var/lib/hadoop-hdfs/socket/dn_socket",
                false, 1048576, 67108864, 60000L, "/data/parquet", null,
                32010, 5701, 60,
                3, 1000, 30000);

        try {
            DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool);
            try (Connection connection = adapter.connection()) {
                assertFalse(connection.isClosed());
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies DuckDB results are streamed through the Flight listener. */
    @Test
    void streamSqlDeliversDuckDbRowsToFlight() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 1, 1, 1,
                null, false, null, null,
                null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
        AtomicInteger rows = new AtomicInteger();
        doAnswer(invocation -> {
            rootRef.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            rows.addAndGet(rootRef.get().getRowCount());
            return null;
        }).when(listener).putNext();

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            adapter.streamSql(allocator,
                    "SELECT range AS id FROM range(0, 5)", listener, true);
        } finally {
            ioPool.shutdownNow();
        }

        assertEquals(5, rows.get());
        verify(listener).start(any(VectorSchemaRoot.class));
    }

    /** Verifies streaming reuses DuckDB batches without scheduling background buffering. */
    @Test
    void streamSqlDoesNotScheduleBatchBuffering() throws Exception {
        ExecutorService ioPool = mock(ExecutorService.class);
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            adapter.streamSql(allocator,
                    "SELECT range AS id FROM range(0, 5)", listener, true);
        }

        verify(listener).start(any(VectorSchemaRoot.class));
        verify(listener, org.mockito.Mockito.times(3)).putNext();
        verifyNoInteractions(ioPool);
    }

    /** Verifies stream completion is signalled immediately after the final batch. */
    @Test
    void streamSqlDoesNotAddPollingDelayAfterFinalBatch() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 1, 1, 1,
                null, false, null, null,
                null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        CountDownLatch finalBatchSent = new CountDownLatch(3);
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            finalBatchSent.countDown();
            return null;
        }).when(listener).putNext();

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            Future<Void> result = caller.submit(() -> {
                adapter.streamSql(allocator,
                        "SELECT range AS id FROM range(0, 5)", listener, true);
                return null;
            });
            assertTrue(finalBatchSent.await(1, TimeUnit.SECONDS));
            assertNull(result.get(75, TimeUnit.MILLISECONDS));
        } finally {
            caller.shutdownNow();
            ioPool.shutdownNow();
        }
    }

    /** Verifies a stalled Flight consumer aborts the DuckDB producer with timeout status. */
    @Test
    void streamSqlPropagatesListenerTimeout() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 1, 1, 1,
                null, false, null, null,
                null, null, false, 1048576, 67108864, 25L, null, null,
                32010, 5701, 60, 3, 1000, 30000);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            FlightRuntimeException exception = assertTimeoutPreemptively(
                    Duration.ofSeconds(1), () -> assertThrows(FlightRuntimeException.class,
                            () -> adapter.streamSql(allocator,
                                    "SELECT range AS id FROM range(0, 5)",
                                    listener, true)));
            assertEquals(FlightStatusCode.TIMED_OUT, exception.status().code());
        } finally {
            ioPool.shutdownNow();
        }
    }

    // ── buildDuckSql / buildDuckSqlWithFilter ─────────────────────────────

    @Test
    void buildDuckSqlWithColumnSelect() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t WHERE id > 0");
        String sql = DuckDbAdapter.buildDuckSql(pq, "my_from");
        assertTrue(sql.contains("SELECT \"id\", \"name\" FROM my_from"));
        assertTrue(sql.contains("\"id\" > 0"));
    }

    @Test
    void buildDuckSqlWithCountStar() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("count(*)"));
        assertTrue(sql.contains("FROM t0"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithGroupBy() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT bool_col, count(*) FROM s.t GROUP BY bool_col");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("GROUP BY \"bool_col\""), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithFilter() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    /** Verifies the TPC-H Q4 column comparison reaches DuckDB unchanged. */
    @Test
    void buildDuckSqlWithQ4ColumnComparison() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT l_orderkey FROM tpch.lineitem "
                        + "WHERE l_commitdate < l_receiptdate");

        String sql = DuckDbAdapter.buildDuckSql(pq, "lineitem_files");

        assertTrue(sql.contains("\"l_commitdate\" < \"l_receiptdate\""),
                "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithFilterAppliedSkipsWhere() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10");
        String sql = DuckDbAdapter.buildDuckSqlWithFilter(pq, "t0", true);
        assertFalse(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithSumMinMax() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount), min(id), max(id) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("sum(\"amount\")"), "Got: " + sql);
        assertTrue(sql.contains("min(\"id\")"), "Got: " + sql);
        assertTrue(sql.contains("max(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlDoesNotQuoteDecimalSumAsOneIdentifier() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(cast(l_extendedprice * (1 - l_discount) "
                        + "as decimal(32,4))) FROM tpch.lineitem");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");

        assertTrue(sql.contains("sum(cast("), "Got: " + sql);
        assertTrue(sql.contains("\"l_extendedprice\""), "Got: " + sql);
        assertTrue(sql.contains("\"l_discount\""), "Got: " + sql);
        assertFalse(sql.contains("sum(\"cast("), "Got: " + sql);
    }

    @Test
    void buildDuckSqlPreservesAggregateAliases() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT region, sum(revenue) AS total, count(*) AS rows "
                        + "FROM s.t GROUP BY region");

        String sql = DuckDbAdapter.buildDuckSql(pq, "read_parquet('part.parquet')");

        assertTrue(sql.contains("sum(\"revenue\") AS \"total\""), "Got: " + sql);
        assertTrue(sql.contains("count(*) AS \"rows\""), "Got: " + sql);
    }

    @Test
    void decimalSumExpressionExecutesInDuckDb() throws Exception {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(cast(l_extendedprice * (1 - l_discount) "
                        + "as decimal(32,4))) FROM tpch.lineitem");
        String sql = DuckDbAdapter.buildDuckSql(pq, "q1_input");

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE q1_input (l_extendedprice DECIMAL(15,2), "
                    + "l_discount DECIMAL(15,2))");
            statement.execute("INSERT INTO q1_input VALUES (100.00, 0.10)");
            try (ResultSet result = statement.executeQuery(sql)) {
                assertTrue(result.next());
                assertEquals(0, new java.math.BigDecimal("90.0000")
                        .compareTo(result.getBigDecimal(1)));
                assertEquals(38, result.getMetaData().getPrecision(1));
                assertEquals(4, result.getMetaData().getScale(1));
            }
        }
    }

    @Test
    void buildDuckSqlWithCountColumn() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(id) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("count(\"id\")"), "Got: " + sql);
    }

    // ── buildGroupedDuckSql ───────────────────────────────────────────────

    @Test
    void buildGroupedDuckSqlSingleFile() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildGroupedDuckSql(pq, 1, false);
        assertTrue(sql.contains("FROM \"t0\""), "Got: " + sql);
        assertFalse(sql.contains("UNION ALL"), "Got: " + sql);
    }

    @Test
    void buildGroupedDuckSqlMultipleFiles() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildGroupedDuckSql(pq, 3, false);
        assertTrue(sql.contains("UNION ALL"), "Got: " + sql);
        assertTrue(sql.contains("\"t0\""), "Got: " + sql);
        assertTrue(sql.contains("\"t1\""), "Got: " + sql);
        assertTrue(sql.contains("\"t2\""), "Got: " + sql);
        assertTrue(sql.startsWith("SELECT count(*) AS \"count(*)\" FROM ("), "Got: " + sql);
    }

    // ── buildSelectSql ────────────────────────────────────────────────────

    @Test
    void buildSelectSqlSelectStar() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq,
                DuckDbAdapter.readParquetFromClause(List.of("/data/f.parquet")));
        assertTrue(sql.contains("SELECT *"), "Got: " + sql);
        assertTrue(sql.contains("read_parquet"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithColumns() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("\"id\", \"name\""), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithFilter() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM s.t WHERE id = 1");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithAggregation() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t WHERE id > 0");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("count(*)"), "Got: " + sql);
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithCountColumn() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("count(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithSum() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("sum(\"amount\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithMin() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT min(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("min(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithMax() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT max(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("max(\"id\")"), "Got: " + sql);
    }

    // ── appendSelectExpr ──────────────────────────────────────────────────

    @Test
    void appendSelectExprCountStar() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT count(*) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().startsWith("count(*)"),
                "Got: " + sb);
    }

    @Test
    void appendSelectExprCountColumn() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT count(id) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("count(\"id\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprSum() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT sum(amount) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("sum(\"amount\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprMin() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT min(price) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("min(\"price\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprMax() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT max(score) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("max(\"score\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprColumn() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT col_name FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("\"col_name\""), "Got: " + sb);
    }

    private static ParquetQueryParser.SelectExpr parseSingleExpr(String sql) {
        return ParquetQueryParser.parse(sql).selectExprs.get(0);
    }
}
