package net.surpin.data.arrowflight.server;

import net.surpin.data.arrowflight.server.model.ExecutionPath;
import net.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JoinIntegrationTest {

    private TestFlightServerHelper helper;
    private String fullTable;

    @BeforeAll
    void setUp() throws Exception {
        helper = TestFlightServerHelper.builder().start();
        fullTable = "test_schema.test_table";
    }

    @AfterAll
    void tearDown() throws Exception {
        if (helper != null) helper.close();
    }

    @Test
    void selfJoinDifferentAliases() throws Exception {
        String query = "SELECT a.id, b.id "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.id";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");
        assertEquals(2, pq.joinTables.size(), "Should have 2 table references");
        assertNotNull(pq.duckDbSql, "Should produce DuckDB SQL");

        CountingListener listener = new CountingListener();
        ExecutionPathTracker pathTracker = new ExecutionPathTracker();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        assertTrue(files.length > 0, "Should find files for join query");
        helper.executionService.readParquet(
                helper.allocator, query, files, listener, true, pathTracker);
        assertTrue(listener.totalRows > 0, "Self-join should return rows");
        assertEquals(ExecutionPath.DUCKDB_JOIN, pathTracker.path());
    }

    @Test
    void innerJoinOnMatchingColumn() throws Exception {
        String query = "SELECT a.id, a.bool_col, b.tinyint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.tinyint_col";

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertTrue(listener.totalRows > 0, "Inner join should return matching rows");
    }

    @Test
    void joinSchemaExtraction() {
        String query = "SELECT a.id, a.bool_col, b.tinyint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.tinyint_col";

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(3, schema.getFields().size(), "Should have 3 output columns");
    }

    @Test
    void joinQueryParserExtractsAliases() {
        String query = "SELECT t1.id, t2.bool_col "
                + "FROM " + fullTable + " t1 "
                + "JOIN " + fullTable + " t2 "
                + "ON t1.id = t2.id";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertEquals("t1", pq.joinTables.get(0).alias());
        assertEquals("t2", pq.joinTables.get(1).alias());
    }

    @Test
    void joinQueryParserProducesDuckDbSql() {
        String query = "SELECT a.id, b.id "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.id";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        String duckSql = pq.duckDbSql;
        assertNotNull(duckSql);
        assertTrue(duckSql.contains("JOIN b"), "DuckDB SQL should use alias 'b': " + duckSql);
        assertFalse(duckSql.contains("test_schema.test_table"),
                "DuckDB SQL should not contain schema-qualified names: " + duckSql);
    }

    @Test
    void innerJoinIntToSmallint() throws Exception {
        String query = "SELECT a.id, b.smallint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.smallint_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertEquals(1000, listener.totalRows,
                "Join id(INT32) = smallint_col(INT16) must return 1000 pairs");
    }

    @Test
    void innerJoinIntToBigint() throws Exception {
        String query = "SELECT a.id, b.bigint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.bigint_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertEquals(1000, listener.totalRows,
                "Join id(INT32) = bigint_col(INT64) must return 1000 pairs");
    }

    @Test
    void innerJoinIntToFloat() throws Exception {
        String query = "SELECT a.id, b.float_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.float_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertTrue(listener.totalRows >= 0,
                "Join id(INT32) = float_col(FLOAT) must not throw ClassCastException");
    }

    @Test
    void innerJoinIntToDouble() throws Exception {
        String query = "SELECT a.id, b.double_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.id = b.double_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertTrue(listener.totalRows >= 0,
                "Join id(INT32) = double_col(DOUBLE) must not throw ClassCastException");
    }

    @Test
    void innerJoinFloatToDouble() throws Exception {
        String query = "SELECT a.float_col, b.double_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.float_col = b.double_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertTrue(listener.totalRows >= 0,
                "Join float_col(FLOAT) = double_col(DOUBLE) must not throw ClassCastException");
    }

    @Test
    void innerJoinBoolToTinyint() throws Exception {
        String query = "SELECT a.bool_col, b.tinyint_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.bool_col = b.tinyint_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertEquals(100000, listener.totalRows,
                "Join bool_col(BOOL) = tinyint_col(INT8) must return 100000 pairs without ClassCastException");
    }

    @Test
    void innerJoinStringColumns() throws Exception {
        String query = "SELECT a.date_string_col, b.string_col "
                + "FROM " + fullTable + " a "
                + "JOIN " + fullTable + " b "
                + "ON a.date_string_col = b.string_col";

        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        assertTrue(pq.isJoin, "Should detect as join");

        org.apache.arrow.vector.types.pojo.Schema schema = helper.metadataService.getQuerySchema(query);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size(), "Should have 2 output columns");

        CountingListener listener = new CountingListener();
        String[] files = helper.parquetAdapter.locationsForQuery(query)
                .keySet().toArray(new String[0]);
        helper.executionService.readParquet(helper.allocator, query, files, listener, true);
        assertEquals(0, listener.totalRows,
                "Join date_string_col(STRING) = string_col(STRING) must return 0 rows without ClassCastException");
    }

    static class CountingListener implements FlightProducer.ServerStreamListener {
        int totalRows;
        VectorSchemaRoot root;
        @Override public void start(VectorSchemaRoot r, DictionaryProvider p, IpcOption o) { this.root = r; }
        @Override public void putNext() { totalRows += root.getRowCount(); }
        @Override public void putNext(ArrowBuf metadata) { totalRows += root.getRowCount(); }
        @Override public void putMetadata(ArrowBuf metadata) {
            // Metadata frames do not contribute to the row count asserted by this listener.
        }
        @Override public boolean isReady() { return true; }
        @Override public boolean isCancelled() { return false; }
        @Override public void setOnReadyHandler(Runnable handler) {
            // The test listener is always ready and never needs a readiness callback.
        }
        @Override public void setOnCancelHandler(Runnable handler) {
            // The test listener is never cancelled and does not register a callback.
        }
        @Override public void error(Throwable ex) { throw new RuntimeException(ex); }
        @Override public void completed() {
            // Completion is observed through the accumulated row count.
        }
    }
}
