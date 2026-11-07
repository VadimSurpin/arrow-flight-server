package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeInFiltersTest {

    @Test
    void blockRoundTripsFiltersInOrder() {
        List<RuntimeInFilters.Filter> filters = List.of(
                new RuntimeInFilters.Filter("l_partkey", new long[] {3, -5, Long.MAX_VALUE}),
                new RuntimeInFilters.Filter("l_suppkey", new long[] {}));

        List<RuntimeInFilters.Filter> decoded = RuntimeInFilters.decode(RuntimeInFilters.encode(filters));

        assertEquals(2, decoded.size());
        assertEquals("l_partkey", decoded.get(0).column());
        assertArrayEquals(new long[] {3, -5, Long.MAX_VALUE}, decoded.get(0).values());
        assertEquals("l_suppkey", decoded.get(1).column());
        assertEquals(0, decoded.get(1).values().length);
    }

    @Test
    void decodeRejectsWrongMagicTruncationAndTrailingBytes() {
        byte[] good = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("k", new long[] {1, 2})));

        byte[] wrongMagic = good.clone();
        wrongMagic[0] ^= 0x55;
        assertThrows(IllegalArgumentException.class, () -> RuntimeInFilters.decode(wrongMagic));
        assertThrows(IllegalArgumentException.class,
                () -> RuntimeInFilters.decode(java.util.Arrays.copyOf(good, good.length - 3)));
        assertThrows(IllegalArgumentException.class,
                () -> RuntimeInFilters.decode(java.util.Arrays.copyOf(good, good.length + 1)));
    }

    @Test
    void decodeRejectsAValueCountLargerThanTheBlock() {
        ByteBuffer block = ByteBuffer.allocate(4 + 4 + 4 + 1 + 4).order(ByteOrder.LITTLE_ENDIAN);
        block.putInt(RuntimeInFilters.MAGIC).putInt(1).putInt(1).put((byte) 'k').putInt(1_000_000);
        assertThrows(IllegalArgumentException.class, () -> RuntimeInFilters.decode(block.array()));
    }

    @Test
    void decodeRejectsMoreValuesThanTheLimit() {
        long[] tooMany = new long[RuntimeInFilters.MAX_VALUES + 1];
        byte[] block = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("k", tooMany)));
        assertThrows(IllegalArgumentException.class, () -> RuntimeInFilters.decode(block));
    }

    @Test
    void applyWrapsTheQueryWithAnInListOnASelectedColumn() {
        String sql = RuntimeInFilters.apply("SELECT a, b FROM t", List.of("a", "b"),
                List.of(new RuntimeInFilters.Filter("a", new long[] {1, 2, 3})));
        assertEquals("SELECT * FROM (SELECT a, b FROM t) AS rf WHERE \"a\" IN (1,2,3)", sql);
    }

    @Test
    void applyMatchesColumnsIgnoringCaseAndQuotesTheNameFromTheSelectList() {
        String sql = RuntimeInFilters.apply("SELECT L_PARTKEY FROM t", List.of("L_PARTKEY"),
                List.of(new RuntimeInFilters.Filter("l_partkey", new long[] {7})));
        assertEquals("SELECT * FROM (SELECT L_PARTKEY FROM t) AS rf WHERE \"L_PARTKEY\" IN (7)", sql);
    }

    @Test
    void applyIgnoresFiltersOnColumnsTheQueryDoesNotReturn() {
        String sql = "SELECT a FROM t";
        assertEquals(sql, RuntimeInFilters.apply(sql, List.of("a"),
                List.of(new RuntimeInFilters.Filter("zzz", new long[] {1}))));
        assertEquals(sql, RuntimeInFilters.apply(sql, List.of(),
                List.of(new RuntimeInFilters.Filter("a", new long[] {1}))));
        assertEquals(sql, RuntimeInFilters.apply(sql, List.of("a"), List.of()));
    }

    @Test
    void applyAnEmptyValueSetMatchesNoRows() {
        String sql = RuntimeInFilters.apply("SELECT a FROM t", List.of("a"),
                List.of(new RuntimeInFilters.Filter("a", new long[] {})));
        assertTrue(sql.endsWith("WHERE FALSE"), sql);
    }

    @Test
    void applyCombinesSeveralFiltersWithAnd() {
        String sql = RuntimeInFilters.apply("SELECT a, b FROM t", List.of("a", "b"), List.of(
                new RuntimeInFilters.Filter("a", new long[] {1}),
                new RuntimeInFilters.Filter("b", new long[] {2, 3})));
        assertTrue(sql.endsWith("WHERE \"a\" IN (1) AND \"b\" IN (2,3)"), sql);
    }

    @Test
    void wrappedQueryReturnsOnlyTheMatchingRowsInDuckDb() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE t AS SELECT range AS a, range * 10 AS b FROM range(100)");
            String sql = RuntimeInFilters.apply("SELECT a, b FROM t", List.of("a", "b"),
                    List.of(new RuntimeInFilters.Filter("a", new long[] {4, 40, 400})));
            List<Long> got = new ArrayList<>();
            try (ResultSet rows = statement.executeQuery(sql + " ORDER BY a")) {
                while (rows.next()) {
                    got.add(rows.getLong("a"));
                    assertEquals(rows.getLong("a") * 10, rows.getLong("b"));
                }
            }
            assertEquals(List.of(4L, 40L), got);
        }
    }
}
