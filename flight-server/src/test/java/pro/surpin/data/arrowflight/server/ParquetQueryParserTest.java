package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetQueryParserTest {

    /** Verifies repeated SQL reuses an immutable parsed representation. */
    @Test
    void repeatedSqlUsesCachedImmutableParse() {
        String sql = "SELECT cache_probe FROM s.cache_table WHERE cache_probe > 42";

        ParquetQueryParser first = ParquetQueryParser.parse(sql);
        ParquetQueryParser second = ParquetQueryParser.parse(sql);

        assertSame(first, second);
        assertThrows(UnsupportedOperationException.class,
                () -> first.columns.add("unexpected"));
    }

    @Test
    void selectStarReturnsEmptyColumns() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT * FROM my_schema.my_table");
        assertEquals("my_schema", p.schema);
        assertEquals("my_table", p.table);
        // jOOQ cannot expand * without table metadata → empty list signals "all columns"
        assertTrue(p.columns.isEmpty(), "SELECT * should produce empty columns list");
    }

    @Test
    void selectStarNoSchemaQualifier() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT * FROM my_table");
        assertNull(p.schema, "Unqualified table name should have null schema");
        assertEquals("my_table", p.table);
        assertTrue(p.columns.isEmpty());
    }

    @Test
    void selectSpecificColumnsReturnsColumnList() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT id, name, value FROM s.t");
        assertEquals("s", p.schema);
        assertEquals("t", p.table);
        assertEquals(List.of("id", "name", "value"), p.columns);
    }

    @Test
    void filterIsRenderedWithDoubleQuotes() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT * FROM s.t WHERE tinyint_col = 0");
        assertNotNull(p.filter);
        assertFalse(p.filter.trim().isEmpty(), "Filter should not be empty for WHERE clause");
        assertTrue(p.filter.contains("\"tinyint_col\""),
                "Column names in filter should be double-quoted, got: " + p.filter);
    }

    @Test
    void noWhereClauseParsesToEmptyFilter() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT * FROM s.t");
        // jOOQ renders noCondition() as empty string
        assertTrue(p.filter == null || p.filter.trim().isEmpty(),
                "No WHERE clause should yield null or empty filter, got: " + p.filter);
    }

    @Test
    void compoundFilterAndRenderedCorrectly() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10 AND id < 100");
        assertNotNull(p.filter);
        assertTrue(p.filter.contains("\"id\""), "Filter should contain double-quoted id");
        assertTrue(p.filter.contains("10") && p.filter.contains("100"),
                "Filter should retain literal values");
    }

    @Test
    void filterWithStringLiteral() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT * FROM s.t WHERE name = 'alice'");
        assertNotNull(p.filter);
        assertTrue(p.filter.contains("'alice'") || p.filter.contains("alice"),
                "String literal should appear in filter: " + p.filter);
    }

    @Test
    void nonSelectQueryThrowsException() {
        assertThrows(RuntimeException.class,
                () -> ParquetQueryParser.parse("UPDATE s.t SET id = 1"));
    }

    @Test
    void multiTableFromClauseThrowsException() {
        // jOOQ parses "FROM a, b" as two separate table references → size != 1 → exception
        assertThrows(RuntimeException.class,
                () -> ParquetQueryParser.parse("SELECT * FROM a, b WHERE a.id = b.id"));
    }

    @Test
    void toStringContainsAllFields() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT col FROM s.t WHERE col > 0");
        String str = p.toString();
        assertTrue(str.contains("schema="), "toString should include schema");
        assertTrue(str.contains("table="), "toString should include table");
        assertTrue(str.contains("columns="), "toString should include columns");
        assertTrue(str.contains("filter="), "toString should include filter");
    }

    // ── JOIN parsing ──────────────────────────────────────────────────────

    @Test
    void parseJoinSelectDetectsJoin() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT a.id, b.name FROM s.t1 a JOIN s.t2 b ON a.id = b.t1_id");
        assertTrue(p.isJoin, "JOIN query should have isJoin=true");
        assertNotNull(p.joinTables, "joinTables should not be null");
        assertTrue(p.joinTables.size() >= 2, "Should extract at least 2 tables, got: " + p.joinTables);
    }

    @Test
    void parseJoinSelectInnerJoin() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT * FROM s.t1 INNER JOIN s.t2 ON t1.id = t2.id");
        assertTrue(p.isJoin);
        assertFalse(p.joinTables.isEmpty(), "INNER JOIN should extract tables");
    }

    @Test
    void parseJoinSelectLeftJoin() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id");
        assertTrue(p.isJoin);
        assertEquals(2, p.joinTables.size(),
                "LEFT JOIN should extract 2 tables, got: " + p.joinTables.size());
    }

    @Test
    void parseJoinSelectCollectedColumns() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT t1.col_a, t2.col_b FROM s.t1 t1 JOIN s.t2 t2 ON t1.id = t2.id");
        assertEquals(List.of("col_a", "col_b"), p.columns,
                "JOIN query should collect selected columns");
    }

    @Test
    void parseJoinSelectHasDuckDbSql() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT * FROM s.t1 JOIN s.t2 ON t1.id = t2.id");
        assertNotNull(p.duckDbSql, "JOIN query should have duckDbSql");
        assertFalse(p.duckDbSql.isBlank(), "duckDbSql should not be blank");
    }
}
