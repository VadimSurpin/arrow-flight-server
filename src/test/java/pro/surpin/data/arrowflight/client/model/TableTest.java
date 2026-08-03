package pro.surpin.data.arrowflight.client.model;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private static Table newTable() {
        return Table.forTable("test_table", "\"");
    }

    // ── toWhereClause ───────────────────────────────────────────────────────

    @Test
    void equalToNumber() {
        EqualTo f = new EqualTo("id", 42);
        String clause = newTable().toWhereClause(f);
        assertEquals("\"id\" = 42", clause);
    }

    @Test
    void equalToString() {
        EqualTo f = new EqualTo("name", "hello");
        String clause = newTable().toWhereClause(f);
        assertEquals("\"name\" = 'hello'", clause);
    }

    @Test
    void equalNullSafe() {
        EqualNullSafe f = new EqualNullSafe("col", "val");
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.contains("is not null"));
        assertTrue(clause.contains("="));
    }

    @Test
    void lessThan() {
        LessThan f = new LessThan("score", 100);
        assertEquals("\"score\" < 100", newTable().toWhereClause(f));
    }

    @Test
    void lessThanOrEqual() {
        LessThanOrEqual f = new LessThanOrEqual("score", 100);
        assertEquals("\"score\" <= 100", newTable().toWhereClause(f));
    }

    @Test
    void greaterThan() {
        GreaterThan f = new GreaterThan("age", 18);
        assertEquals("\"age\" > 18", newTable().toWhereClause(f));
    }

    @Test
    void greaterThanOrEqual() {
        GreaterThanOrEqual f = new GreaterThanOrEqual("age", 18);
        assertEquals("\"age\" >= 18", newTable().toWhereClause(f));
    }

    @Test
    void lessThanString() {
        LessThan f = new LessThan("name", "john");
        assertEquals("\"name\" < 'john'", newTable().toWhereClause(f));
    }

    @Test
    void andTwoFilters() {
        And f = new And(new EqualTo("id", 1), new EqualTo("status", "active"));
        String clause = newTable().toWhereClause(f);
        assertEquals("(\"id\" = 1 and \"status\" = 'active')", clause);
    }

    @Test
    void orTwoFilters() {
        Or f = new Or(new EqualTo("x", 1), new EqualTo("x", 2));
        String clause = newTable().toWhereClause(f);
        assertEquals("(\"x\" = 1 or \"x\" = 2)", clause);
    }

    @Test
    void nestedAndOr() {
        And inner = new And(new EqualTo("a", 1), new EqualTo("b", 2));
        Or f = new Or(inner, new EqualTo("c", 3));
        String clause = newTable().toWhereClause(f);
        assertEquals("((\"a\" = 1 and \"b\" = 2) or \"c\" = 3)", clause);
    }

    @Test
    void isNullFilter() {
        IsNull f = new IsNull("deleted_at");
        assertEquals("\"deleted_at\" is null", newTable().toWhereClause(f));
    }

    @Test
    void isNotNullFilter() {
        IsNotNull f = new IsNotNull("email");
        assertEquals("\"email\" is not null", newTable().toWhereClause(f));
    }

    @Test
    void stringStartsWith() {
        StringStartsWith f = new StringStartsWith("name", "foo");
        assertEquals("\"name\" like 'foo%' escape '\\'", newTable().toWhereClause(f));
    }

    @Test
    void stringContains() {
        StringContains f = new StringContains("desc", "bar");
        assertEquals("\"desc\" like '%bar%' escape '\\'", newTable().toWhereClause(f));
    }

    @Test
    void stringEndsWith() {
        StringEndsWith f = new StringEndsWith("file", ".parquet");
        assertEquals("\"file\" like '%.parquet' escape '\\'", newTable().toWhereClause(f));
    }

    @Test
    void notFilter() {
        Not f = new Not(new EqualTo("active", 1));
        String clause = newTable().toWhereClause(f);
        assertEquals("not (\"active\" = 1)", clause);
    }

    @Test
    void inFilterNumbers() {
        In f = new In("id", new Object[] { 1, 2, 3 });
        assertEquals("\"id\" in (1,2,3)", newTable().toWhereClause(f));
    }

    @Test
    void inFilterStrings() {
        In f = new In("code", new Object[] { "A", "B" });
        assertEquals("\"code\" in ('A','B')", newTable().toWhereClause(f));
    }

    @Test
    void inFilterMixed() {
        In f = new In("col", new Object[] { 1, "two" });
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.startsWith("\"col\" in ("));
    }

    @Test
    void inFilterEmptyArray() {
        In f = new In("id", new Object[0]);
        assertEquals("(1 = 0)", newTable().toWhereClause(f));
    }

    // ── Edge cases ────────────────────────────────────────────────────────

    @Test
    void equalToNullValueThrows() {
        EqualTo f = new EqualTo("col", null);
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    @Test
    void equalNullSafeWithNullValue() {
        EqualNullSafe f = new EqualNullSafe("col", null);
        String clause = newTable().toWhereClause(f);
        assertEquals("\"col\" is null", clause);
    }

    @Test
    void canPushFilterReturnsTrueForSupported() {
        assertTrue(newTable().canPushFilter(new EqualTo("id", 1)));
        assertTrue(newTable().canPushFilter(new And(
                new EqualTo("a", 1), new EqualTo("b", 2))));
    }

    @Test
    void canPushFilterReturnsFalseForUnsupported() {
        assertFalse(newTable().canPushFilter(new AlwaysTrue()));
    }

    @Test
    void notWithUnsupportedChildThrows() {
        Not f = new Not(new AlwaysTrue());
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    @Test
    void v2ColumnComparisonPushesQ4DatePredicate() {
        Predicate predicate = new Predicate("<", new Expression[]{
                FieldReference.column("l_commitdate"),
                FieldReference.column("l_receiptdate")
        });

        assertEquals("\"l_commitdate\" < \"l_receiptdate\"",
                newTable().toWhereClause(predicate));
    }

    @Test
    void v2DateLiteralUsesSqlDateInsteadOfEpochDay() {
        Predicate predicate = new Predicate(">=", new Expression[]{
                FieldReference.column("o_orderdate"),
                new LiteralValue<>(1, DataTypes.DateType)
        });

        assertEquals("\"o_orderdate\" >= '1970-01-02'",
                newTable().toWhereClause(predicate));
    }

    @Test
    void v2NestedExpressionIsNotAcceptedAsFilterOperand() {
        GeneralScalarExpression arithmetic = new GeneralScalarExpression(
                "+", new Expression[]{
                        FieldReference.column("id"),
                        new LiteralValue<>(1, DataTypes.IntegerType)
                });
        Predicate predicate = new Predicate(">", new Expression[]{
                arithmetic, new LiteralValue<>(10, DataTypes.IntegerType)
        });

        assertFalse(newTable().canPushPredicate(predicate));
    }

    @Test
    void sqlLiteralRejectsNaN() {
        EqualTo f = new EqualTo("col", Double.NaN);
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    @Test
    void sqlLiteralRejectsPositiveInfinity() {
        GreaterThan f = new GreaterThan("col", Double.POSITIVE_INFINITY);
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    @Test
    void sqlLiteralRejectsFloatNaN() {
        EqualTo f = new EqualTo("col", Float.NaN);
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    @Test
    void likePatternEscapesPercent() {
        StringStartsWith f = new StringStartsWith("name", "100%");
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.contains("100\\%%"),
                "Pattern % must be escaped in LIKE, got: " + clause);
    }

    @Test
    void likePatternEscapesUnderscore() {
        StringContains f = new StringContains("name", "test_value");
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.contains("test\\_value"),
                "Underscore must be escaped in LIKE, got: " + clause);
    }

    @Test
    void likePatternEscapesBackslash() {
        StringStartsWith f = new StringStartsWith("path", "C:\\Users");
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.contains("C:\\\\Users"),
                "Backslash must be escaped in LIKE, got: " + clause);
    }

    @Test
    void sqlLiteralBooleanTrue() {
        EqualTo f = new EqualTo("active", true);
        assertEquals("\"active\" = true", newTable().toWhereClause(f));
    }

    @Test
    void sqlLiteralBooleanFalse() {
        EqualTo f = new EqualTo("active", false);
        assertEquals("\"active\" = false", newTable().toWhereClause(f));
    }

    @Test
    void filterWithQuoteInColumnName() {
        Table t = Table.forTable("tab\"le", "\"");
        EqualTo f = new EqualTo("na\"me", "val");
        String clause = t.toWhereClause(f);
        assertTrue(clause.contains("\"na\"\"me\""),
                "Quote in column name must be doubled, got: " + clause);
    }

    @Test
    void filterWithQuoteInStringLiteral() {
        EqualTo f = new EqualTo("name", "it's");
        String clause = newTable().toWhereClause(f);
        assertTrue(clause.contains("'it''s'"),
                "Quote in string literal must be doubled, got: " + clause);
    }

    @Test
    void unsupportedFilterTypeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(new AlwaysTrue()));
    }

    @Test
    void andWithOneUnsupportedChildThrows() {
        And f = new And(new EqualTo("id", 1), new AlwaysTrue());
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    @Test
    void orWithOneUnsupportedChildThrows() {
        Or f = new Or(new EqualTo("x", 1), new AlwaysTrue());
        assertThrows(IllegalArgumentException.class,
                () -> newTable().toWhereClause(f));
    }

    // ── forTable ────────────────────────────────────────────────────────────

    @Test
    void forTableQueryWrapsInSubquery() {
        Table t = Table.forTable("SELECT * FROM remote_table", "\"");
        assertEquals("(SELECT * FROM remote_table) t", t.getName());
    }

    // ── post-pushdown scan-output estimates ─────────────────────────────────

    @Test
    void estimateWithoutPushdownReturnsRawStatistics() {
        Table t = newTable();
        long[] est = t.estimateScanOutput(1_000L, 10_000L);
        assertEquals(1_000L, est[0], "rows unchanged with default selectivity 1.0");
        assertEquals(10_000L, est[1], "bytes unchanged with default selectivity 1.0");
    }

    @Test
    void estimateScalesRowsAndBytesByFilterSelectivity() {
        Table t = newTable();
        t.applyPushdownEstimateHints(0.1, false);
        long[] est = t.estimateScanOutput(1_000L, 10_000L);
        assertEquals(100L, est[0], "rows scaled by 0.1 selectivity");
        assertEquals(1_000L, est[1], "bytes scaled by the same row fraction");
    }

    @Test
    void estimateGlobalAggregationReturnsSingleRow() {
        Table t = newTable();
        t.applyPushdownEstimateHints(0.3, true);
        long[] est = t.estimateScanOutput(1_000_000L, 50_000_000L);
        assertEquals(1L, est[0], "global aggregation yields exactly one row");
        assertEquals(50L, est[1], "bytes scaled to the single-row fraction");
    }

    @Test
    void estimatePreservesUnknownRawStatistics() {
        Table t = newTable();
        t.applyPushdownEstimateHints(0.1, false);
        long[] est = t.estimateScanOutput(-1L, -1L);
        assertEquals(-1L, est[0], "unknown raw rows stay unknown");
        assertEquals(-1L, est[1], "unknown raw bytes stay unknown");
    }

    @Test
    void estimateClampsInvalidSelectivityToOne() {
        Table t = newTable();
        t.applyPushdownEstimateHints(5.0, false);
        long[] est = t.estimateScanOutput(1_000L, 10_000L);
        assertEquals(1_000L, est[0], "out-of-range selectivity falls back to 1.0");
    }
}
