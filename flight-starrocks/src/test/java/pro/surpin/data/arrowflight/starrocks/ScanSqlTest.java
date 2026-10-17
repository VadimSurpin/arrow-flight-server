package pro.surpin.data.arrowflight.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ScanSqlTest {
    @Test
    void buildsFullAndProjectedQueries() {
        assertEquals("SELECT * FROM sales.orders",
                ScanSql.fullTable("sales", "orders"));
        assertEquals("SELECT id, amount FROM sales.orders",
                ScanSql.select("sales", "orders", List.of("id", "amount")));
    }

    @Test
    void rejectsInjectedIdentifiers() {
        assertThrows(IllegalArgumentException.class,
                () -> ScanSql.fullTable("sales; DROP TABLE users", "orders"));
        assertThrows(IllegalArgumentException.class,
                () -> ScanSql.select("sales", "orders", List.of("id; DELETE")));
    }

    @Test
    void rendersComparisonAndEscapesStrings() {
        ScanPredicate p = new ScanPredicate.Compare("name", ScanPredicate.Op.EQ,
                new ScanPredicate.StringLit("O'Brien\\x\n"));
        assertEquals("SELECT a FROM s.t WHERE (name = 'O''Brien\\x\n')",
                ScanSql.select("s", "t", List.of("a"), p));
    }

    @Test
    void rendersBooleanTreeInAndNotIn() {
        ScanPredicate p = new ScanPredicate.And(List.of(
                new ScanPredicate.Or(List.of(
                        new ScanPredicate.Compare("a", ScanPredicate.Op.GE, new ScanPredicate.IntLit(1)),
                        new ScanPredicate.IsNull("b", false))),
                new ScanPredicate.Not(new ScanPredicate.In("c",
                        List.of(new ScanPredicate.IntLit(1), new ScanPredicate.IntLit(2))))));
        assertEquals("(((a >= 1) OR (b IS NULL)) AND (NOT (c IN (1, 2))))", p.toSql());
    }

    @Test
    void rendersTemporalAndDecimalLiterals() {
        assertEquals("(d = DATE '2026-10-17')", new ScanPredicate.Compare("d", ScanPredicate.Op.EQ,
                new ScanPredicate.DateLit(java.time.LocalDate.of(2026, 10, 17))).toSql());
        assertEquals("(t < TIMESTAMP '2026-10-17 01:02:03.5')", new ScanPredicate.Compare("t",
                ScanPredicate.Op.LT, new ScanPredicate.TimestampLit(
                        java.time.LocalDateTime.of(2026, 10, 17, 1, 2, 3, 500_000_000))).toSql());
        assertEquals("(x = 12.50)", new ScanPredicate.Compare("x", ScanPredicate.Op.EQ,
                new ScanPredicate.DecimalLit(new java.math.BigDecimal("12.50"))).toSql());
    }

    @Test
    void rejectsBadColumnAndEmptyBooleanTerms() {
        assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.IsNull("a b", false).toSql());
        assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.And(List.of()).toSql());
        assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.In("a", List.of()).toSql());
        assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.Compare("a",
                ScanPredicate.Op.EQ, new ScanPredicate.DoubleLit(Double.NaN)).toSql());
    }
}
