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

    @Test
    void rendersGroupedPartialAggregates() {
        ScanAggregate agg = new ScanAggregate(List.of("k"), List.of(
                ScanAggregate.Partial.column(ScanAggregate.Func.COUNT_STAR, null, "__agg0"),
                ScanAggregate.Partial.column(ScanAggregate.Func.SUM, "d", "__agg1"),
                ScanAggregate.Partial.column(ScanAggregate.Func.MIN, "x", "__agg2"),
                ScanAggregate.Partial.column(ScanAggregate.Func.MAX, "x", "__agg3"),
                ScanAggregate.Partial.column(ScanAggregate.Func.COUNT, "x", "__agg4")));
        ScanPredicate p = new ScanPredicate.IsNull("k", true);
        assertEquals("SELECT k, COUNT(*) AS __agg0, SUM(CAST(d AS DOUBLE)) AS __agg1, MIN(x) AS __agg2, MAX(x) AS __agg3, "
                + "COUNT(x) AS __agg4 FROM s.t WHERE (k IS NOT NULL) GROUP BY k",
                ScanSql.select("s", "t", agg, p));
    }

    @Test
    void ungroupedAggregateHasNoGroupByAndNoFilterMeansNoWhere() {
        ScanAggregate agg = new ScanAggregate(List.of(), List.of(
                ScanAggregate.Partial.column(ScanAggregate.Func.COUNT_STAR, null, "__agg0")));
        assertEquals("SELECT COUNT(*) AS __agg0 FROM s.t", ScanSql.select("s", "t", agg, null));
    }

    @Test
    void rejectsBadIdentifiersAndEmptyAggregates() {
        assertThrows(IllegalArgumentException.class, () -> ScanSql.select("s", "t", new ScanAggregate(List.of(),
                List.of(ScanAggregate.Partial.column(ScanAggregate.Func.COUNT, "x; y", "__agg0"))), null));
        assertThrows(IllegalArgumentException.class, () -> ScanSql.select("s", "t", new ScanAggregate(List.of(),
                List.of(ScanAggregate.Partial.column(ScanAggregate.Func.COUNT, "x", "a b"))), null));
        assertThrows(IllegalArgumentException.class, () -> new ScanAggregate(List.of("k"), List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> ScanAggregate.Partial.column(ScanAggregate.Func.COUNT, null, "__agg0"));
    }

    @Test
    void rendersExpressionPartials() {
        ScanExpr discounted = new ScanExpr.Arithmetic(ScanExpr.Op.MUL, new ScanExpr.Column("price"),
                new ScanExpr.Arithmetic(ScanExpr.Op.SUB,
                        new ScanExpr.Constant(new ScanPredicate.IntLit(1)), new ScanExpr.Column("disc")));
        ScanAggregate agg = new ScanAggregate(List.of("k"), List.of(
                new ScanAggregate.Partial(ScanAggregate.Func.SUM, ScanExpr.Cast.toDecimal(discounted, 38, 4), "__agg0"),
                new ScanAggregate.Partial(ScanAggregate.Func.MAX, new ScanExpr.Column("price"), "__agg1")));
        assertEquals("SELECT k, SUM(CAST((price * (1 - disc)) AS DECIMAL(38,4))) AS __agg0, MAX(price) AS __agg1 "
                + "FROM s.t GROUP BY k", ScanSql.select("s", "t", agg, null));
    }
}
