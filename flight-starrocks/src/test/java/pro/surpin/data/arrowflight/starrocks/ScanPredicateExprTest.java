package pro.surpin.data.arrowflight.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ScanPredicateExprTest {
    private static ScanExpr col(String name) {
        return new ScanExpr.Column(name);
    }

    @Test
    void comparesTwoColumns() {
        ScanPredicate p = new ScanPredicate.ExprCompare(col("l_commitdate"), ScanPredicate.Op.LT, col("l_receiptdate"));
        assertEquals("(l_commitdate < l_receiptdate)", p.toSql());
        assertEquals(List.of("l_commitdate", "l_receiptdate"), ScanPredicate.columnsOf(p));
    }

    @Test
    void comparesAFunctionOfAColumnWithAConstant() {
        ScanPredicate p = new ScanPredicate.ExprCompare(ScanExpr.Call.year(col("l_shipdate")), ScanPredicate.Op.EQ,
                new ScanExpr.Constant(new ScanPredicate.IntLit(1995)));
        assertEquals("(year(l_shipdate) = 1995)", p.toSql());
        assertEquals(List.of("l_shipdate"), ScanPredicate.columnsOf(p));
    }

    @Test
    void rendersSubstringIn() {
        ScanPredicate p = new ScanPredicate.ExprIn(ScanExpr.Call.substring(col("c_phone"), 1, 2),
                List.of(new ScanPredicate.StringLit("13"), new ScanPredicate.StringLit("31")));
        assertEquals("(substring(c_phone, 1, 2) IN ('13', '31'))", p.toSql());
        assertEquals(List.of("c_phone"), ScanPredicate.columnsOf(p));
    }

    @Test
    void rendersLikeAndEscapesQuotes() {
        assertEquals("(p_type LIKE '%BRASS')", new ScanPredicate.Like(col("p_type"), "%BRASS").toSql());
        assertEquals("(c_name LIKE 'O''%')", new ScanPredicate.Like(col("c_name"), "O'%").toSql());
        assertEquals("(NOT (o_comment LIKE '%special%requests%'))",
                new ScanPredicate.Not(new ScanPredicate.Like(col("o_comment"), "%special%requests%")).toSql());
    }

    @Test
    void likePatternWithBackslashIsRejectedBecauseTheServerHasNoDefaultEscape() {
        assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.Like(col("c"), "a\\%b"));
    }

    @Test
    void functionsAreWhitelistedAndValidated() {
        assertEquals("substring(c_phone, 2, 5)", ScanExpr.Call.substring(col("c_phone"), 2, 5).toSql());
        assertEquals("month(d)", ScanExpr.Call.month(col("d")).toSql());
        assertEquals("day(d)", ScanExpr.Call.day(col("d")).toSql());
        assertThrows(IllegalArgumentException.class, () -> ScanExpr.Call.substring(col("c"), 0, 2));
        assertThrows(IllegalArgumentException.class, () -> ScanExpr.Call.substring(col("c"), 1, -1));
        assertThrows(IllegalArgumentException.class, () -> new ScanExpr.Call("system", List.of(col("c"))));
        assertThrows(IllegalArgumentException.class, () -> new ScanExpr.Call("year", List.of()));
    }

    @Test
    void expressionPredicatesNestInsideBooleanTerms() {
        ScanPredicate p = new ScanPredicate.And(List.of(
                new ScanPredicate.ExprCompare(col("a"), ScanPredicate.Op.LT, col("b")),
                new ScanPredicate.Compare("d", ScanPredicate.Op.GE, new ScanPredicate.DateLit(LocalDate.of(1994, 1, 1)))));
        assertEquals("((a < b) AND (d >= DATE '1994-01-01'))", p.toSql());
        assertEquals(List.of("a", "b", "d"), ScanPredicate.columnsOf(p));
    }
}
