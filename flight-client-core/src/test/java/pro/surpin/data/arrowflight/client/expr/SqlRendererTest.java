package pro.surpin.data.arrowflight.client.expr;

import pro.surpin.data.arrowflight.client.query.PushAggregation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class SqlRendererTest {

    private final SqlRenderer renderer = new SqlRenderer("\"");
    private final Expr.Column col = new Expr.Column("a");

    @Test
    void quotesAndEscapesIdentifiers() {
        assertEquals("\"we\"\"ird\"", renderer.render(new Expr.Column("we\"ird")));
        assertEquals("a", new SqlRenderer("").render(col));
        assertEquals("a", new SqlRenderer(null).render(col));
    }

    @Test
    void rendersLiterals() {
        assertEquals("null", renderer.render(Expr.literal(null)));
        assertEquals("42", renderer.render(Expr.literal(42)));
        assertEquals("true", renderer.render(Expr.literal(true)));
        assertEquals("'it''s'", renderer.render(Expr.literal("it's")));
        assertEquals("'2024-02-03'", renderer.render(Expr.literal(LocalDate.of(2024, 2, 3))));
        assertEquals("'2024-02-03 04:05:06'",
                renderer.render(Expr.literal(LocalDateTime.of(2024, 2, 3, 4, 5, 6))));
    }

    @Test
    void rendersBigDecimalWithoutExponent() {
        assertEquals("1000", renderer.render(Expr.literal(new BigDecimal("1E+3"))));
    }

    @Test
    void rejectsUnsupportedLiterals() {
        assertTrue(Expr.Literal.of(Double.NaN).isEmpty());
        assertTrue(Expr.Literal.of(Float.POSITIVE_INFINITY).isEmpty());
        assertTrue(Expr.Literal.of(new java.util.Date(0)).isEmpty());
        assertTrue(Expr.Literal.of(new Object()).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> Expr.literal(new Object()));
    }

    @Test
    void rendersComparisonsAndLogic() {
        Expr eq = new Expr.Compare(Expr.CompareOp.EQ, col, Expr.literal(1));
        Expr gt = new Expr.Compare(Expr.CompareOp.GT, col, Expr.literal(2));
        assertEquals("(\"a\" = 1 and \"a\" > 2)", renderer.render(new Expr.And(eq, gt)));
        assertEquals("(\"a\" = 1 or \"a\" > 2)", renderer.render(new Expr.Or(eq, gt)));
        assertEquals("not (\"a\" = 1)", renderer.render(new Expr.Not(eq)));
    }

    @Test
    void expandsNullSafeEquality() {
        Expr e = new Expr.Compare(Expr.CompareOp.NULL_SAFE_EQ, col, Expr.literal(1));
        assertEquals("((\"a\" is not null and 1 is not null and \"a\" = 1) or (\"a\" is null and 1 is null))",
                renderer.render(e));
    }

    @Test
    void rendersNullChecksInAndConstants() {
        assertEquals("\"a\" is null", renderer.render(new Expr.IsNull(col, false)));
        assertEquals("\"a\" is not null", renderer.render(new Expr.IsNull(col, true)));
        assertEquals("\"a\" in (1,null)",
                renderer.render(new Expr.In(col, List.of(Expr.literal(1), Expr.literal(null)))));
        assertEquals("(1 = 1)", renderer.render(new Expr.Constant(true)));
        assertEquals("(1 = 0)", renderer.render(new Expr.Constant(false)));
    }

    @Test
    void escapesLikeWildcards() {
        assertEquals("\"a\" like '50\\%\\_x%' escape '\\'",
                renderer.render(new Expr.Like(col, Expr.LikeKind.STARTS_WITH, "50%_x")));
        assertEquals("\"a\" like '%o''k' escape '\\'",
                renderer.render(new Expr.Like(col, Expr.LikeKind.ENDS_WITH, "o'k")));
        assertEquals("\"a\" like '%\\\\%' escape '\\'",
                renderer.render(new Expr.Like(col, Expr.LikeKind.CONTAINS, "\\")));
    }

    @Test
    void rendersArithmeticAndCasts() {
        Expr product = new Expr.Arithmetic("*", col, Expr.literal(new BigDecimal("0.5")));
        assertEquals("cast((\"a\" * 0.5) as decimal(38,2))",
                renderer.render(Expr.castDecimal(product, 38, 2)));
        assertThrows(IllegalArgumentException.class, () -> new Expr.Arithmetic("/", col, col));
    }

    @Test
    void rendersAggregationWithGroupByFirst() {
        Aggregation agg = new Aggregation(List.of(new Expr.Column("g")), List.of(
                new Aggregation.Call(Aggregation.Function.COUNT_STAR, null),
                new Aggregation.Call(Aggregation.Function.SUM, col)));
        PushAggregation pushed = renderer.render(agg);
        assertArrayEquals(new String[] {"\"g\"", "count(*)", "sum(\"a\")"}, pushed.getColumnExpressions());
        assertArrayEquals(new String[] {"\"g\""}, pushed.getGroupByColumns());
    }

    @Test
    void rendersGlobalAggregationWithoutGroupBy() {
        Aggregation agg = new Aggregation(List.of(), List.of(
                new Aggregation.Call(Aggregation.Function.MIN, col),
                new Aggregation.Call(Aggregation.Function.MAX, col),
                new Aggregation.Call(Aggregation.Function.COUNT, col)));
        PushAggregation pushed = renderer.render(agg);
        assertArrayEquals(new String[] {"min(\"a\")", "max(\"a\")", "count(\"a\")"}, pushed.getColumnExpressions());
        assertNull(pushed.getGroupByColumns());
    }

    @Test
    void aggregationValidatesArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new Aggregation.Call(Aggregation.Function.COUNT_STAR, col));
        assertThrows(IllegalArgumentException.class,
                () -> new Aggregation.Call(Aggregation.Function.SUM, null));
        assertTrue(new Aggregation(List.of(), List.of()).isEmpty());
    }

    @Test
    void findsCompareOpBySymbol() {
        assertEquals(Expr.CompareOp.LE, Expr.CompareOp.fromSymbol("<=").orElseThrow());
        assertTrue(Expr.CompareOp.fromSymbol("~").isEmpty());
    }
}
