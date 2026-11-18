package pro.surpin.data.arrowflight.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ScanExprTest {
    private static ScanExpr col(String name) {
        return new ScanExpr.Column(name);
    }

    @Test
    void rendersNestedDecimalArithmetic() {
        ScanExpr one = new ScanExpr.Constant(new ScanPredicate.IntLit(1));
        ScanExpr charge = new ScanExpr.Arithmetic(ScanExpr.Op.MUL,
                new ScanExpr.Arithmetic(ScanExpr.Op.MUL, col("price"),
                        new ScanExpr.Arithmetic(ScanExpr.Op.SUB, one, col("disc"))),
                new ScanExpr.Arithmetic(ScanExpr.Op.ADD, one, col("tax")));
        assertEquals("((price * (1 - disc)) * (1 + tax))", charge.toSql());
    }

    @Test
    void rendersCastsAndCase() {
        ScanExpr urgent = new ScanExpr.Case(
                new ScanPredicate.Compare("prio", ScanPredicate.Op.EQ, new ScanPredicate.StringLit("1-URGENT")),
                new ScanExpr.Constant(new ScanPredicate.IntLit(1)),
                new ScanExpr.Constant(new ScanPredicate.IntLit(0)));
        assertEquals("CASE WHEN (prio = '1-URGENT') THEN 1 ELSE 0 END", urgent.toSql());
        assertEquals("CAST(x AS DOUBLE)", ScanExpr.Cast.toDouble(col("x")).toSql());
        assertEquals("CAST(x AS DECIMAL(38,6))", ScanExpr.Cast.toDecimal(col("x"), 38, 6).toSql());
        assertEquals("CAST(x AS BIGINT)", ScanExpr.Cast.toBigint(col("x")).toSql());
    }

    @Test
    void rendersDecimalAndDateConstants() {
        assertEquals("0.05", new ScanExpr.Constant(new ScanPredicate.DecimalLit(new BigDecimal("0.05"))).toSql());
    }

    @Test
    void rejectsUnsafeNamesAndBadDecimalTypes() {
        assertThrows(IllegalArgumentException.class, () -> col("a; b").toSql());
        assertThrows(IllegalArgumentException.class, () -> ScanExpr.Cast.toDecimal(col("x"), 39, 2));
        assertThrows(IllegalArgumentException.class, () -> ScanExpr.Cast.toDecimal(col("x"), 10, 11));
        assertThrows(IllegalArgumentException.class, () -> ScanExpr.Cast.toDecimal(col("x"), 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ScanExpr.Arithmetic(null, col("x"), col("y")));
        assertThrows(IllegalArgumentException.class,
                () -> new ScanExpr.Case(null, col("x"), col("y")).toSql());
    }

    @Test
    void collectsReferencedColumns() {
        ScanExpr expr = new ScanExpr.Case(
                new ScanPredicate.Compare("mode", ScanPredicate.Op.EQ, new ScanPredicate.StringLit("MAIL")),
                new ScanExpr.Arithmetic(ScanExpr.Op.MUL, col("price"), col("disc")), col("price"));
        assertEquals(List.of("mode", "price", "disc"), expr.columns());
    }
}
