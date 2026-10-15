package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.expr.Expr;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class SparkPushdownTest {

    private final SqlRenderer renderer = new SqlRenderer("\"");

    private String sql(Filter filter) {
        return SparkPushdown.translate(filter).map(renderer::render).orElseThrow();
    }

    @Test
    void v1LocalDateTimeUsesSqlTimestampFormat() {
        assertEquals("\"ts\" > '2024-01-02 03:04:05'",
                sql(new GreaterThan("ts", LocalDateTime.of(2024, 1, 2, 3, 4, 5))));
    }

    @Test
    void v1BigDecimalRendersPlain() {
        assertEquals("\"x\" = 1000", sql(new EqualTo("x", new BigDecimal("1E+3"))));
    }

    @Test
    void v1EmptyInIsAlwaysFalse() {
        assertEquals(new Expr.Constant(false),
                SparkPushdown.translate(new In("x", new Object[0])).orElseThrow());
    }

    @Test
    void v1UnsupportedValueIsNotPushed() {
        assertTrue(SparkPushdown.translate(new EqualTo("x", new Object())).isEmpty());
        assertTrue(SparkPushdown.translate((Filter) null).isEmpty());
    }
}
