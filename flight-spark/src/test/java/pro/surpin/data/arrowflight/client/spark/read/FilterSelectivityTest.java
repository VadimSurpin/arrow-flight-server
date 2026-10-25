package pro.surpin.data.arrowflight.client.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.spark.read.FlightColumnStatistics.Column;

class FilterSelectivityTest {
    private static final Map<String, Column> STATS = Map.of(
            "qty", new Column(0.0, 100.0, 0, 50L),
            "name", new Column(null, null, 10, null));

    private static double of(Filter... filters) {
        return FilterSelectivity.of(filters, new Predicate[0], STATS, 1000);
    }

    @Test
    void noFiltersKeepEverything() {
        assertEquals(1.0, of());
    }

    @Test
    void patternMatchesAreAssumedToKeepAboutATenth() {
        assertEquals(0.1, of(new StringContains("name", "green")), 1e-9);
    }

    @Test
    void equalityKeepsOneDistinctValue() {
        assertEquals(1.0 / 50, of(new EqualTo("qty", 7)), 1e-9);
    }

    @Test
    void rangeInterpolatesBetweenTheBounds() {
        assertEquals(0.25, of(new GreaterThan("qty", 75)), 1e-9);
    }

    @Test
    void unknownColumnFallsBackToDefaults() {
        assertEquals(0.1, of(new EqualTo("other", 1)), 1e-9);
        assertEquals(1.0 / 3, of(new GreaterThan("other", 1)), 1e-9);
    }

    @Test
    void inListScalesWithItsSize() {
        assertEquals(3.0 / 50, of(new In("qty", new Object[] {1, 2, 3})), 1e-9);
    }

    @Test
    void nullChecksUseTheNullCount() {
        assertEquals(0.99, of(new IsNotNull("name")), 1e-9);
    }

    @Test
    void combinesAsIndependentConditions() {
        Filter like = new StringContains("name", "green");
        Filter range = new GreaterThan("qty", 75);
        assertEquals(0.025, of(new And(like, range)), 1e-9);
        assertEquals(0.325, of(new Or(like, range)), 1e-9);
        assertEquals(0.9, of(new Not(like)), 1e-9);
        assertEquals(0.025, of(like, range), 1e-9);
    }

    @Test
    void v2PredicatesAreEstimatedToo() {
        Expression[] children = {Expressions.column("qty"), Expressions.literal(75L)};
        Predicate greater = new Predicate(">", children);
        assertEquals(0.25, FilterSelectivity.of(new Filter[0], new Predicate[] {greater}, STATS, 1000), 1e-9);
    }

    @Test
    void neverDropsBelowOneRow() {
        assertEquals(1.0 / 1000, FilterSelectivity.of(new Filter[] {new EqualTo("qty", 1), new EqualTo("qty", 2),
                new EqualTo("qty", 3), new EqualTo("qty", 4)}, new Predicate[0], STATS, 1000), 1e-12);
    }

    @Test
    void namesTheColumnsItNeedsStatisticsFor() {
        assertEquals(java.util.Set.of("qty", "name"), FilterSelectivity.columns(
                new Filter[] {new And(new EqualTo("qty", 1), new StringContains("name", "x"))}, new Predicate[0]));
    }
}
