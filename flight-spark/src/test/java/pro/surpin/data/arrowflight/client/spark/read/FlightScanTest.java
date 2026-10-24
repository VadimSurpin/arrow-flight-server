package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.read.Statistics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests Spark statistics reported by Flight scans.
 */
class FlightScanTest {

    /** Verifies byte and row estimates are exposed to Spark's optimizer. */
    @Test
    void reportsFlightInfoStatistics() {
        Table table = mock(Table.class);
        when(table.getEstimatedBytes()).thenReturn(2048L);
        when(table.getEstimatedRows()).thenReturn(64L);
        FlightScan scan = new FlightScan(mock(Configuration.class), table);

        Statistics statistics = scan.estimateStatistics();

        assertEquals(2048L, statistics.sizeInBytes().orElseThrow());
        assertEquals(64L, statistics.numRows().orElseThrow());
    }

    /** Verifies absent server estimates remain unknown instead of becoming zero. */
    @Test
    void keepsMissingStatisticsUnknown() {
        Table table = mock(Table.class);
        when(table.getEstimatedBytes()).thenReturn(-1L);
        when(table.getEstimatedRows()).thenReturn(-1L);
        FlightScan scan = new FlightScan(mock(Configuration.class), table);

        Statistics statistics = scan.estimateStatistics();

        assertTrue(statistics.sizeInBytes().isEmpty());
        assertTrue(statistics.numRows().isEmpty());
    }

    private static FlightScan keyedScan(java.util.List<java.util.List<String>> seen, boolean aggregated) {
        Table table = Table.forTable("sf100.orders", "\"");
        table.setSparkSchema(new org.apache.spark.sql.types.StructType()
                .add("o_orderkey", org.apache.spark.sql.types.DataTypes.LongType)
                .add("o_comment", org.apache.spark.sql.types.DataTypes.StringType));
        return new FlightScan(mock(Configuration.class), table, aggregated, seen::add);
    }

    private static org.apache.spark.sql.connector.expressions.filter.Predicate in(String column, long... keys) {
        org.apache.spark.sql.connector.expressions.Expression[] children =
                new org.apache.spark.sql.connector.expressions.Expression[keys.length + 1];
        children[0] = org.apache.spark.sql.connector.expressions.Expressions.column(column);
        for (int i = 0; i < keys.length; i++) {
            children[i + 1] = org.apache.spark.sql.connector.expressions.Expressions.literal(keys[i]);
        }
        return new org.apache.spark.sql.connector.expressions.filter.Predicate("IN", children);
    }

    /** Verifies only integer key columns are offered for runtime pruning. */
    @Test
    void offersIntegerKeysForRuntimeFiltering() {
        FlightScan scan = keyedScan(new java.util.ArrayList<>(), false);

        assertEquals(1, scan.filterAttributes().length);
        assertEquals("o_orderkey", scan.filterAttributes()[0].fieldNames()[0]);
        assertEquals(0, keyedScan(new java.util.ArrayList<>(), true).filterAttributes().length,
                "an aggregated scan has no table columns to prune");
    }

    /** Verifies a runtime IN filter is rendered and handed to the replanner. */
    @Test
    void replansWithRuntimeInFilter() {
        java.util.List<java.util.List<String>> seen = new java.util.ArrayList<>();
        keyedScan(seen, false).filter(new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
            in("o_orderkey", 3L, 7L)});

        assertEquals(1, seen.size());
        assertTrue(seen.get(0).get(0).contains("3") && seen.get(0).get(0).contains("7"), seen.get(0).get(0));
    }

    /** Verifies a filter with too many keys is ignored. */
    @Test
    void ignoresHugeRuntimeFilters() {
        java.util.List<java.util.List<String>> seen = new java.util.ArrayList<>();
        long[] keys = java.util.stream.LongStream.range(0, 100_001).toArray();
        keyedScan(seen, false).filter(new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
            in("o_orderkey", keys)});

        assertTrue(seen.isEmpty());
    }
}
