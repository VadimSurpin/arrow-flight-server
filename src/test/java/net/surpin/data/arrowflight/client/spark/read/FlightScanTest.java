package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
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
}
