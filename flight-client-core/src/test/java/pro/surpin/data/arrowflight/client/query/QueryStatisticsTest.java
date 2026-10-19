package pro.surpin.data.arrowflight.client.query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the query statistics action representation. */
@Tag("unit")
class QueryStatisticsTest {
    @Test
    void roundTripsBinaryRepresentation() {
        QueryStatistics statistics = new QueryStatistics(4096L, 125L);
        assertEquals(statistics, QueryStatistics.decode(statistics.encode()));
    }

    @Test
    void rejectsInvalidBinaryRepresentation() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryStatistics.decode(new byte[1]));
    }
}
