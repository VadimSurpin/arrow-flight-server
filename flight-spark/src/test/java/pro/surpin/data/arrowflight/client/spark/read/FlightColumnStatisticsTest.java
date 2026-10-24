package pro.surpin.data.arrowflight.client.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.junit.jupiter.api.Test;

class FlightColumnStatisticsTest {

    @Test
    void reportsDistinctValuesAndNulls() throws Exception {
        String json = "{\"rows\":600,\"columns\":{"
                + "\"l_orderkey\":{\"min\":1.0,\"max\":9.0,\"nulls\":0,\"bytes\":10,\"ndv\":150,\"extra\":1},"
                + "\"l_comment\":{\"nulls\":-1,\"bytes\":10,\"ndv\":null}}}";
        Map<NamedReference, ColumnStatistics> stats = FlightColumnStatistics.convert(FlightColumnStatistics.parse(json));
        ColumnStatistics key = stats.get(Expressions.column("l_orderkey"));
        assertEquals(150, key.distinctCount().getAsLong());
        assertEquals(0, key.nullCount().getAsLong());
        assertFalse(stats.containsKey(Expressions.column("l_comment")), "a column with nothing known is left out");
    }

    @Test
    void pendingDistinctCountKeepsTheNullCount() throws Exception {
        String json = "{\"rows\":5,\"columns\":{\"c\":{\"nulls\":2,\"bytes\":1,\"ndv\":null}}}";
        ColumnStatistics c = FlightColumnStatistics.convert(FlightColumnStatistics.parse(json))
                .get(Expressions.column("c"));
        assertTrue(c.distinctCount().isEmpty());
        assertEquals(2, c.nullCount().getAsLong());
    }
}
