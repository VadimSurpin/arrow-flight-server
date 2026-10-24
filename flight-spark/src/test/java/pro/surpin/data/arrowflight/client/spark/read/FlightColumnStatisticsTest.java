package pro.surpin.data.arrowflight.client.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class FlightColumnStatisticsTest {

    private static final StructType SCHEMA = new StructType()
            .add("l_orderkey", DataTypes.LongType)
            .add("l_shipdate", DataTypes.DateType)
            .add("l_discount", DataTypes.createDecimalType(15, 2))
            .add("l_comment", DataTypes.StringType);

    @Test
    void reportsCompleteStatisticsInTheColumnType() throws Exception {
        String json = "{\"rows\":600,\"columns\":{"
                + "\"l_orderkey\":{\"min\":1.0,\"max\":9.0,\"nulls\":0,\"bytes\":10,\"ndv\":150,\"extra\":1},"
                + "\"l_shipdate\":{\"min\":8036.0,\"max\":10561.0,\"nulls\":0,\"bytes\":10,\"ndv\":2500},"
                + "\"l_discount\":{\"min\":0.0,\"max\":0.1,\"nulls\":3,\"bytes\":10,\"ndv\":11}}}";
        Map<NamedReference, ColumnStatistics> stats = FlightColumnStatistics.convert(
                FlightColumnStatistics.parse(json), SCHEMA);
        ColumnStatistics key = stats.get(Expressions.column("l_orderkey"));
        assertEquals(150, key.distinctCount().getAsLong());
        assertEquals(1L, key.min().orElseThrow());
        assertEquals(9L, key.max().orElseThrow());
        assertEquals(LocalDate.ofEpochDay(8036), stats.get(Expressions.column("l_shipdate")).min().orElseThrow());
        assertEquals(new BigDecimal("0.1"), stats.get(Expressions.column("l_discount")).max().orElseThrow());
    }

    @Test
    void leavesOutColumnsWithAnyBoundMissing() throws Exception {
        // Spark reads a missing bound as an empty range and estimates every join on the column to return no rows.
        String json = "{\"rows\":5,\"columns\":{"
                + "\"l_comment\":{\"nulls\":2,\"bytes\":1,\"ndv\":4},"
                + "\"l_orderkey\":{\"min\":1.0,\"max\":9.0,\"nulls\":0,\"bytes\":1,\"ndv\":null},"
                + "\"l_shipdate\":{\"nulls\":0,\"bytes\":1,\"ndv\":7}}}";
        assertTrue(FlightColumnStatistics.convert(FlightColumnStatistics.parse(json), SCHEMA).isEmpty());
    }
}
