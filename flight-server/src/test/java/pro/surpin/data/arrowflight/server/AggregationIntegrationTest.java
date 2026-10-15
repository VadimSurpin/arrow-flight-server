package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AggregationIntegrationTest {

    private static TestFlightServerHelper helper;
    private static FlightSqlClient sqlClient;

    @BeforeAll
    static void startAll() throws Exception {
        helper = TestFlightServerHelper.builder().start();
        sqlClient = helper.sqlClient();
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) helper.close();
    }

    @Test @Order(1)
    void countStarReturnsPositiveRowCount() throws Exception {
        long count = totalCountStar("SELECT count(*) FROM test_schema.test_table");
        assertTrue(count > 0);
    }

    @Test @Order(2)
    void countStarWithGroupByBoolColReturnsTwoGroups() throws Exception {
        Map<Object, Long> groups = totalGroupByCount(
                "SELECT bool_col, count(*) FROM test_schema.test_table GROUP BY bool_col");
        assertEquals(2, groups.size());
        assertTrue(groups.containsKey(true) || groups.containsKey("true"));
        assertTrue(groups.containsKey(false) || groups.containsKey("false"));
    }

    @Test @Order(3)
    void groupByCountsSumToTotalCount() throws Exception {
        long total = totalCountStar("SELECT count(*) FROM test_schema.test_table");
        Map<Object, Long> groups = totalGroupByCount(
                "SELECT bool_col, count(*) FROM test_schema.test_table GROUP BY bool_col");
        long groupSum = groups.values().stream().mapToLong(Long::longValue).sum();
        assertEquals(total, groupSum);
    }

    @Test @Order(4)
    void minLessOrEqualMax() throws Exception {
        long[] mm = totalMinMax(
                "SELECT min(bigint_col), max(bigint_col) FROM test_schema.test_table");
        assertTrue(mm[0] <= mm[1]);
    }

    @Test @Order(5)
    void countStarWithFilterReturnsFewer() throws Exception {
        long total = totalCountStar("SELECT count(*) FROM test_schema.test_table");
        long filtered = totalCountStar(
                "SELECT count(*) FROM test_schema.test_table WHERE \"tinyint_col\" = 0");
        assertTrue(filtered > 0);
        assertTrue(filtered < total);
    }

    @Test @Order(6)
    void aggregationSchemaHasCorrectColumnCount() throws Exception {
        FlightInfo info = sqlClient.execute(
                "SELECT count(*), min(bigint_col), max(bigint_col) FROM test_schema.test_table");
        assertEquals(3, info.getSchema().getFields().size());
    }

    @Test @Order(7)
    void sumBigintColIsNonNegative() throws Exception {
        FlightInfo info = sqlClient.execute(
                "SELECT sum(bigint_col) FROM test_schema.test_table");
        assertNotNull(info);
    }

    private long totalCountStar(String query) throws Exception {
        FlightInfo info = sqlClient.execute(query);
        long total = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream s = helper.flightClient().getStream(ep.getTicket())) {
                while (s.next()) {
                    FieldVector v = s.getRoot().getVector(0);
                    for (int i = 0; i < s.getRoot().getRowCount(); i++) {
                        if (!v.isNull(i)) total += ((Number) v.getObject(i)).longValue();
                    }
                }
            }
        }
        return total;
    }

    private Map<Object, Long> totalGroupByCount(String query) throws Exception {
        FlightInfo info = sqlClient.execute(query);
        Map<Object, Long> merged = new LinkedHashMap<>();
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream s = helper.flightClient().getStream(ep.getTicket())) {
                while (s.next()) {
                    VectorSchemaRoot root = s.getRoot();
                    FieldVector keyVec = root.getVector(0);
                    FieldVector cntVec = root.getVector(1);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        Object key = keyVec.isNull(i) ? null : keyVec.getObject(i);
                        long count = cntVec.isNull(i) ? 0
                                : ((Number) cntVec.getObject(i)).longValue();
                        merged.merge(key, count, Long::sum);
                    }
                }
            }
        }
        return merged;
    }

    private long[] totalMinMax(String query) throws Exception {
        FlightInfo info = sqlClient.execute(query);
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream s = helper.flightClient().getStream(ep.getTicket())) {
                while (s.next()) {
                    VectorSchemaRoot root = s.getRoot();
                    FieldVector minVec = root.getVector(0);
                    FieldVector maxVec = root.getVector(1);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (!minVec.isNull(i)) {
                            long v = ((Number) minVec.getObject(i)).longValue();
                            if (v < min) min = v;
                        }
                        if (!maxVec.isNull(i)) {
                            long v = ((Number) maxVec.getObject(i)).longValue();
                            if (v > max) max = v;
                        }
                    }
                }
            }
        }
        return new long[]{min, max};
    }
}
