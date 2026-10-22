package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.services.TableStatisticsService;

/** A client asks a real Flight server for the statistics of a table through the custom action. */
@Tag("integration")
class TableStatisticsIntegrationTest {
    private static TestFlightServerHelper helper;

    @BeforeAll
    static void startAll() throws Exception {
        helper = TestFlightServerHelper.builder().start();
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    private static String ask(String request) {
        Iterator<Result> results = helper.flightClient().doAction(
                new Action(FlightSqlProducer.TABLE_STATISTICS_ACTION, request.getBytes(StandardCharsets.UTF_8)));
        List<String> bodies = new ArrayList<>();
        while (results.hasNext()) {
            bodies.add(new String(results.next().getBody(), StandardCharsets.UTF_8));
        }
        assertEquals(1, bodies.size());
        return bodies.get(0);
    }

    @Test
    void reportsRowCountAndColumnBounds() {
        TableStatisticsService.TableStatistics stats = TableStatisticsService.fromJson(ask(
                "{\"schema\":\"test_schema\",\"table\":\"test_table\",\"columns\":[\"bigint_col\",\"nope\"]}"));
        assertTrue(stats.rows() > 0);
        TableStatisticsService.ColumnStatistics column = stats.columns().get("bigint_col");
        assertNotNull(column);
        assertNotNull(column.min());
        assertNotNull(column.max());
        assertTrue(column.min() <= column.max());
        assertTrue(!stats.columns().containsKey("nope"));
    }

    @Test
    void rejectsUnknownTablesAndBadRequests() {
        assertThrows(FlightRuntimeException.class, () -> ask(
                "{\"schema\":\"test_schema\",\"table\":\"missing\",\"columns\":[\"id\"]}"));
        assertThrows(FlightRuntimeException.class, () -> ask("not json"));
        assertThrows(FlightRuntimeException.class, () -> ask(
                "{\"schema\":\"..\",\"table\":\"x\",\"columns\":[\"id\"]}"));
    }
}
