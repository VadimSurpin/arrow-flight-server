package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@Tag("smoke")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightServerIntegrationTest {

    private static TestFlightServerHelper helper;

    @BeforeAll
    static void startServer() throws Exception {
        helper = TestFlightServerHelper.builder().start();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (helper != null) helper.close();
    }

    @Test
    @Order(1)
    void serverIsReachable() {
        assertNotNull(helper.server);
        assertTrue(helper.server.getPort() > 0);
    }

    @Test
    @Order(2)
    void getCatalogsReturnsOneRow() throws Exception {
        FlightInfo info = helper.sqlClient().getCatalogs();
        List<Integer> rowCounts = collectRowCounts(info);
        int total = rowCounts.stream().mapToInt(Integer::intValue).sum();
        assertEquals(1, total);
    }

    @Test
    @Order(3)
    void getSchemasReturnsTestSchema() throws Exception {
        FlightInfo info = helper.sqlClient().getSchemas(null, null);
        List<String> allStrings = collectAllStrings(info);
        assertTrue(allStrings.contains("test_schema"),
                "Schema list should contain 'test_schema', got: " + allStrings);
    }

    @Test
    @Order(4)
    void getTableTypesReturnsTABLE() throws Exception {
        FlightInfo info = helper.sqlClient().getTableTypes();
        List<String> types = collectFirstColumnStrings(info);
        assertTrue(types.contains("TABLE"));
    }

    @Test
    @Order(5)
    void getTablesReturnsTestTable() throws Exception {
        FlightInfo info = helper.sqlClient().getTables(null, null, null, null, false);
        List<String> allStrings = collectAllStrings(info);
        assertTrue(allStrings.contains("test_table"),
                "Tables list should contain 'test_table', got: " + allStrings);
    }

    @Test
    @Order(6)
    void executeSelectStarReturnsAllRows() throws Exception {
        FlightInfo info = helper.sqlClient().execute("SELECT * FROM test_schema.test_table");
        assertNotNull(info.getSchema());
        assertFalse(info.getSchema().getFields().isEmpty());

        int totalRows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        assertTrue(totalRows > 0);
    }

    @Test
    @Order(7)
    void executeWithFilterReducesRowCount() throws Exception {
        int allRows = countRows(helper.sqlClient().execute(
                "SELECT * FROM test_schema.test_table"));
        int filtered = countRows(helper.sqlClient().execute(
                "SELECT * FROM test_schema.test_table WHERE tinyint_col = 0"));

        assertTrue(filtered > 0);
        assertTrue(filtered < allRows,
                "Filtered result (" + filtered + ") should be less than full scan (" + allRows + ")");
    }

    @Test
    @Order(8)
    void executeWithColumnProjection() throws Exception {
        FlightInfo info = helper.sqlClient().execute(
                "SELECT id, bool_col FROM test_schema.test_table");
        assertEquals(2, info.getSchema().getFields().size());
    }

    @Test
    @Order(9)
    void executeWithNonexistentTableThrows() {
        RuntimeException e = assertThrows(RuntimeException.class, () ->
                helper.sqlClient().execute("SELECT * FROM test_schema.nonexistent"));
        assertTrue(e.getMessage().contains("Could not find"),
                "Must return not-found error, got: " + e.getMessage());
    }

    @Test
    @Order(10)
    void getTablesWithInvalidFilterReturnsEmpty() throws Exception {
        FlightInfo info = helper.sqlClient().getTables(null, null, "zzz_nonexistent_table_xyz", null, false);
        assertNotNull(info);
    }

    private int countRows(FlightInfo info) throws Exception {
        int total = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    total += stream.getRoot().getRowCount();
                }
            }
        }
        return total;
    }

    private List<Integer> collectRowCounts(FlightInfo info) throws Exception {
        List<Integer> counts = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    counts.add(stream.getRoot().getRowCount());
                }
            }
        }
        return counts;
    }

    private List<String> collectFirstColumnStrings(FlightInfo info) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    if (root.getSchema().getFields().isEmpty()) continue;
                    var vec = root.getVector(0);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        Object val = vec.getObject(i);
                        if (val != null) values.add(val.toString());
                    }
                }
            }
        }
        return values;
    }

    private List<String> collectAllStrings(FlightInfo info) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int col = 0; col < root.getSchema().getFields().size(); col++) {
                        var vec = root.getVector(col);
                        for (int row = 0; row < root.getRowCount(); row++) {
                            Object val = vec.getObject(row);
                            if (val != null) values.add(val.toString());
                        }
                    }
                }
            }
        }
        return values;
    }
}
