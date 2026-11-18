package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies the complete Flight SQL path against a real Paimon warehouse.
 * The DuckDB paimon extension is taken from PAIMON_EXTENSION, the paimon.extension
 * system property, or the DuckDB community repository. Tests are skipped when the
 * extension cannot be loaded.
 */
@Tag("integration")
class PaimonServerIntegrationTest {

    private static TestFlightServerHelper helper;
    private static FlightSqlClient sqlClient;

    /** Creates a Paimon warehouse and starts the server in Paimon mode. */
    @BeforeAll
    static void startAll(@TempDir Path warehouse) throws Exception {
        String extension = extensionFile();
        createTable(warehouse, extension);
        helper = TestFlightServerHelper.builder()
                .dataDir(warehouse.toString())
                .tableFormat("paimon")
                .paimonExtensionPath(extension)
                .start();
        sqlClient = helper.sqlClient();
    }

    /** Stops the server. */
    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    /** Verifies projection and filtering through the Flight SQL client. */
    @Test
    void selectsWithFilter() throws Exception {
        FlightInfo info = sqlClient.execute(
                "SELECT id, price FROM testdb.orders WHERE price >= 200 ORDER BY id");

        assertEquals(List.of("id", "price"), fieldNames(info));
        List<List<Object>> rows = readRows(info);
        assertEquals(2, rows.size());
        assertEquals(2L, ((Number) rows.get(0).get(0)).longValue());
        assertEquals(200L, ((Number) rows.get(0).get(1)).longValue());
        assertEquals(3L, ((Number) rows.get(1).get(0)).longValue());
    }

    /** Verifies the advertised schema follows the projection order. */
    @Test
    void keepsProjectionOrder() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT price, id FROM testdb.orders ORDER BY id");

        assertEquals(List.of("price", "id"), fieldNames(info));
        List<List<Object>> rows = readRows(info);
        assertEquals(3, rows.size());
        assertEquals(100L, ((Number) rows.get(0).get(0)).longValue());
        assertEquals(1L, ((Number) rows.get(0).get(1)).longValue());
    }

    /** Verifies aggregation is executed over the Paimon snapshot. */
    @Test
    void countsRows() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT count(*) FROM testdb.orders");

        List<List<Object>> rows = readRows(info);
        assertEquals(1, rows.size());
        assertEquals(3L, ((Number) rows.get(0).get(0)).longValue());
    }

    /** Verifies an empty Paimon table streams the declared schema with no rows. */
    @Test
    void readsEmptyTable() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT * FROM testdb.empty_orders");

        assertEquals(List.of("id"), fieldNames(info));
        assertEquals(List.of(), readRows(info));
    }

    /** Verifies a missing table is reported as not found. */
    @Test
    void reportsMissingTable() {
        FlightRuntimeException error = assertThrows(FlightRuntimeException.class,
                () -> sqlClient.execute("SELECT * FROM testdb.missing"));
        assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
    }

    private static List<String> fieldNames(FlightInfo info) {
        return info.getSchema().getFields().stream().map(field -> field.getName()).toList();
    }

    private static List<List<Object>> readRows(FlightInfo info) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int row = 0; row < root.getRowCount(); row++) {
                        List<Object> values = new ArrayList<>(root.getFieldVectors().size());
                        for (FieldVector vector : root.getFieldVectors()) {
                            values.add(vector.isNull(row) ? null : vector.getObject(row));
                        }
                        rows.add(values);
                    }
                }
            }
        }
        return rows;
    }

    private static void createTable(Path warehouse, String extension) throws Exception {
        Properties props = new Properties();
        if (extension != null) {
            props.setProperty("allow_unsigned_extensions", "true");
        }
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", props);
                Statement statement = connection.createStatement()) {
            loadPaimon(statement, extension);
            statement.execute("ATTACH '" + warehouse + "' AS cat (TYPE paimon)");
            statement.execute("CREATE SCHEMA cat.testdb");
            statement.execute("CREATE TABLE cat.testdb.orders (id BIGINT, price BIGINT)");
            statement.execute("INSERT INTO cat.testdb.orders"
                    + " VALUES (1, 100), (2, 200), (3, 300)");
            statement.execute("CREATE TABLE cat.testdb.empty_orders (id BIGINT)");
        }
    }

    private static void loadPaimon(Statement statement, String extension) throws SQLException {
        if (extension != null) {
            statement.execute("LOAD '" + extension + "'");
            return;
        }
        try {
            statement.execute("INSTALL paimon FROM community");
            statement.execute("LOAD paimon");
        } catch (SQLException e) {
            assumeTrue(false, "Paimon extension is unavailable: " + e.getMessage());
        }
    }

    private static String extensionFile() {
        String configured = System.getProperty("paimon.extension");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("PAIMON_EXTENSION");
        }
        if (configured == null || configured.isBlank()) {
            return null;
        }
        assumeTrue(Files.isRegularFile(Path.of(configured)),
                "Paimon extension file is missing: " + configured);
        return configured;
    }
}
