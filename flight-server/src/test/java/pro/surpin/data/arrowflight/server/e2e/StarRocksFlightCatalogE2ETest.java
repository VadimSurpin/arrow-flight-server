package pro.surpin.data.arrowflight.starrocks.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs StarRocks' JDBC catalog plugin against this repository's live Flight SQL service. */
@Tag("real-services")
class StarRocksFlightCatalogE2ETest {
    private static final String STARROCKS_IMAGE = System.getenv().getOrDefault(
            "STARROCKS_TEST_IMAGE", "registry.starrocks.io/starrocks/allin1-ubuntu:4.0.14");
    private static final String CATALOG = "flight_e2e";
    private static final String DRIVER_CLASS =
            "pro.surpin.data.arrowflight.starrocks.jdbc.postgresql.FlightSqlPostgresqlDriver";

    @Test
    void loadsFlightDriverDiscoversTableAndScansRows() throws Exception {
        Path driverJar = Path.of(requiredProperty("flight.starrocks.driver.jar")).toAbsolutePath();
        assertTrue(Files.isRegularFile(driverJar), "Build the StarRocks driver jar before this test");

        Path fixture = Path.of(requiredProperty("flight.server.test.data")).toAbsolutePath();
        try (TestFlightServerHelper flight = TestFlightServerHelper.builder()
                     .dataDir(fixture.toString()).start();
             GenericContainer<?> starrocks = startStarRocks(driverJar)) {
            try (Connection flightConnection = DriverManager.getConnection(
                    "jdbc:arrow-flight-sql://localhost:" + flight.server.getPort()
                            + "/?useEncryption=0");
                 var columns = flightConnection.getMetaData().getColumns(
                         null, "test_schema", "test_table", "%")) {
                assertTrue(columns.next(), "Flight SQL should expose table columns to JDBC clients");
            }

            Connection connection = awaitStarRocks();
            try (connection; var statement = connection.createStatement()) {
                statement.execute("CREATE EXTERNAL CATALOG " + CATALOG + " PROPERTIES ("
                        + "\"type\"=\"jdbc\","
                        + "\"user\"=\"\","
                        + "\"password\"=\"\","
                        + "\"jdbc_uri\"=\"jdbc:arrow-flight-sql://localhost:" + flight.server.getPort()
                        + "/?useEncryption=0\","
                        + "\"driver_url\"=\"file:///opt/flight-starrocks/driver.jar\","
                        + "\"driver_class\"=\"" + DRIVER_CLASS + "\")");

                assertTrue(showContains(statement, "SHOW DATABASES FROM " + CATALOG, "test_schema"),
                        "StarRocks should discover the Flight SQL schema");
                assertTrue(showContains(statement, "SHOW TABLES FROM " + CATALOG + ".test_schema", "test_table"),
                        "StarRocks should discover the Flight SQL table");

                try (var rows = statement.executeQuery(
                        "SELECT COUNT(*) FROM " + CATALOG + ".test_schema.test_table")) {
                    assertTrue(rows.next());
                    assertTrue(rows.getLong(1) > 0,
                            "StarRocks should read rows through its JDBC scan from Arrow Flight SQL");
                }
            } finally {
                dropCatalogIfPresent();
            }
        }
    }

    private static GenericContainer<?> startStarRocks(Path driverJar) {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(STARROCKS_IMAGE))
                .withNetworkMode("host")
                // StarRocks 4.0 loads JDBC driver classes in the FE JVM with Class.forName;
                // driver_url alone is used for the BE side and does not extend the FE classpath.
                .withFileSystemBind(driverJar.toString(),
                        "/data/deploy/starrocks/fe/lib/flight-starrocks-driver.jar", BindMode.READ_ONLY)
                .withFileSystemBind(driverJar.toString(), "/opt/flight-starrocks/driver.jar", BindMode.READ_ONLY)
                .waitingFor(new AbstractWaitStrategy() {
                    @Override
                    protected void waitUntilReady() {
                        // Readiness is checked with the StarRocks SQL protocol below.
                    }
                })
                .withStartupTimeout(Duration.ofMinutes(5));
        container.start();
        return container;
    }

    private static Connection awaitStarRocks() throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        SQLException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                Connection connection = DriverManager.getConnection(
                        "jdbc:mariadb://127.0.0.1:9030/", "root", "");
                try (var statement = connection.createStatement();
                     var result = statement.executeQuery("SELECT 1")) {
                    assertTrue(result.next());
                    return connection;
                } catch (Exception e) {
                    connection.close();
                    throw e;
                }
            } catch (SQLException e) {
                lastFailure = e;
                Thread.sleep(2_000);
            }
        }
        throw new IllegalStateException("StarRocks FE did not become ready on port 9030", lastFailure);
    }

    private static boolean showContains(java.sql.Statement statement, String sql, String expected)
            throws SQLException {
        try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
                if (expected.equalsIgnoreCase(result.getString(1))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void dropCatalogIfPresent() {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:mariadb://127.0.0.1:9030/", "root", "");
             var statement = connection.createStatement()) {
            statement.execute("DROP CATALOG IF EXISTS " + CATALOG);
        } catch (SQLException ignored) {
            // The container can fail during startup; its lifecycle still removes the catalog state.
        }
    }

    private static String requiredProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing Maven property " + key);
        }
        return value;
    }
}
