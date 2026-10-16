package pro.surpin.data.arrowflight.starrocks;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies a real StarRocks FE and BE are available to connector integration runs. */
@Tag("real-services")
class StarRocksFeBeIntegrationTest {
    @Test
    void executesQueryThroughFrontendAndSeesLiveBackend() throws Exception {
        String jdbcUrl = required("STARROCKS_JDBC_URL");
        String user = System.getenv().getOrDefault("STARROCKS_USER", "root");
        String password = System.getenv().getOrDefault("STARROCKS_PASSWORD", "");
        try (var connection = DriverManager.getConnection(jdbcUrl, user, password);
                var query = connection.createStatement()) {
            try (var result = query.executeQuery("SELECT 42")) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
            }
            try (var result = query.executeQuery("SHOW BACKENDS")) {
                assertTrue(result.next(), "StarRocks FE should have a registered BE");
                int aliveColumn = result.findColumn("Alive");
                do {
                    assertEquals("true", result.getString(aliveColumn).toLowerCase(),
                            "StarRocks backend should be alive");
                } while (result.next());
            }
        }
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Set " + name + " for the real-services test profile");
        }
        return value;
    }
}
