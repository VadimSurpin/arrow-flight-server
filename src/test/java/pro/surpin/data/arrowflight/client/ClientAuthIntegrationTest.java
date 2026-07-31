package pro.surpin.data.arrowflight.client;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ClientAuthIntegrationTest {

    private static TestFlightServerHelper helper;
    private static String host;
    private static int port;

    @BeforeAll
    static void startServer() throws Exception {
        helper = TestFlightServerHelper.builder().start();
        host = "localhost";
        port = helper.location.getUri().getPort();
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) helper.close();
    }

    // ── regression: bearer token + routing headers ─────────────────────────

    @Test
    void bearerTokenWithRoutingHeadersDoesNotThrowClassCastException() {
        Configuration config = new Configuration(host, port, "test-user", null, "test-token");
        config.setDefaultSchema("test_schema");
        config.setRoutingTag("batch");
        config.setRoutingQueue("high");

        Client client = assertDoesNotThrow(
                () -> Client.getOrCreate(config),
                "Bearer-token auth with routing headers must not throw ClassCastException");

        assertNotNull(client);

        long rows = client.execute("SELECT count(*) FROM test_schema.test_table");
        assertTrue(rows > 0, "Query must return rows: got " + rows);
    }

    // ── sanity: other modes still work ─────────────────────────────────────

    @Test
    void bearerTokenWithoutRoutingHeadersStillWorks() {
        Configuration config = new Configuration(host, port, "test-user", null, "test-token-2");

        Client client = assertDoesNotThrow(
                () -> Client.getOrCreate(config),
                "Plain bearer-token auth must still work");

        assertNotNull(client);
        long rows = client.execute("SELECT count(*) FROM test_schema.test_table");
        assertTrue(rows > 0);
    }

    @Test
    void passwordAuthStillWorks() {
        Configuration config = new Configuration(host, port, "test-user", "test-pass", null);

        Client client = assertDoesNotThrow(
                () -> Client.getOrCreate(config),
                "Password-based auth must still work");

        assertNotNull(client);
        long rows = client.execute("SELECT count(*) FROM test_schema.test_table");
        assertTrue(rows > 0);
    }

    @Test
    void queryNonExistentTableReturnsError() {
        Configuration config = new Configuration(host, port, "test-user", null, "test-token-non-existent-table");

        Client client = assertDoesNotThrow(() -> Client.getOrCreate(config));

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> client.getQueryEndpoints("SELECT * FROM test_schema.nonexistent_xyz"));
        assertTrue(e.getMessage().contains("Could not find"),
                "Non-existent table must return not-found, got: " + e.getMessage());
    }
}
