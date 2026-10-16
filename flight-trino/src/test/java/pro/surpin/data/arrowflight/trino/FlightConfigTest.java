package pro.surpin.data.arrowflight.trino;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.Configuration;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FlightConfigTest {

    private static Map<String, String> base() {
        Map<String, String> props = new HashMap<>();
        props.put("flight.host", "flight.local");
        props.put("flight.port", "32010");
        props.put("flight.bearer-token", "secret");
        return props;
    }

    @Test
    void parsesDefaults() {
        FlightConfig config = FlightConfig.fromProperties(base());
        assertEquals("flight.local", config.host());
        assertEquals(32010, config.port());
        assertFalse(config.tlsEnabled());
        assertTrue(config.verifyServer());
        assertEquals("", config.columnQuote());
        assertNull(config.maxRetries());
        assertNull(config.connectTimeoutMs());
    }

    @Test
    void buildsClientConfiguration() {
        Map<String, String> props = base();
        props.remove("flight.bearer-token");
        props.put("flight.user", "alice");
        props.put("flight.password", "pw");
        props.put("flight.tls.enabled", "TRUE");
        props.put("flight.tls.verify-server", "false");
        props.put("flight.column-quote", "\"");
        props.put("flight.max-retries", "5");
        props.put("flight.connect-timeout-ms", "1500");

        FlightConfig config = FlightConfig.fromProperties(props);
        Configuration client = config.toClientConfiguration();

        assertEquals("\"", config.columnQuote());
        assertEquals("flight.local", client.getFlightHost());
        assertEquals(32010, client.getFlightPort());
        assertTrue(client.getTlsEnabled());
        assertFalse(client.verifyServer());
        assertEquals("alice", client.getUser());
        assertEquals("pw", client.getPassword());
        assertNull(client.getBearerToken());
        assertEquals(5, client.getMaxRetries());
        assertEquals(1500L, client.getConnectTimeoutMs());
    }

    @Test
    void toStringHidesSecrets() {
        String text = FlightConfig.fromProperties(base()).toString();
        assertFalse(text.contains("secret"));
    }

    @Test
    void rejectsUnknownProperty() {
        Map<String, String> props = base();
        props.put("flight.hots", "typo");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> FlightConfig.fromProperties(props));
        assertTrue(e.getMessage().contains("flight.hots"));
    }

    @Test
    void rejectsMissingHost() {
        Map<String, String> props = base();
        props.put("flight.host", " ");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
    }

    @Test
    void rejectsBadPort() {
        Map<String, String> props = base();
        props.put("flight.port", "70000");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
        props.put("flight.port", "abc");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
    }

    @Test
    void requiresExactlyOneCredential() {
        Map<String, String> both = base();
        both.put("flight.user", "alice");
        both.put("flight.password", "pw");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(both));

        Map<String, String> none = base();
        none.remove("flight.bearer-token");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(none));
    }

    @Test
    void requiresUserWithPassword() {
        Map<String, String> props = base();
        props.remove("flight.bearer-token");
        props.put("flight.password", "pw");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
    }

    @Test
    void rejectsBadBooleanAndNumbers() {
        Map<String, String> tls = base();
        tls.put("flight.tls.enabled", "yes");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(tls));

        Map<String, String> retries = base();
        retries.put("flight.max-retries", "many");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(retries));

        Map<String, String> timeout = base();
        timeout.put("flight.connect-timeout-ms", "soon");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(timeout));
    }
}
