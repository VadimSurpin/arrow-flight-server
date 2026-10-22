package pro.surpin.data.arrowflight.trino.config;

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
        assertNull(config.kerberosServicePrincipal());
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
    void buildsKerberosClientConfiguration() {
        Map<String, String> props = base();
        props.remove("flight.bearer-token");
        props.put("flight.tls.enabled", "true");
        props.put("flight.kerberos.service-principal", "flight/flight.local@EXAMPLE.COM");

        Configuration client = FlightConfig.fromProperties(props).toClientConfiguration();

        assertTrue(client.getTlsEnabled());
        assertNull(client.getPassword());
        assertNull(client.getBearerToken());
        assertEquals("flight/flight.local@EXAMPLE.COM", client.getKerberosServicePrincipal());
    }

    @Test
    void requiresTlsForKerberos() {
        Map<String, String> props = base();
        props.remove("flight.bearer-token");
        props.put("flight.kerberos.service-principal", "flight/flight.local@EXAMPLE.COM");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> FlightConfig.fromProperties(props));

        assertTrue(error.getMessage().contains("flight.tls.enabled=true"));
    }

    @Test
    void rejectsKerberosWithAnotherCredential() {
        Map<String, String> props = base();
        props.put("flight.tls.enabled", "true");
        props.put("flight.kerberos.service-principal", "flight/flight.local@EXAMPLE.COM");

        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
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

    /** Checks routing, schema and backoff propagation to the shared client. */
    @Test
    void passesRoutingAndBackoff() {
        Map<String, String> props = base();
        props.put("flight.routing.tag", "analytics");
        props.put("flight.routing.queue", "batch");
        props.put("flight.default-schema", "sales");
        props.put("flight.retry-backoff-ms", "250");
        Configuration config = FlightConfig.fromProperties(props).toClientConfiguration();
        assertEquals("analytics", config.getRoutingTag());
        assertEquals("batch", config.getRoutingQueue());
        assertEquals("sales", config.getDefaultSchema());
        assertEquals(250, config.getRetryBackoffMs());
    }

    /** Checks invalid retry values before any connection is opened. */
    @Test
    void rejectsNegativeRetrySettings() {
        for (String key : java.util.List.of("flight.max-retries", "flight.connect-timeout-ms", "flight.retry-backoff-ms")) {
            Map<String, String> props = base();
            props.put(key, "-1");
            assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
        }
    }

    /** Checks JKS TLS defaults, secret redaction and incompatible settings. */
    @Test
    void validatesTruststoreConfiguration() {
        Map<String, String> props = base();
        props.put("flight.tls.truststore.jks-file", "missing.jks");
        props.put("flight.tls.truststore.password", "private-password");
        FlightConfig config = FlightConfig.fromProperties(props);
        assertTrue(config.tlsEnabled());
        assertTrue(config.verifyServer());
        assertFalse(config.toString().contains("private-password"));
        assertThrows(IllegalArgumentException.class, config::toClientConfiguration);
        props.put("flight.tls.enabled", "false");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
        props.remove("flight.tls.enabled");
        props.put("flight.tls.verify-server", "false");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
        props.remove("flight.tls.truststore.jks-file");
        assertThrows(IllegalArgumentException.class, () -> FlightConfig.fromProperties(props));
    }

    /**
     * Checks loading a real trusted certificate from a JKS file.
     * @param directory temporary directory
     * @throws Exception if the fixture cannot be created
     */
    @Test
    void loadsJksCertificate(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        java.security.KeyStore system = java.security.KeyStore.getInstance("JKS");
        try (var input = java.nio.file.Files.newInputStream(java.nio.file.Path.of(System.getProperty("java.home"), "lib", "security", "cacerts"))) {
            system.load(input, "changeit".toCharArray());
        }
        java.security.cert.Certificate certificate = system.getCertificate(system.aliases().nextElement());
        java.security.KeyStore jks = java.security.KeyStore.getInstance("JKS");
        jks.load(null, null);
        jks.setCertificateEntry("server", certificate);
        java.nio.file.Path file = directory.resolve("trust.jks");
        try (var output = java.nio.file.Files.newOutputStream(file)) {
            jks.store(output, "password".toCharArray());
        }
        Map<String, String> props = base();
        props.put("flight.tls.truststore.jks-file", file.toString());
        props.put("flight.tls.truststore.password", "password");
        Configuration config = FlightConfig.fromProperties(props).toClientConfiguration();
        assertTrue(config.getTlsEnabled());
        assertEquals(file.toString(), config.getTruststoreJks());
        assertTrue(new String(config.getCertificateBytes(), java.nio.charset.StandardCharsets.UTF_8)
                .contains("BEGIN CERTIFICATE"));
    }
}
