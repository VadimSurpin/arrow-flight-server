package pro.surpin.data.arrowflight.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClientRetryTest {

    @Test
    void toStringRedactsPassword() {
        Configuration config = new Configuration("localhost", 32010, "user", "my-secret-pass", null);
        String str = config.toString();

        assertFalse(str.contains("my-secret-pass"), "toString must redact password");
        assertTrue(str.contains("password='[hidden]'"), "toString must show redacted password");
    }

    @Test
    void toStringRedactsBearerToken() {
        Configuration config = new Configuration("localhost", 32010, "user", null, "my-access-token");
        String str = config.toString();

        assertFalse(str.contains("my-access-token"), "toString must redact bearer token");
        assertTrue(str.contains("bearerToken='[hidden]'"), "toString must show redacted bearer token");
    }

    @Test
    void toStringRedactsTruststorePassword() {
        Configuration config = new Configuration("localhost", 32010, "/path/to/trust.jks", "trust-pass", "user", "pass", null);
        String str = config.toString();

        assertFalse(str.contains("trust-pass"), "toString must redact truststore password");
        assertTrue(str.contains("trustStorePass='[hidden]'"), "toString must show redacted truststore pass");
    }

    @Test
    void toStringDoesNotLeakCertBytes() {
        Configuration config = new Configuration("localhost", 32010, "/path/to/trust.jks", "trust-pass", "user", "pass", null);
        String str = config.toString();

        assertTrue(str.contains("certBytes='[hidden"), "toString must redact cert bytes");
        assertFalse(str.contains("certBytes=[") && !str.contains("[hidden"), "toString must not leak raw bytes");
    }

    @Test
    void toStringShowsNullFieldsAsNotSet() {
        Configuration config = new Configuration("localhost", 32010, null, null, null);
        String str = config.toString();

        assertTrue(str.contains("password='[not set]'"));
        assertTrue(str.contains("bearerToken='[not set]'"));
    }

    // ── connection string hashing ───────────────────────────────────────────

    @Test
    void connectionStringDoesNotExposePassword() {
        Configuration config = new Configuration("localhost", 32010, "user", "my-password", null);
        String cs = config.getConnectionString();

        assertFalse(cs.contains("my-password"),
                "connection string must not contain raw password: " + cs);
    }

    @Test
    void connectionStringDoesNotExposeBearerToken() {
        Configuration config = new Configuration("localhost", 32010, "user", null, "my-token");
        String cs = config.getConnectionString();

        assertFalse(cs.contains("my-token"),
                "connection string must not contain raw bearer token: " + cs);
    }

    @Test
    void connectionStringIsDeterministic() {
        Configuration c1 = new Configuration("localhost", 32010, "user", "pass", null);
        Configuration c2 = new Configuration("localhost", 32010, "user", "pass", null);

        assertEquals(c1.getConnectionString(), c2.getConnectionString(),
                "Same credentials must produce same connection identity");
    }

    @Test
    void connectionStringDiffersForDifferentSecrets() {
        Configuration c1 = new Configuration("localhost", 32010, "user", "pass1", null);
        Configuration c2 = new Configuration("localhost", 32010, "user", "pass2", null);

        assertNotEquals(c1.getConnectionString(), c2.getConnectionString(),
                "Different secrets must produce different connection identities");
    }

    /** Verifies long Flight streams have no default whole-call deadline. */
    @Test
    void defaultConfigurationDoesNotLimitFlightStreamDuration() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        String str = config.toString();

        assertTrue(str.contains("maxRetries=3"));
        assertTrue(str.contains("retryBackoffMs=1000"));
        assertEquals(0, config.getConnectTimeoutMs());
        assertTrue(str.contains("connectTimeoutMs=0"));
    }
}
