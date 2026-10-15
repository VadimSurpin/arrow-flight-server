package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Verifies built-in and ServiceLoader security provider selection. */
@Tag("unit")
class SecurityProvidersTest {
    /** Releases custom authentication provider resources during security shutdown. */
    @Test
    void closesAuthenticationProvider() {
        FlightAuthenticationProvider provider = mock(FlightAuthenticationProvider.class);
        new FlightSecurity(false, null, null, provider).close();
        verify(provider).close();
    }

    /** Discovers custom authentication and authorization without changing server wiring. */
    @Test
    void discoversCustomProviders() throws Exception {
        Map<String, String> settings = Map.of(
                "FLIGHT_AUTH_PROVIDER", "test-auth",
                "FLIGHT_AUTHORIZATION_PROVIDER", "test-policy",
                "FLIGHT_TLS_CERT", "cert.pem",
                "FLIGHT_TLS_KEY", "key.pem");
        FlightSecurity security = FlightSecurity.fromSettings(settings);
        assertTrue(security.tlsEnabled());
        assertEquals("test-user", security.authentication().authenticator()
                .authenticate(new FlightCallHeaders()).getPeerIdentity());
        try (FlightAuthorization policy = SecurityProviders.authorization(settings)) {
            assertTrue(policy.canSeeSchema("test-user", "public"));
            assertFalse(policy.canReadTable("test-user", "private", "orders"));
            assertTrue(policy.canReadTable("test-user", "public", "orders"));
            policy.requireQuery("test-user", "SELECT * FROM public.orders");
            assertEquals(FlightStatusCode.UNAUTHORIZED,
                    assertThrows(FlightRuntimeException.class,
                            () -> policy.requireQuery("test-user",
                                    "SELECT * FROM public.orders o JOIN private.orders p "
                                            + "ON o.id = p.id")).status().code());
        }
    }

    /** Keeps existing bearer, Kerberos, and development selection behavior. */
    @Test
    void selectsBuiltInProviders() {
        FlightSecurity bearer = FlightSecurity.fromSettings(Map.of(
                "FLIGHT_TLS_CERT", "cert.pem", "FLIGHT_TLS_KEY", "key.pem",
                "FLIGHT_BEARER_TOKEN", "secret"));
        assertTrue(bearer.authentication() instanceof BearerFlightAuthentication);
        assertTrue(SecurityProviders.authorization(Map.of()) instanceof AllowAllAuthorization);
        FlightSecurity insecure = FlightSecurity.fromSettings(
                Map.of("FLIGHT_ALLOW_INSECURE", "true"));
        assertFalse(insecure.tlsEnabled());
        assertTrue(insecure.authentication() instanceof InsecureFlightAuthentication);
        FlightSecurity kerberos = FlightSecurity.fromSettings(Map.of(
                "FLIGHT_TLS_CERT", "cert.pem", "FLIGHT_TLS_KEY", "key.pem",
                "FLIGHT_KERBEROS_PRINCIPAL", "flight/host@EXAMPLE.COM",
                "FLIGHT_KERBEROS_KEYTAB", "flight.keytab",
                "FLIGHT_KERBEROS_SESSION_SECRET", Base64.getEncoder()
                        .encodeToString(new byte[32]),
                "RANGER_SERVICE_NAME", "flight-prod"));
        assertTrue(kerberos.authentication() instanceof KerberosFlightAuthentication);
    }

    /** Rejects unknown providers and conflicting policy or authentication settings. */
    @Test
    void rejectsInvalidProviderConfigurations() {
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.fromSettings(Map.of(
                        "FLIGHT_AUTH_PROVIDER", "unknown",
                        "FLIGHT_TLS_CERT", "cert.pem", "FLIGHT_TLS_KEY", "key.pem")));
        assertThrows(IllegalArgumentException.class,
                () -> SecurityProviders.authorization(Map.of(
                        "FLIGHT_AUTHORIZATION_PROVIDER", "unknown")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.fromSettings(Map.of(
                        "FLIGHT_AUTH_PROVIDER", "test-auth")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.fromSettings(Map.of(
                        "FLIGHT_ALLOW_INSECURE", "true",
                        "FLIGHT_TLS_CERT", "cert.pem", "FLIGHT_TLS_KEY", "key.pem")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.fromSettings(Map.of(
                        "FLIGHT_AUTH_PROVIDER", "bearer",
                        "FLIGHT_AUTHORIZATION_PROVIDER", "test-policy",
                        "FLIGHT_BEARER_TOKEN", "secret",
                        "FLIGHT_TLS_CERT", "cert.pem", "FLIGHT_TLS_KEY", "key.pem")));
        assertThrows(IllegalArgumentException.class,
                () -> SecurityProviders.authorization(Map.of(
                        "FLIGHT_AUTHORIZATION_PROVIDER", "allow-all",
                        "RANGER_SERVICE_NAME", "prod")));
        Map<String, String> conflicting = new HashMap<>();
        conflicting.put("FLIGHT_AUTH_PROVIDER", "test-auth");
        conflicting.put("FLIGHT_BEARER_TOKEN", "secret");
        assertThrows(IllegalArgumentException.class,
                () -> SecurityProviders.authentication(conflicting));
    }
}
