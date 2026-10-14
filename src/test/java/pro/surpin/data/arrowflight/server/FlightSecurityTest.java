package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies Flight TLS requirements and bearer token authentication. */
@Tag("unit")
class FlightSecurityTest {

    /** Prevents a production startup without complete TLS and bearer settings. */
    @Test
    void requiresCompleteSecurityConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.from(null, null, null, false));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.from("cert.pem", null, "secret", true));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.from(null, null, "secret", true));
    }

    /** Selects TLS for secured deployments and plaintext only by explicit opt-in. */
    @Test
    void selectsFlightTransport() {
        FlightSecurity secure = FlightSecurity.from("cert.pem", "key.pem", "secret", false);
        assertTrue(secure.tlsEnabled());
        assertEquals("grpc+tls", secure.location("flight.example", 32010).getUri().getScheme());

        FlightSecurity development = FlightSecurity.from(null, null, null, true);
        assertFalse(development.tlsEnabled());
        assertEquals("grpc+tcp",
                development.location("localhost", 32010).getUri().getScheme());
    }

    /** Rejects missing and incorrect bearer credentials while accepting the configured token. */
    @Test
    void bearerAuthenticatorRejectsInvalidCredentials() {
        CallHeaders headers = mock(CallHeaders.class);
        var authenticator = FlightSecurity.bearerAuthenticator("expected-token");
        assertEquals(FlightStatusCode.UNAUTHENTICATED,
                assertThrows(FlightRuntimeException.class,
                        () -> authenticator.authenticate(headers)).status().code());

        when(headers.get("Authorization")).thenReturn("Bearer wrong-token");
        assertEquals(FlightStatusCode.UNAUTHENTICATED,
                assertThrows(FlightRuntimeException.class,
                        () -> authenticator.authenticate(headers)).status().code());

        when(headers.get("Authorization")).thenReturn("Bearer expected-token");
        assertEquals("flight-client", authenticator.authenticate(headers).getPeerIdentity());
    }
}
