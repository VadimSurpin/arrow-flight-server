package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests Kerberos handshake and bearer session authentication. */
@Tag("unit")
class KerberosFlightAuthenticatorTest {
    /** Exchanges a valid service ticket for a signed session. */
    @Test
    void exchangesTicketAndAcceptsSession() {
        KerberosFlightAuthenticator auth = new KerberosFlightAuthenticator(
                ticket -> "alice@EXAMPLE.COM",
                new KerberosSessionToken(new byte[32], Clock.systemUTC()));
        CallHeaders incoming = mock(CallHeaders.class);
        when(incoming.get("Authorization")).thenReturn("Negotiate "
                + Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}));
        var result = auth.authenticate(incoming);
        assertEquals("alice@EXAMPLE.COM", result.getPeerIdentity());
        CallHeaders outgoing = mock(CallHeaders.class);
        result.appendToOutgoingHeaders(outgoing);
        org.mockito.ArgumentCaptor<String> token = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(outgoing).insert(org.mockito.ArgumentMatchers.eq("Authorization"), token.capture());
        when(incoming.get("Authorization")).thenReturn(token.getValue());
        assertEquals("alice@EXAMPLE.COM", auth.authenticate(incoming).getPeerIdentity());
    }

    /** Rejects missing and invalid Kerberos credentials. */
    @Test
    void rejectsInvalidCredentials() {
        KerberosFlightAuthenticator auth = new KerberosFlightAuthenticator(
                ticket -> { throw new IllegalArgumentException("bad ticket"); },
                new KerberosSessionToken(new byte[32], Clock.systemUTC()));
        CallHeaders incoming = mock(CallHeaders.class);
        assertEquals(FlightStatusCode.UNAUTHENTICATED,
                assertThrows(FlightRuntimeException.class,
                        () -> auth.authenticate(incoming)).status().code());
        when(incoming.get("Authorization")).thenReturn("Negotiate AQID");
        assertEquals(FlightStatusCode.UNAUTHENTICATED,
                assertThrows(FlightRuntimeException.class,
                        () -> auth.authenticate(incoming)).status().code());
    }
}
