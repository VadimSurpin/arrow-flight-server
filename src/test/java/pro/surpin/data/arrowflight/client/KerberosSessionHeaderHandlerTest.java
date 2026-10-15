package pro.surpin.data.arrowflight.client;

import org.apache.arrow.flight.FlightCallHeaders;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests capture and expiration of a Kerberos Flight bearer session. */
@Tag("unit")
class KerberosSessionHeaderHandlerTest {
    /** Captures one bearer response and retains it through unrelated responses. */
    @Test
    void capturesSessionExpiry() {
        KerberosSessionHeaderHandler handler = new KerberosSessionHeaderHandler();
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "1\n1767226200000\nalice@EXAMPLE.COM".getBytes(StandardCharsets.UTF_8));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("Authorization", "Bearer " + payload + ".signature");
        assertNotNull(handler.getCredentialCallOptionFromIncomingHeaders(headers));
        assertEquals(1767226200000L, handler.expiresAt());
        handler.getCredentialCallOptionFromIncomingHeaders(new FlightCallHeaders());
        assertNotNull(handler.credential());
        handler.reset();
        assertNull(handler.credential());
    }
}
