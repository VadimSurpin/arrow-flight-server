package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests signed Flight session validation across nodes. */
@Tag("unit")
class KerberosSessionTokenTest {
    private static final byte[] KEY = new byte[32];

    /** Accepts a session on another node with the same secret. */
    @Test
    void sessionWorksAcrossNodes() {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        String token = new KerberosSessionToken(KEY, clock).issue("alice@EXAMPLE.COM");
        assertEquals("alice@EXAMPLE.COM", new KerberosSessionToken(KEY, clock).verify(token));
    }

    /** Rejects tampering, wrong keys, and expired sessions. */
    @Test
    void rejectsInvalidSessions() {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        String token = new KerberosSessionToken(KEY, clock).issue("alice@EXAMPLE.COM");
        byte[] otherKey = new byte[32];
        otherKey[0] = 1;
        assertThrows(IllegalArgumentException.class,
                () -> new KerberosSessionToken(otherKey, clock).verify(token));
        assertThrows(IllegalArgumentException.class,
                () -> new KerberosSessionToken(KEY, clock).verify(token + "x"));
        Clock later = Clock.offset(clock, java.time.Duration.ofMinutes(11));
        assertThrows(IllegalArgumentException.class,
                () -> new KerberosSessionToken(KEY, later).verify(token));
    }
}
