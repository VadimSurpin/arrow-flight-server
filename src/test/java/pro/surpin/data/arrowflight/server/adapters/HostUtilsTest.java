package pro.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HostUtilsTest {

    @Test
    void normalizeNull() {
        assertNull(HostUtils.normalize(null));
    }

    @Test
    void normalizeEmpty() {
        assertEquals("", HostUtils.normalize(""));
    }

    @Test
    void normalizeLoopbackRaw() {
        assertEquals("127.0.0.1", HostUtils.normalize("127.0.0.1"));
    }

    @Test
    void normalizeLoopbackLocalhost() {
        assertEquals("127.0.0.1", HostUtils.normalize("localhost"));
    }

    @Test
    void normalizeLoopbackIpv6() {
        assertEquals("127.0.0.1", HostUtils.normalize("::1"));
    }

    @Test
    void normalizeUriWithLoopbackHost() {
        assertEquals("127.0.0.1", HostUtils.normalize("grpc://localhost:32010"));
    }

    @Test
    void normalizeUriWithLoopbackIp() {
        assertEquals("127.0.0.1", HostUtils.normalize("grpc://127.0.0.1:32010"));
    }

    @Test
    void normalizeUsesCache() {
        HostUtils.normalize("127.0.0.1");

        long start = System.nanoTime();
        String result = HostUtils.normalize("127.0.0.1");
        long elapsed = System.nanoTime() - start;

        assertEquals("127.0.0.1", result);
        assertTrue(elapsed < 10_000_000,
                "Cached lookup should be sub-millisecond, took " + elapsed + "ns");
    }

    @Test
    void normalizeLoopbackSetContainsExpectedValues() {
        assertTrue(HostUtils.LOOPBACK_HOSTS.contains("localhost"));
        assertTrue(HostUtils.LOOPBACK_HOSTS.contains("127.0.0.1"));
        assertTrue(HostUtils.LOOPBACK_HOSTS.contains("::1"));
        assertEquals(3, HostUtils.LOOPBACK_HOSTS.size());
    }
}
