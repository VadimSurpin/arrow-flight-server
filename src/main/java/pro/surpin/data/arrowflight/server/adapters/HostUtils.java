package pro.surpin.data.arrowflight.server.adapters;

import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Normalizes hostnames and URIs to IP addresses with caching.
 * Accepts server URIs, domain names, and raw IPs.
 */
public final class HostUtils {
    private static final String IPV4_LOOPBACK = "127.0.0.1";

    public static final Set<String> LOOPBACK_HOSTS = Set.of("localhost", IPV4_LOOPBACK, "::1");

    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    private HostUtils() {
    }

    /**
     * Normalizes a hostname or URI to an IP address.
     * Results are cached to avoid repeated DNS resolution.
     *
     * @param input hostname, URI, or IP
     * @return resolved IP address, or original input on failure
     */
    public static String normalize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        String cached = CACHE.get(input);
        if (cached != null) {
            return cached;
        }

        // Short-circuit for known loopback addresses
        String hostOnly = input.contains("://") ? URI.create(input).getHost() : input;
        if (hostOnly != null && LOOPBACK_HOSTS.contains(hostOnly)) {
            CACHE.put(input, IPV4_LOOPBACK);
            return IPV4_LOOPBACK;
        }

        String resolved;
        try {
            String host = input;

            if (input.contains("://")) {
                URI uri = URI.create(input);
                if (uri.getHost() != null) {
                    host = uri.getHost();
                }
            }

            resolved = InetAddress.getByName(host).getHostAddress();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            resolved = input;
        }

        CACHE.put(input, resolved);
        return resolved;
    }
}
