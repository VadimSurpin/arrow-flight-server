package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Validates Flight transport settings and authenticates bearer credentials. */
record FlightSecurity(boolean tlsEnabled, File certificate, File privateKey, String bearerToken) {

    /**
     * Resolves security settings from process environment.
     *
     * @return validated settings
     */
    static FlightSecurity fromEnvironment() {
        return from(System.getenv("FLIGHT_TLS_CERT"), System.getenv("FLIGHT_TLS_KEY"),
                System.getenv("FLIGHT_BEARER_TOKEN"),
                Boolean.parseBoolean(System.getenv("FLIGHT_ALLOW_INSECURE")));
    }

    /**
     * Requires TLS and a bearer token unless the caller explicitly enables development mode.
     *
     * @param certPath certificate path
     * @param keyPath private key path
     * @param token shared bearer token
     * @param allowInsecure explicit development mode
     * @return validated settings
     */
    static FlightSecurity from(String certPath, String keyPath, String token,
            boolean allowInsecure) {
        boolean hasCert = certPath != null && !certPath.isBlank();
        boolean hasKey = keyPath != null && !keyPath.isBlank();
        boolean hasToken = token != null && !token.isBlank();
        if (hasCert && hasKey && hasToken) {
            return new FlightSecurity(true, new File(certPath), new File(keyPath), token);
        }
        if (allowInsecure && !hasCert && !hasKey && !hasToken) {
            return new FlightSecurity(false, null, null, null);
        }
        throw new IllegalArgumentException("Flight requires FLIGHT_TLS_CERT, FLIGHT_TLS_KEY, "
                + "and FLIGHT_BEARER_TOKEN; set FLIGHT_ALLOW_INSECURE=true only for "
                + "isolated development deployments");
    }

    /**
     * Creates a location with the selected transport scheme.
     *
     * @param host advertised server hostname
     * @param port Flight port
     * @return Flight location
     */
    Location location(String host, int port) {
        return tlsEnabled ? Location.forGrpcTls(host, port) : Location.forGrpcInsecure(host, port);
    }

    /**
     * Enables TLS and bearer authentication on a Flight server builder.
     *
     * @param builder server builder
     * @return configured builder
     * @throws IOException if TLS materials cannot be loaded
     */
    FlightServer.Builder configure(FlightServer.Builder builder) throws IOException {
        if (tlsEnabled) {
            builder.useTls(certificate, privateKey)
                    .headerAuthenticator(bearerAuthenticator(bearerToken));
        }
        return builder;
    }

    /**
     * Checks the Authorization header with a constant-time token comparison.
     *
     * @param expectedToken configured shared token
     * @return Flight header authenticator
     */
    static CallHeaderAuthenticator bearerAuthenticator(String expectedToken) {
        byte[] expected = expectedToken.getBytes(StandardCharsets.UTF_8);
        return headers -> {
            String authorization = headers.get("Authorization");
            if (authorization == null || !authorization.startsWith("Bearer ")
                    || !MessageDigest.isEqual(expected,
                            authorization.substring(7).getBytes(StandardCharsets.UTF_8))) {
                throw CallStatus.UNAUTHENTICATED
                        .withDescription("Missing or invalid Flight bearer token")
                        .toRuntimeException();
            }
            return () -> "flight-client";
        };
    }
}
