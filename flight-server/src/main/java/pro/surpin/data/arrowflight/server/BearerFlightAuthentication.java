package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Authenticates requests using a shared bearer secret. */
final class BearerFlightAuthentication implements FlightAuthenticationProvider {
    private final byte[] expected;

    /**
     * Creates a shared-secret authenticator.
     *
     * @param token configured bearer secret
     */
    BearerFlightAuthentication(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Flight bearer token is required");
        }
        expected = token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public CallHeaderAuthenticator authenticator() {
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

    @Override
    public boolean requiresTls() {
        return true;
    }

    @Override
    public boolean establishesIdentity() {
        return false;
    }
}
