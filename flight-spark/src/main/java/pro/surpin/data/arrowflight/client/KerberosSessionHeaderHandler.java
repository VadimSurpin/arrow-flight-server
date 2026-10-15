package pro.surpin.data.arrowflight.client;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientHeaderHandler;
import org.apache.arrow.flight.grpc.CredentialCallOption;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Captures the signed session expiry returned by the Flight server. */
final class KerberosSessionHeaderHandler implements ClientHeaderHandler {
    private final ClientBearerHeaderHandler delegate = new ClientBearerHeaderHandler();
    private volatile long expiresAt;
    private volatile CredentialCallOption credential;

    @Override
    public CredentialCallOption getCredentialCallOptionFromIncomingHeaders(CallHeaders headers) {
        CredentialCallOption option = delegate.getCredentialCallOptionFromIncomingHeaders(headers);
        if (option != null) {
            String bearer = headers.get("Authorization");
            if (bearer == null || !bearer.startsWith("Bearer ")) {
                throw new IllegalArgumentException("Missing Kerberos Flight session header");
            }
            String token = bearer.substring(7);
            int separator = token.indexOf('.');
            if (separator <= 0) {
                throw new IllegalArgumentException("Invalid Kerberos Flight session");
            }
            String payload = new String(Base64.getUrlDecoder().decode(
                    token.substring(0, separator)), StandardCharsets.UTF_8);
            String[] fields = payload.split("\\n", 3);
            if (fields.length != 3 || !"1".equals(fields[0])) {
                throw new IllegalArgumentException("Invalid Kerberos Flight session");
            }
            expiresAt = Long.parseLong(fields[1]);
            credential = option;
        }
        return option;
    }

    /**
     * Returns the session expiration in Unix milliseconds.
     *
     * @return expiration time
     */
    long expiresAt() {
        return expiresAt;
    }

    /**
     * Returns the most recently issued bearer credential.
     *
     * @return session credential
     */
    CredentialCallOption credential() {
        return credential;
    }

    /** Clears the previous bearer before a fresh Kerberos exchange. */
    void reset() {
        credential = null;
        expiresAt = 0;
    }
}
