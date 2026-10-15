package pro.surpin.data.arrowflight.server;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.Base64;

/** Signs short-lived bearer sessions issued after Kerberos authentication. */
final class KerberosSessionToken {
    private static final long LIFETIME_MILLIS = 10 * 60 * 1000L;
    private static final int MAX_TOKEN_LENGTH = 4096;
    private final byte[] secret;
    private final Clock clock;

    /**
     * Creates a codec using a secret shared by all Flight nodes.
     *
     * @param secret HMAC key with at least 32 bytes
     * @param clock time source
     */
    KerberosSessionToken(byte[] secret, Clock clock) {
        if (secret == null || secret.length < 32) {
            throw new IllegalArgumentException("Kerberos session secret must be at least 32 bytes");
        }
        this.secret = secret.clone();
        this.clock = clock;
    }

    /**
     * Issues a signed bearer token for a Kerberos principal.
     *
     * @param principal authenticated Kerberos principal
     * @return bearer token
     */
    String issue(String principal) {
        if (principal == null || principal.isBlank() || principal.length() > 1024
                || principal.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("Invalid Kerberos principal");
        }
        String payload = "1\n" + (clock.millis() + LIFETIME_MILLIS) + "\n" + principal;
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String signature = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(sign(encoded.getBytes(StandardCharsets.US_ASCII)));
        return encoded + "." + signature;
    }

    /**
     * Verifies a bearer token and returns its authenticated principal.
     *
     * @param token bearer token
     * @return Kerberos principal
     */
    String verify(String token) {
        if (token == null || token.length() > MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException("Invalid Kerberos session token");
        }
        int separator = token.indexOf('.');
        if (separator < 1 || separator != token.lastIndexOf('.')) {
            throw new IllegalArgumentException("Invalid Kerberos session token");
        }
        try {
            String encoded = token.substring(0, separator);
            byte[] signature = Base64.getUrlDecoder().decode(token.substring(separator + 1));
            if (!MessageDigest.isEqual(signature,
                    sign(encoded.getBytes(StandardCharsets.US_ASCII)))) {
                throw new IllegalArgumentException("Invalid Kerberos session signature");
            }
            String payload = new String(Base64.getUrlDecoder().decode(encoded),
                    StandardCharsets.UTF_8);
            String[] parts = payload.split("\n", 3);
            if (parts.length != 3 || !"1".equals(parts[0]) || parts[2].isBlank()
                    || parts[2].length() > 1024 || parts[2].indexOf('\n') >= 0) {
                throw new IllegalArgumentException("Invalid Kerberos session payload");
            }
            long expires = Long.parseLong(parts[1]);
            if (expires <= clock.millis() || expires > clock.millis() + LIFETIME_MILLIS) {
                throw new IllegalArgumentException("Expired Kerberos session token");
            }
            return parts[2];
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Kerberos session token", e);
        }
    }

    /**
     * Signs token bytes with HMAC SHA-256.
     *
     * @param payload signed bytes
     * @return signature
     */
    private byte[] sign(byte[] payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(payload);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC SHA-256 unavailable", e);
        }
    }
}
