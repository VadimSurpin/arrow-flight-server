package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.Base64;

/** Validates Flight transport settings and authenticates bearer credentials. */
record FlightSecurity(boolean tlsEnabled, File certificate, File privateKey,
        String bearerToken, String kerberosPrincipal, String kerberosKeytab,
        byte[] kerberosSessionSecret) {

    /**
     * Resolves security settings from process environment.
     *
     * @return validated settings
     */
    static FlightSecurity fromEnvironment() {
        String principal = System.getenv("FLIGHT_KERBEROS_PRINCIPAL");
        String keytab = System.getenv("FLIGHT_KERBEROS_KEYTAB");
        String sessionSecret = System.getenv("FLIGHT_KERBEROS_SESSION_SECRET");
        if (principal != null || keytab != null || sessionSecret != null) {
            if (System.getenv("FLIGHT_BEARER_TOKEN") != null) {
                throw new IllegalArgumentException(
                        "Kerberos Flight authentication cannot use FLIGHT_BEARER_TOKEN");
            }
            return fromKerberos(System.getenv("FLIGHT_TLS_CERT"),
                    System.getenv("FLIGHT_TLS_KEY"), principal, keytab, sessionSecret);
        }
        if (System.getenv("RANGER_SERVICE_NAME") != null) {
            throw new IllegalArgumentException("Ranger requires Kerberos Flight authentication");
        }
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
            return new FlightSecurity(true, new File(certPath), new File(keyPath),
                    token, null, null, null);
        }
        if (allowInsecure && !hasCert && !hasKey && !hasToken) {
            return new FlightSecurity(false, null, null, null, null, null, null);
        }
        throw new IllegalArgumentException("Flight requires FLIGHT_TLS_CERT, FLIGHT_TLS_KEY, "
                + "and FLIGHT_BEARER_TOKEN; set FLIGHT_ALLOW_INSECURE=true only for "
                + "isolated development deployments");
    }

    /**
     * Validates keytab-based Kerberos settings for a TLS Flight server.
     *
     * @param certPath PEM certificate path
     * @param keyPath PEM private key path
     * @param principal server Kerberos principal
     * @param keytab keytab file path
     * @param sessionSecret cluster-shared base64 HMAC key
     * @return validated settings
     */
    static FlightSecurity fromKerberos(String certPath, String keyPath, String principal,
            String keytab, String sessionSecret) {
        if (certPath == null || certPath.isBlank() || keyPath == null || keyPath.isBlank()
                || principal == null || principal.isBlank()
                || keytab == null || keytab.isBlank()
                || sessionSecret == null || sessionSecret.isBlank()) {
            throw new IllegalArgumentException("Kerberos Flight requires TLS certificate, key, "
                    + "FLIGHT_KERBEROS_PRINCIPAL, FLIGHT_KERBEROS_KEYTAB, and "
                    + "FLIGHT_KERBEROS_SESSION_SECRET");
        }
        byte[] secret = Base64.getDecoder().decode(sessionSecret);
        if (secret.length < 32) {
            throw new IllegalArgumentException("Kerberos session secret must be at least 32 bytes");
        }
        return new FlightSecurity(true, new File(certPath), new File(keyPath),
                null, principal, keytab, secret);
    }

    /**
     * Logs the server into Kerberos before Hadoop file systems are constructed.
     *
     * @param hadoopConfig shared Hadoop configuration
     * @throws IOException on keytab login failure
     */
    void initializeKerberos(Configuration hadoopConfig) throws IOException {
        if (kerberosPrincipal != null) {
            hadoopConfig.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(hadoopConfig);
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
        }
    }

    /**
     * Reports whether the server uses Kerberos credentials.
     *
     * @return true when Kerberos is configured
     */
    boolean kerberosEnabled() {
        return kerberosPrincipal != null;
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
            CallHeaderAuthenticator authenticator = kerberosEnabled()
                    ? new KerberosFlightAuthenticator(
                            KerberosFlightAuthenticator.gssAcceptor(
                                    UserGroupInformation.getLoginUser()),
                            new KerberosSessionToken(kerberosSessionSecret, Clock.systemUTC()))
                    : bearerAuthenticator(bearerToken);
            builder.useTls(certificate, privateKey).headerAuthenticator(authenticator);
        }
        return builder;
    }

    @Override
    public String toString() {
        return "FlightSecurity[tlsEnabled=" + tlsEnabled
                + ", kerberosEnabled=" + kerberosEnabled() + "]";
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
