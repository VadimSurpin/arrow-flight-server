package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Combines Flight transport settings with a selected authentication provider. */
record FlightSecurity(boolean tlsEnabled, File certificate, File privateKey,
        FlightAuthenticationProvider authentication) implements AutoCloseable {

    /**
     * Resolves transport and authentication from the process environment.
     *
     * @return validated Flight security settings
     */
    static FlightSecurity fromEnvironment() {
        return fromSettings(System.getenv());
    }

    /**
     * Resolves transport and authentication from settings.
     *
     * @param environment deployment settings
     * @return validated Flight security settings
     */
    static FlightSecurity fromSettings(Map<String, String> environment) {
        String cert = environment.get("FLIGHT_TLS_CERT");
        String key = environment.get("FLIGHT_TLS_KEY");
        boolean hasCert = cert != null && !cert.isBlank();
        boolean hasKey = key != null && !key.isBlank();
        if (hasCert != hasKey) {
            throw new IllegalArgumentException("Flight TLS requires certificate and private key");
        }
        FlightAuthenticationProvider provider = SecurityProviders.authentication(environment);
        if (hasCert && provider instanceof InsecureFlightAuthentication) {
            throw new IllegalArgumentException("Insecure Flight mode cannot use TLS settings");
        }
        if (!hasCert && provider.requiresTls()) {
            throw new IllegalArgumentException("Flight authentication provider requires TLS");
        }
        if (SecurityProviders.authorizationRequiresIdentity(environment)
                && !provider.establishesIdentity()) {
            throw new IllegalArgumentException("Flight authorization provider requires "
                    + "distinct authenticated caller identities");
        }
        return new FlightSecurity(hasCert, hasCert ? new File(cert) : null,
                hasKey ? new File(key) : null, provider);
    }

    /**
     * Validates static bearer or explicit development settings.
     *
     * @param certPath certificate path
     * @param keyPath private key path
     * @param token shared bearer secret
     * @param allowInsecure explicit development mode
     * @return validated settings
     */
    static FlightSecurity from(String certPath, String keyPath, String token,
            boolean allowInsecure) {
        Map<String, String> settings = new HashMap<>();
        if (certPath != null) {
            settings.put("FLIGHT_TLS_CERT", certPath);
        }
        if (keyPath != null) {
            settings.put("FLIGHT_TLS_KEY", keyPath);
        }
        if (token != null) {
            settings.put("FLIGHT_BEARER_TOKEN", token);
        }
        if (allowInsecure) {
            settings.put("FLIGHT_ALLOW_INSECURE", "true");
        }
        return fromSettings(settings);
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
        Map<String, String> settings = new HashMap<>();
        if (certPath != null) {
            settings.put("FLIGHT_TLS_CERT", certPath);
        }
        if (keyPath != null) {
            settings.put("FLIGHT_TLS_KEY", keyPath);
        }
        if (principal != null) {
            settings.put("FLIGHT_KERBEROS_PRINCIPAL", principal);
        }
        if (keytab != null) {
            settings.put("FLIGHT_KERBEROS_KEYTAB", keytab);
        }
        if (sessionSecret != null) {
            settings.put("FLIGHT_KERBEROS_SESSION_SECRET", sessionSecret);
        }
        return fromSettings(settings);
    }

    /**
     * Initializes the selected provider before Hadoop services are constructed.
     *
     * @param hadoopConfig shared Hadoop configuration
     * @throws IOException if credentials cannot be initialized
     */
    void initializeAuthentication(Configuration hadoopConfig) throws IOException {
        authentication.initialize(hadoopConfig);
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
     * Applies TLS and the selected authenticator to a Flight server builder.
     *
     * @param builder server builder
     * @return configured builder
     * @throws IOException if security materials cannot be loaded
     */
    FlightServer.Builder configure(FlightServer.Builder builder) throws IOException {
        if (tlsEnabled) {
            builder.useTls(certificate, privateKey);
        }
        CallHeaderAuthenticator authenticator = authentication.authenticator();
        if (authenticator != null) {
            builder.headerAuthenticator(authenticator);
        }
        return builder;
    }

    @Override
    public String toString() {
        return "FlightSecurity[tlsEnabled=" + tlsEnabled
                + ", authentication=" + authentication.getClass().getSimpleName() + "]";
    }

    /**
     * Builds the built-in bearer authenticator for legacy callers.
     *
     * @param expectedToken configured shared token
     * @return bearer authenticator
     */
    static CallHeaderAuthenticator bearerAuthenticator(String expectedToken) {
        return new BearerFlightAuthentication(expectedToken).authenticator();
    }

    @Override
    public void close() {
        authentication.close();
    }
}
