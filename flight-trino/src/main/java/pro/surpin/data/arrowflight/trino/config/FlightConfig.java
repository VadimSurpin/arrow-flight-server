package pro.surpin.data.arrowflight.trino.config;

import pro.surpin.data.arrowflight.client.Configuration;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Catalog properties of the Arrow Flight connector.
 *
 * @param host Flight server host
 * @param port Flight server port
 * @param tlsEnabled whether the connection uses TLS
 * @param verifyServer whether the TLS certificate is verified
 * @param user user name for password authentication
 * @param password password, or null for bearer authentication
 * @param bearerToken bearer token, or null for password authentication
 * @param kerberosServicePrincipal Kerberos service principal, or null for other authentication
 * @param columnQuote identifier quote used in generated SQL
 * @param maxRetries retry count for Flight calls, or null for the client default
 * @param connectTimeoutMs Flight call timeout in milliseconds, or null for the client default
 * @param retryBackoffMs initial retry delay in milliseconds, or null for the client default
 * @param truststoreJks JKS truststore path, or null for system certificates
 * @param truststorePassword JKS password
 * @param defaultSchema default remote schema
 * @param routingTag remote routing tag
 * @param routingQueue remote routing queue
 * @param partitioning optional scan partitioning
 */
public record FlightConfig(String host, int port, boolean tlsEnabled, boolean verifyServer,
                           String user, String password, String bearerToken,
                           String kerberosServicePrincipal, String columnQuote, Integer maxRetries,
                           Long connectTimeoutMs, Long retryBackoffMs, String truststoreJks,
                           String truststorePassword, String defaultSchema, String routingTag,
                           String routingQueue, FlightPartitioning partitioning) {
    static final String HOST = "flight.host";
    static final String PORT = "flight.port";
    static final String TLS_ENABLED = "flight.tls.enabled";
    static final String TLS_VERIFY = "flight.tls.verify-server";
    static final String USER = "flight.user";
    static final String PASSWORD = "flight.password";
    static final String BEARER_TOKEN = "flight.bearer-token";
    static final String KERBEROS_SERVICE_PRINCIPAL = "flight.kerberos.service-principal";
    static final String COLUMN_QUOTE = "flight.column-quote";
    static final String MAX_RETRIES = "flight.max-retries";
    static final String TIMEOUT_MS = "flight.connect-timeout-ms";
    static final String BACKOFF_MS = "flight.retry-backoff-ms";
    static final String TRUSTSTORE = "flight.tls.truststore.jks-file";
    static final String TRUSTSTORE_PASSWORD = "flight.tls.truststore.password";
    static final String DEFAULT_SCHEMA = "flight.default-schema";
    static final String ROUTING_TAG = "flight.routing.tag";
    static final String ROUTING_QUEUE = "flight.routing.queue";

    private static final Set<String> KNOWN = Set.of(HOST, PORT, TLS_ENABLED, TLS_VERIFY, USER,
            PASSWORD, BEARER_TOKEN, KERBEROS_SERVICE_PRINCIPAL, COLUMN_QUOTE, MAX_RETRIES,
            TIMEOUT_MS, BACKOFF_MS, TRUSTSTORE, TRUSTSTORE_PASSWORD, DEFAULT_SCHEMA, ROUTING_TAG, ROUTING_QUEUE);

    /**
     * Parses and validates catalog properties.
     *
     * @param props catalog properties without connector.name
     * @return parsed configuration
     * @throws IllegalArgumentException if a property is unknown, missing or invalid
     */
    public static FlightConfig fromProperties(Map<String, String> props) {
        Set<String> unknown = new TreeSet<>(props.keySet());
        unknown.removeAll(KNOWN);
        unknown.removeAll(FlightPartitioning.PROPERTIES);
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Unknown Arrow Flight catalog properties: " + unknown);
        }
        String host = required(props, HOST);
        int port = parseInt(PORT, required(props, PORT));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException(PORT + " must be between 1 and 65535: " + port);
        }
        String password = blankToNull(props.get(PASSWORD));
        String token = blankToNull(props.get(BEARER_TOKEN));
        String kerberosPrincipal = blankToNull(props.get(KERBEROS_SERVICE_PRINCIPAL));
        int credentialCount = (password == null ? 0 : 1) + (token == null ? 0 : 1)
                + (kerberosPrincipal == null ? 0 : 1);
        if (credentialCount != 1) {
            throw new IllegalArgumentException("Set exactly one of " + PASSWORD + ", "
                    + BEARER_TOKEN + " and " + KERBEROS_SERVICE_PRINCIPAL);
        }
        String user = blankToNull(props.get(USER));
        if (password != null && user == null) {
            throw new IllegalArgumentException(USER + " is required with " + PASSWORD);
        }
        String truststore = blankToNull(props.get(TRUSTSTORE));
        String truststorePassword = props.get(TRUSTSTORE_PASSWORD);
        boolean tlsEnabled = parseBoolean(TLS_ENABLED,
                props.getOrDefault(TLS_ENABLED, Boolean.toString(truststore != null)));
        boolean verifyServer = parseBoolean(TLS_VERIFY, props.getOrDefault(TLS_VERIFY, "true"));
        if (truststore != null && (!tlsEnabled || !verifyServer)) {
            throw new IllegalArgumentException(TRUSTSTORE + " requires TLS and server verification");
        }
        if (truststore == null && truststorePassword != null) {
            throw new IllegalArgumentException(TRUSTSTORE_PASSWORD + " requires " + TRUSTSTORE);
        }
        if (kerberosPrincipal != null && !tlsEnabled) {
            throw new IllegalArgumentException(KERBEROS_SERVICE_PRINCIPAL + " requires "
                    + TLS_ENABLED + "=true");
        }
        String retries = props.get(MAX_RETRIES);
        String timeout = props.get(TIMEOUT_MS);
        return new FlightConfig(host, port,
                tlsEnabled,
                verifyServer,
                user, password, token, kerberosPrincipal,
                props.getOrDefault(COLUMN_QUOTE, ""),
                retries == null ? null : Math.toIntExact(nonNegative(MAX_RETRIES, retries)),
                timeout == null ? null : nonNegative(TIMEOUT_MS, timeout),
                props.containsKey(BACKOFF_MS) ? nonNegative(BACKOFF_MS, props.get(BACKOFF_MS)) : null,
                truststore, truststorePassword == null ? "" : truststorePassword, props.getOrDefault(DEFAULT_SCHEMA, ""),
                props.getOrDefault(ROUTING_TAG, ""), props.getOrDefault(ROUTING_QUEUE, ""),
                FlightPartitioning.fromProperties(props));
    }

    /**
     * Builds the shared Flight client configuration.
     *
     * @return client configuration
     */
    public Configuration toClientConfiguration() {
        Configuration configuration = truststoreJks == null
                ? new Configuration(host, port, tlsEnabled, verifyServer, user, password, bearerToken)
                : new Configuration(host, port, truststoreJks, truststorePassword, user, password, bearerToken);
        if (truststoreJks != null && configuration.getCertificateBytes().length == 0) {
            throw new IllegalArgumentException("JKS truststore must contain a readable trusted certificate");
        }
        configuration.setDefaultSchema(defaultSchema);
        configuration.setRoutingTag(routingTag);
        configuration.setRoutingQueue(routingQueue);
        if (retryBackoffMs != null) {
            configuration.setRetryBackoffMs(retryBackoffMs);
        }
        if (kerberosServicePrincipal != null) {
            configuration.setKerberosServicePrincipal(kerberosServicePrincipal);
        }
        if (maxRetries != null) {
            configuration.setMaxRetries(maxRetries);
        }
        if (connectTimeoutMs != null) {
            configuration.setConnectTimeoutMs(connectTimeoutMs);
        }
        return configuration;
    }

    @Override
    public String toString() {
        return "FlightConfig{host=" + host + ", port=" + port + ", tls=" + tlsEnabled
                + ", user=" + user + ", kerberos=" + (kerberosServicePrincipal != null) + "}";
    }

    /**
     * Reads a required non-blank property.
     *
     * @param props properties
     * @param key property name
     * @return trimmed value
     * @throws IllegalArgumentException if the property is missing
     */
    private static String required(Map<String, String> props, String key) {
        String value = blankToNull(props.get(key));
        if (value == null) {
            throw new IllegalArgumentException("Missing required catalog property: " + key);
        }
        return value.trim();
    }

    /**
     * Parses an integer property.
     *
     * @param key property name
     * @param value property value
     * @return parsed value
     * @throws IllegalArgumentException if the value is not an integer
     */
    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer: " + value, e);
        }
    }

    /**
     * Parses a long property.
     *
     * @param key property name
     * @param value property value
     * @return parsed value
     * @throws IllegalArgumentException if the value is not an integer
     */
    private static long parseLong(String key, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer: " + value, e);
        }
    }

    /**
     * Parses a nonnegative duration or count.
     * @param key property name
     * @param value property value
     * @return parsed value
     * @throws IllegalArgumentException if the value is negative or invalid
     */
    private static long nonNegative(String key, String value) {
        long parsed = parseLong(key, value);
        if (parsed < 0 || (MAX_RETRIES.equals(key) && parsed > Integer.MAX_VALUE)) {
            throw new IllegalArgumentException(key + " is outside its supported range");
        }
        return parsed;
    }

    /**
     * Parses a strict boolean property.
     *
     * @param key property name
     * @param value property value
     * @return parsed value
     * @throws IllegalArgumentException if the value is neither true nor false
     */
    private static boolean parseBoolean(String key, String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (!"true".equals(normalized) && !"false".equals(normalized)) {
            throw new IllegalArgumentException(key + " must be true or false: " + value);
        }
        return Boolean.parseBoolean(normalized);
    }

    /**
     * Converts a missing or blank value to null.
     *
     * @param value value
     * @return value, or null when blank
     */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
