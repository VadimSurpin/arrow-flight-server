package net.surpin.data.arrowflight.client;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.util.Enumeration;

/**
 * Describes the data-structure for connecting to remote flight-service.
 */
public final class Configuration implements Serializable {
    //the host name of the flight end-point
    private final String fsHost;
    //the port # of the flight end-point
    private final int fsPort;
    //the flight end-point is whether tls enabled
    private final Boolean tlsEnabled;
    private final Boolean crtVerify;

    //the trust-store file & pass-code
    private String trustStoreJks;
    private String trustStorePass;

    //the user and password/access-token with which to connect to flight service
    private final String user;
    private final String password;
    private final String bearerToken;

    //information to manage work-loads
    private String defaultSchema = "";
    private String routingTag = "";
    private String routingQueue = "";

    //the binary content of the certificate
    private byte[] certBytes;

    //memory allocation limit (bytes, 0 = default 2GB)
    private long allocationLimit = 0;

    //retry configuration
    private int maxRetries = 3;
    private long retryBackoffMs = 1000;
    private long connectTimeoutMs;

    //string constant
    private static final String NOT_SET = "[not set]";

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     * @param bearerToken - the pat or auth2 token
     */
    public Configuration(String host, int port, String user, String password, String bearerToken) {
        this(host, port, false, false, user, password, bearerToken);
    }

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param tlsEnabled - whether the flight service has tls enabled for secure connection
     * @param crtVerify -whether to verify the certificate if remote flight service is tls-enabled.
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     * @param bearerToken - the pat or auth2 token
     */
    public Configuration(String host, int port, Boolean tlsEnabled, Boolean crtVerify, String user, String password, String bearerToken) {
        this.fsHost = host;
        this.fsPort = port;
        this.tlsEnabled = tlsEnabled;
        this.crtVerify = crtVerify;

        this.trustStoreJks = null;
        this.trustStorePass = null;
        this.certBytes = null;

        this.user = user;
        this.password = password;
        this.bearerToken = bearerToken;
    }

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param trustStoreJks - the filename of the trust store in jks
     * @param truststorePass - the pass code of the trust store
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     * @param bearerToken - the pat or auth2 token
     */
    public Configuration(String host, int port, String trustStoreJks, String truststorePass, String user, String password, String bearerToken) {
        this(host, port, true, true, user, password, bearerToken);

        this.trustStoreJks = trustStoreJks;
        this.trustStorePass = truststorePass;

        this.certBytes = Configuration.getCertificateBytes(this.trustStoreJks, this.trustStorePass);
    }

    /**
     * Get the host name of the remote flight service
     * @return - the host name
     */
    public String getFlightHost() {
        return this.fsHost;
    }

    /**
     * Get the port number of the remote flight service
     * @return - the port number
     */
    public int getFlightPort() {
        return this.fsPort;
    }

    /**
     * Get the filename of the truststore
     * @return - the filename
     */
    public String getTruststoreJks() {
        return this.trustStoreJks;
    }

    /**
     * Get the password of the truststore.
     * @return - the password
     */
    public String getTruststorePass() {
        return this.trustStorePass;
    }

    /**
     * Get the flag of whether the remote flight service has tls enabled for secure connections
     * @return - true if the remtoe flight service supports secure connections
     */
    public Boolean getTlsEnabled() {
        return this.tlsEnabled;
    }

    /**
     * Get the flag of whether to skip verifying the remote flight service
     * @return - true to skip the verification
     */
    public Boolean verifyServer() {
        return this.crtVerify;
    }

    /**
     * Get the byte conent of the service certificate
     * @return - the byte content
     */
    public byte[] getCertificateBytes() {
        return this.certBytes;
    }

    /**
     * Get the user account
     * @return - the user account
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Get the password of the user account
     * @return - the password of the user account
     */
    public String getPassword() {
        return this.password;
    }
    /**
     * Get the access-token of the user account
     * @return - the access-token of the user account
     */
    public String getBearerToken() {
        return this.bearerToken;
    }

    /**
     * Retrieve an opaque connection identity for client pooling.
     * Does not contain raw secrets — the credential is replaced with a SHA-256 hash.
     * @return - the connection identity string
     */
    public String getConnectionString() {
        String secret = (this.password != null && !this.password.isEmpty()) ? this.password : this.bearerToken;
        return String.format("%s://%s:%s@%s:%d",
                this.tlsEnabled.booleanValue() ? "https:" : "http",
                this.user,
                hash(secret),
                this.fsHost,
                this.fsPort);
    }

    /**
     * Computes SHA-256 hex digest of a value.
     * @param value input string (may be null)
     * @return hex-encoded hash or empty string if null
     */
    private static String hash(String value) {
        if (value == null) {
            return "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads certificate bytes from a JKS truststore.
     * @param keyStorePath path to JKS file
     * @param keyStorePassword truststore password
     * @return certificate bytes or empty array on failure
     */
    private static byte[] getCertificateBytes(String keyStorePath, String keyStorePassword) {
        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream keyStoreStream = Files.newInputStream(Paths.get(keyStorePath))) {
                keyStore.load(keyStoreStream, keyStorePassword.toCharArray());
            }

            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isCertificateEntry(alias)) {
                    Certificate certificates = keyStore.getCertificate(alias);
                    return toBytes(certificates);
                }
            }
        } catch (GeneralSecurityException | IOException e) {
            LoggerFactory.getLogger(Configuration.class).warn(
                    "Cannot load certificate from {}", keyStorePath, e);
        }
        return new byte[0];
    }

    /**
     * Serializes an X.509 certificate to PEM-encoded bytes.
     * @param certificate the certificate
     * @return PEM bytes
     * @throws IOException on write error
     */
    private static byte[] toBytes(Certificate certificate) throws IOException {
        try (
            StringWriter writer = new StringWriter();
            JcaPEMWriter pemWriter = new JcaPEMWriter(writer)
        ) {
            pemWriter.writeObject(certificate);
            pemWriter.flush();
            return writer.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Get the path of the default schema
     * @return - Default schema path to the dataset that the user wants to query.
     */
    public String getDefaultSchema() {
        return defaultSchema;
    }

    /**
     * Set the path of default schema
     * @param defaultSchema - Default schema path to the dataset that the user wants to query.
     */
    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    /**
     * Get the routing-tag
     * @return - Tag name associated with all queries executed within a Flight session. Used only during authentication.
     */
    public String getRoutingTag() {
        return this.routingTag;
    }

    /**
     * Set the rouging-tag
     * @param routingTag - Tag name associated with all queries executed within a Flight session. Used only during authentication.
     */
    public void setRoutingTag(String routingTag) {
        this.routingTag = routingTag;
    }

    /**
     * Get the routing-queue
     * @return - Name of the workload management queue. Used only during authentication.
     */
    public String getRoutingQueue() {
        return this.routingQueue;
    }

    /**
     * Set the routing-queue
     * @param routingQueue - Name of the workload management queue. Used only during authentication.
     */
    public void setRoutingQueue(String routingQueue) {
        this.routingQueue = routingQueue;
    }

    /**
     * Get the Arrow memory allocation limit in bytes (0 = default 2GB).
     */
    public long getAllocationLimit() {
        return this.allocationLimit;
    }

    /**
     * Set the Arrow memory allocation limit in bytes (0 = default 2GB).
     */
    public void setAllocationLimit(long limit) {
        this.allocationLimit = limit;
    }

    /**
     * Gets the maximum number of retry attempts.
     * @return max retries
     */
    public int getMaxRetries() {
        return this.maxRetries;
    }

    /**
     * Sets the maximum number of retry attempts.
     * @param maxRetries max retries
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Gets the retry backoff interval in milliseconds.
     * @return backoff in ms
     */
    public long getRetryBackoffMs() {
        return this.retryBackoffMs;
    }

    /**
     * Sets the retry backoff interval in milliseconds.
     * @param retryBackoffMs backoff in ms
     */
    public void setRetryBackoffMs(long retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Gets the connection timeout in milliseconds.
     * @return timeout in ms
     */
    public long getConnectTimeoutMs() {
        return this.connectTimeoutMs;
    }

    /**
     * Sets the connection timeout in milliseconds.
     * @param connectTimeoutMs timeout in ms
     */
    public void setConnectTimeoutMs(long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    private static String formatValue(Object value) {
        return value != null ? value.toString() : NOT_SET;
    }

    private static String formatSensitive(Object value) {
        return value != null ? "[hidden]" : NOT_SET;
    }

    private static String formatBytes(byte[] bytes) {
        return bytes != null ? "[hidden, " + bytes.length + " bytes]" : NOT_SET;
    }

    @Override
    public String toString() {
        return "Configuration{" +
            "fsHost='" + formatValue(fsHost) + '\'' +
            ", fsPort=" + fsPort +
            ", tlsEnabled=" + tlsEnabled +
            ", crtVerify=" + crtVerify +
            ", trustStoreJks='" + formatValue(trustStoreJks) + '\'' +
            ", trustStorePass='" + formatSensitive(trustStorePass) + '\'' +
            ", user='" + formatValue(user) + '\'' +
            ", password='" + formatSensitive(password) + '\'' +
            ", bearerToken='" + formatSensitive(bearerToken) + '\'' +
            ", defaultSchema='" + formatValue(defaultSchema) + '\'' +
            ", routingTag='" + formatValue(routingTag) + '\'' +
            ", routingQueue='" + formatValue(routingQueue) + '\'' +
            ", certBytes='" + formatBytes(certBytes) + '\'' +
            ", allocationLimit=" + allocationLimit +
            ", maxRetries=" + maxRetries +
            ", retryBackoffMs=" + retryBackoffMs +
            ", connectTimeoutMs=" + connectTimeoutMs +
            '}';
    }
}
