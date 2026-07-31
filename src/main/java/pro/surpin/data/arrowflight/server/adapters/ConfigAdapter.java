package pro.surpin.data.arrowflight.server.adapters;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import pro.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Loads configuration from arrowflight.properties, system properties, and environment variables.
 * Produces an immutable AppConfig record.
 */
public class ConfigAdapter {

    private static final String CONFIG_RESOURCE = "arrowflight.properties";

    /**
     * Utility class, no instantiation.
     */
    private ConfigAdapter() {
    }

    /**
     * Loads and returns the complete server configuration.
     *
     * @return resolved AppConfig
     */
    public static AppConfig getConfig() {
        Properties props = loadProperties();
        int ioParallelism = computeIoParallelism(props);
        int numServers = getInt("numServers", "arrowflight.cluster.numServers", 3, props);
        int batchSize = getInt("batchSize", "arrowflight.duckdb.batchSize", 65536, props);
        int ioFileBufferSize = getInt("ioFileBufferSize", null, 131072, props);
        int duckDbWarmConnections = getInt("duckDbWarmConnections",
                "arrowflight.duckdb.warmConnections", Math.min(8, ioParallelism), props);
        int duckDbGroups = getInt("duckDbGroups",
                "arrowflight.duckdb.groups", Math.min(8, ioParallelism), props);
        int duckDbThreads = getInt("duckDbThreads", "arrowflight.duckdb.threads", 1, props);
        String duckDbHdfsExtension = getStringWithEnv("duckDbHdfsExtension",
                "arrowflight.duckdb.hdfs.extension", "DUCKDB_HDFS_EXTENSION", null, props);
        boolean duckDbAllowUnsignedExtensions = getBooleanWithEnv("duckDbAllowUnsignedExtensions",
                "arrowflight.duckdb.allowUnsignedExtensions", "DUCKDB_ALLOW_UNSIGNED_EXTENSIONS",
                false, props);
        String duckDbHdfsDefaultNamenode = getStringWithEnv("duckDbHdfsDefaultNamenode",
                "arrowflight.duckdb.hdfs.defaultNamenode", "HDFS_DEFAULT_NAMENODE", null, props);
        String duckDbHdfsHaNamenodes = getStringWithEnv("duckDbHdfsHaNamenodes",
                "arrowflight.duckdb.hdfs.haNamenodes", "HDFS_HA_NAMENODES", null, props);
        String duckDbHdfsShortcircuit = getStringWithEnv("duckDbHdfsShortcircuit",
                "arrowflight.duckdb.hdfs.shortcircuit", "HDFS_SHORTCIRCUIT", null, props);
        String duckDbHdfsDomainSocketPath = getStringWithEnv("duckDbHdfsDomainSocketPath",
                "arrowflight.duckdb.hdfs.domainSocketPath", "HDFS_DOMAIN_SOCKET_PATH", null, props);
        boolean metricsEnabled = getBooleanWithEnv("metricsEnabled",
                "arrowflight.metrics.enabled", "FLIGHT_METRICS_ENABLED", false, props);
        int grpcMaxInboundMessageSize = getInt("grpcMaxInboundMessageSize",
                "arrowflight.grpc.maxInboundMessageSize", Integer.MAX_VALUE, props);
        int flightBackpressureThresholdBytes = getInt("flightBackpressureThresholdBytes",
                "arrowflight.flight.backpressureThresholdBytes", 67_108_864, props);
        if (flightBackpressureThresholdBytes <= 0) {
            throw new IllegalArgumentException(
                    "flightBackpressureThresholdBytes must be positive: "
                            + flightBackpressureThresholdBytes);
        }
        long flightListenerReadyTimeoutMillis = getLong("flightListenerReadyTimeoutMs",
                "arrowflight.flight.listenerReadyTimeoutMs", 300_000L, props);
        if (flightListenerReadyTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "flightListenerReadyTimeoutMs must be positive: "
                            + flightListenerReadyTimeoutMillis);
        }
        String dataDir = getString("dataDir", null, "/data/parquet", props);
        String localDataDir = getStringWithEnv("localDataDir", "arrowflight.localDataDir",
                "FLIGHT_LOCAL_DATA_DIR", null, props);
        int port = getInt("port", null, 32010, props);
        int hazelcastPort = getInt("hazelcastPort", null, 5701, props);
        int hazelcastClusterJoinTimeoutSec = getInt("hazelcastClusterJoinTimeoutSec",
                "arrowflight.hazelcast.clusterJoinTimeoutSec", 60, props);
        int clientMaxRetries = getInt("client.maxRetries", null, 3, props);
        int clientRetryBackoffMs = getInt("client.retryBackoffMs", null, 1000, props);
        int clientConnectTimeoutMs = getInt("client.connectTimeoutMs", null, 0, props);

        return new AppConfig(
                numServers, batchSize, ioParallelism, ioFileBufferSize,
                duckDbWarmConnections, duckDbGroups, duckDbThreads,
                duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes,
                duckDbHdfsShortcircuit, duckDbHdfsDomainSocketPath,
                metricsEnabled,
                grpcMaxInboundMessageSize, flightBackpressureThresholdBytes,
                flightListenerReadyTimeoutMillis,
                dataDir, localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs);
    }

    /**
     * Computes I/O thread pool parallelism from config, cores, and constraints.
     *
     * @param props loaded properties
     * @return thread count (clamped to 64)
     */
    private static int computeIoParallelism(Properties props) {
        Integer explicit = getOptionalInt("ioParallelism", "arrowflight.io.parallelism", props);
        if (explicit != null && explicit > 0) {
            return Math.min(explicit, 64);
        }
        int availableCores = Runtime.getRuntime().availableProcessors();
        int maxCores = getInt("ioParallelismMaxCores", "arrowflight.io.maxCores", 8, props);
        int effectiveCores = maxCores > 0 ? Math.min(availableCores, maxCores) : availableCores;
        int multiplier = getInt("ioParallelismMultiplier", "arrowflight.io.parallelismMultiplier", 8, props);
        int minThreads = getInt("ioParallelismMinThreads", "arrowflight.io.minParallelism", 32, props);
        return Math.min(64, Math.max(minThreads, effectiveCores * multiplier));
    }

    /**
     * Reads a string config from system property or properties file.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static String getString(String key, String sysAlias, String fallback, Properties props) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = System.getProperty(sysAlias);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = props.getProperty(sysAlias);
        }
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    /**
     * Reads a string config with environment variable fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static String getStringWithEnv(String key, String sysAlias, String envName,
            String fallback, Properties props) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = System.getProperty(sysAlias);
        }
        if (value == null || value.isBlank()) {
            value = props.getProperty(sysAlias);
        }
        if (value == null || value.isBlank()) {
            String env = System.getenv(envName);
            if (env != null && !env.isBlank()) {
                value = env;
            }
        }
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    /**
     * Reads an integer config with fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static int getInt(String key, String sysAlias, int fallback, Properties props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + raw, e);
        }
    }

    /**
     * Reads an optional integer config, returns null if not set.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param props    loaded properties
     * @return integer value or null
     */
    private static Integer getOptionalInt(String key, String sysAlias, Properties props) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = System.getProperty(sysAlias);
        }
        if ((value == null || value.isBlank()) && sysAlias != null) {
            value = props.getProperty(sysAlias);
        }
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + value, e);
        }
    }

    /**
     * Reads a boolean config with environment variable fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static boolean getBooleanWithEnv(String key, String sysAlias, String envName,
            boolean fallback, Properties props) {
        String value = getStringWithEnv(key, sysAlias, envName, null, props);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    /**
     * Reads a long config with fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary system property key
     * @param fallback default value
     * @param props    loaded properties
     * @return resolved value
     */
    private static long getLong(String key, String sysAlias, long fallback, Properties props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for " + key + ": " + raw, e);
        }
    }

    /**
     * Loads properties from classpath resource.
     *
     * @return loaded Properties
     */
    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = ConfigAdapter.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        return properties;
    }
}
