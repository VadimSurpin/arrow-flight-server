package pro.surpin.data.arrowflight.server.model;

/**
 * Server configuration loaded at startup.
 * Values are resolved from arrowflight.properties, system properties, and environment variables.
 */
public record AppConfig(
    int numServers,
    int batchSize,
    int ioParallelism,
    int ioFileBufferSize,
    int duckDbWarmConnections,
    int duckDbGroups,
    int duckDbThreads,
    String duckDbHdfsExtension,
    boolean duckDbAllowUnsignedExtensions,
    String duckDbHdfsDefaultNamenode,
    String duckDbHdfsHaNamenodes,
    String duckDbHdfsShortcircuit,
    String duckDbHdfsDomainSocketPath,
    boolean metricsEnabled,
    int grpcMaxInboundMessageSize,
    int flightBackpressureThresholdBytes,
    long flightListenerReadyTimeoutMillis,
    String dataDir,
    String localDataDir,
    int port,
    int hazelcastPort,
    int hazelcastClusterJoinTimeoutSec,
    int clientMaxRetries,
    int clientRetryBackoffMs,
    int clientConnectTimeoutMs
) {
}
