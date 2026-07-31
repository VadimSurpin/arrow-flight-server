package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Tests system-property and environment configuration mapping. */
@Tag("unit")
class ConfigAdapterTest {

    private static final Set<String> CLEARED = new HashSet<>();

    @AfterEach
    void clearSystemProps() {
        for (String key : CLEARED) {
            System.clearProperty(key);
        }
        CLEARED.clear();
    }

    private static void setProp(String key, String value) {
        System.setProperty(key, value);
        CLEARED.add(key);
    }

    @Test
    void getConfigDefaults() {
        AppConfig cfg = ConfigAdapter.getConfig();

        assertEquals(6, cfg.numServers());
        assertEquals(65536, cfg.batchSize());
        assertEquals(1048576, cfg.ioFileBufferSize());
        assertEquals(32, cfg.ioParallelism());
        assertEquals(1, cfg.duckDbThreads());
        assertEquals("/data/parquet", cfg.dataDir());
        assertNull(cfg.localDataDir());
        assertEquals(32010, cfg.port());
        assertEquals(5701, cfg.hazelcastPort());
        assertEquals(60, cfg.hazelcastClusterJoinTimeoutSec());
        assertEquals(3, cfg.clientMaxRetries());
        assertEquals(1000, cfg.clientRetryBackoffMs());
        assertEquals(0, cfg.clientConnectTimeoutMs());
        assertFalse(cfg.duckDbAllowUnsignedExtensions());
        assertTrue(cfg.metricsEnabled());
        assertEquals(Integer.MAX_VALUE, cfg.grpcMaxInboundMessageSize());
        assertEquals(67108864, cfg.flightBackpressureThresholdBytes());
        assertEquals(300000, cfg.flightListenerReadyTimeoutMillis());
    }

    @Test
    void getConfigNumServersViaSysProp() {
        setProp("numServers", "10");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(10, cfg.numServers());
    }

    @Test
    void getConfigNumServersDefaults() {
        // No explicit property set, should use arrowflight.properties.
        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(6, cfg.numServers());
    }

    @Test
    void getConfigBatchSizeViaSysProp() {
        setProp("batchSize", "8192");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(8192, cfg.batchSize());
    }

    @Test
    void getConfigSysPropTakesPrecedenceOverPropsFile() {
        // The system property must override arrowflight.properties.
        // System property must override it.
        setProp("batchSize", "100");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(100, cfg.batchSize());
    }

    @Test
    void getConfigIoParallelismExplicit() {
        setProp("ioParallelism", "8");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(8, cfg.ioParallelism());
    }

    @Test
    void getConfigIoParallelismClamped() {
        setProp("ioParallelism", "999");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(64, cfg.ioParallelism());
    }

    @Test
    void getConfigDuckDbThreads() {
        setProp("duckDbThreads", "4");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(4, cfg.duckDbThreads());
    }

    /** Verifies DuckDB HDFS extension settings map from system properties. */
    @Test
    void getConfigDuckDbHdfsExtension() {
        setProp("arrowflight.duckdb.hdfs.extension", "/opt/duckdb-hdfs/hadoopfs.duckdb_extension");
        setProp("arrowflight.duckdb.allowUnsignedExtensions", "true");

        AppConfig cfg = ConfigAdapter.getConfig();

        assertEquals("/opt/duckdb-hdfs/hadoopfs.duckdb_extension",
                cfg.duckDbHdfsExtension());
        assertTrue(cfg.duckDbAllowUnsignedExtensions());
    }

    @Test
    void getConfigHazelcastJoinTimeout() {
        setProp("hazelcastClusterJoinTimeoutSec", "30");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(30, cfg.hazelcastClusterJoinTimeoutSec());
    }

    @Test
    void getConfigHazelcastJoinTimeoutViaAlias() {
        setProp("arrowflight.hazelcast.clusterJoinTimeoutSec", "120");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(120, cfg.hazelcastClusterJoinTimeoutSec());
    }

    @Test
    void getConfigClientSettings() {
        setProp("client.maxRetries", "5");
        setProp("client.retryBackoffMs", "2000");
        setProp("client.connectTimeoutMs", "60000");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(5, cfg.clientMaxRetries());
        assertEquals(2000, cfg.clientRetryBackoffMs());
        assertEquals(60000, cfg.clientConnectTimeoutMs());
    }

    @Test
    void getConfigDataDir() {
        setProp("dataDir", "/custom/data/path");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals("/custom/data/path", cfg.dataDir());
    }

    @Test
    void getConfigLocalDataDir() {
        setProp("localDataDir", "/staging");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals("/staging", cfg.localDataDir());
    }

    @Test
    void getConfigPort() {
        setProp("port", "32020");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(32020, cfg.port());
    }

    @Test
    void getConfigHazelcastPort() {
        setProp("hazelcastPort", "5702");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(5702, cfg.hazelcastPort());
    }

    @Test
    void getConfigGrpcMaxInbound() {
        setProp("grpcMaxInboundMessageSize", "1048576");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(1048576, cfg.grpcMaxInboundMessageSize());
    }

    /** Verifies the Flight outbound backpressure budget is configurable. */
    @Test
    void getConfigFlightBackpressureThreshold() {
        setProp("flightBackpressureThresholdBytes", "33554432");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(33554432, cfg.flightBackpressureThresholdBytes());
    }

    /** Verifies a non-positive Flight outbound backpressure budget is rejected. */
    @Test
    void getConfigRejectsNonPositiveFlightBackpressureThreshold() {
        setProp("flightBackpressureThresholdBytes", "0");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, ConfigAdapter::getConfig);
        assertTrue(error.getMessage().contains("flightBackpressureThresholdBytes"));
    }

    /** Verifies benchmark metrics can be disabled with a system property. */
    @Test
    void getConfigMetricsDisabled() {
        setProp("metricsEnabled", "false");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertFalse(cfg.metricsEnabled());
    }

    @Test
    void getConfigFlightListenerReady() {
        setProp("flightListenerReadyTimeoutMs", "30000");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(30000, cfg.flightListenerReadyTimeoutMillis());
    }

    /** Verifies invalid readiness timeout configuration is rejected at startup. */
    @Test
    void getConfigRejectsNonPositiveFlightListenerReadyTimeout() {
        setProp("flightListenerReadyTimeoutMs", "0");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, ConfigAdapter::getConfig);
        assertTrue(error.getMessage().contains("flightListenerReadyTimeoutMs"));
    }

    @Test
    void getConfigIoFileBuffer() {
        setProp("ioFileBufferSize", "65536");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(65536, cfg.ioFileBufferSize());
    }

    @Test
    void getConfigInvalidIntThrows() {
        setProp("batchSize", "not-a-number");

        assertThrows(IllegalArgumentException.class, ConfigAdapter::getConfig);
    }
}
