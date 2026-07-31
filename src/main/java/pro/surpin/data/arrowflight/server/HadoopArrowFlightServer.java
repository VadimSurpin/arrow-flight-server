package pro.surpin.data.arrowflight.server;

import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.metrics.MetricsService;
import pro.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Main entry point for the Hadoop Arrow Flight SQL server.
 * Uses Dagger for dependency injection and manages the Flight server lifecycle.
 */
public class HadoopArrowFlightServer {
    private static final String DEFAULT_LOG_LEVEL_PROPERTY = "org.slf4j.simpleLogger.defaultLogLevel";

    static {
        initLogLevel();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopArrowFlightServer.class);

    /**
     * Initializes SLF4J log level from properties, system properties, or env var.
     */
    private static void initLogLevel() {
        Properties props = new Properties();
        try (InputStream input = HadoopArrowFlightServer.class.getClassLoader()
                .getResourceAsStream("arrowflight.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        for (String key : new String[]{"logLevel", "arrowflight.log.level"}) {
            String v = System.getProperty(key);
            if (v != null && !v.isBlank()) {
                System.setProperty(DEFAULT_LOG_LEVEL_PROPERTY, v.trim());
                return;
            }
        }
        String env = System.getenv("LOGGING_LEVEL");
        if (env != null && !env.isBlank()) {
            System.setProperty(DEFAULT_LOG_LEVEL_PROPERTY, env.trim());
            return;
        }
        for (String key : new String[]{"logLevel", "arrowflight.log.level"}) {
            String v = props.getProperty(key);
            if (v != null && !v.isBlank()) {
                System.setProperty(DEFAULT_LOG_LEVEL_PROPERTY, v.trim());
                return;
            }
        }
    }

    private FlightServer server;
    private ServerComponent component;
    private MetricsService metricsService;

    /**
     * Starts the server with command-line arguments.
     *
     * @param args CLI arguments including data, Flight, cluster, and metrics settings
     */
    public void start(String... args) {
        AppConfig config = ConfigAdapter.getConfig();
        String dataDirectory = getArgValue(args, "--data-dir", config.dataDir());
        int port = Integer.parseInt(
                getArgValue(args, "--port", String.valueOf(config.port())));
        String hosts = getArgValue(args, "--hosts", "0.0.0.0");
        String localhost = getArgValue(args, "--localhost", "localhost");
        String storageHost = getArgValue(args, "--storage-host", localhost);
        int hazelcastPort = Integer.parseInt(
                getArgValue(args, "--hazelcast-port", String.valueOf(config.hazelcastPort())));
        int metricsPort = Integer.parseInt(getArgValue(args, "--metrics-port", "9404"));
        boolean metricsEnabled = Boolean.parseBoolean(getArgValue(args, "--metrics-enabled",
                String.valueOf(config.metricsEnabled())));

        // Generate default host list from numServers if --hosts not explicitly set
        String hostsRaw = getArgValue(args, "--hosts", null);
        if (hostsRaw == null && config.numServers() > 1) {
            String[] defaultHosts = new String[config.numServers()];
            for (int i = 0; i < defaultHosts.length; i++) {
                defaultHosts[i] = "flight-server-" + (i + 1);
            }
            hosts = String.join(",", defaultHosts);
            LOGGER.info("Generated --hosts from numServers={}: {}", config.numServers(), hosts);
        }

        // Push CLI overrides into system properties so ConfigAdapter picks them up
        System.setProperty("dataDir", dataDirectory);
        System.setProperty("port", String.valueOf(port));
        System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));

        LOGGER.info("Starting Hadoop Arrow Flight SQL server...");
        LOGGER.info("Data Directory: {}", dataDirectory);
        LOGGER.info("Hosts: {}", hosts);
        LOGGER.info("Local storage host: {}", storageHost);
        LOGGER.info("Port: {}", port);

        Configuration hadoopConfig = new Configuration();
        String[] hazelcastHosts = hosts.split(",");

        // Create Dagger component with runtime parameters
        component = DaggerServerComponent.builder()
                .serverModule(new ServerModule(hazelcastHosts,
                        Location.forGrpcInsecure(localhost, port).getUri().toString(),
                        storageHost, hadoopConfig))
                .build();

        // Wait for cluster nodes (single-node skip)
        if (hazelcastHosts.length > 1) {
            waitForCluster(hazelcastHosts, config.hazelcastClusterJoinTimeoutSec());
        }

        try {
            // Increase HTTP/2 initial flow control window from default 64KB to 1MB.
            // Allows server to send larger Arrow batches without waiting for WINDOW_UPDATE.
            System.setProperty("grpc.netty.server.flowControlWindow", "1048576");

            Location location = Location.forGrpcInsecure(localhost, port);
            server = FlightServer.builder(
                    component.allocator(),
                    location,
                    component.producer())
                    .maxInboundMessageSize(config.grpcMaxInboundMessageSize())
                    .backpressureThreshold(config.flightBackpressureThresholdBytes())
                    .build();
            server.start();

            LOGGER.info("Server Arrow Flight SQL started on {}", location);
            if (metricsEnabled) {
                metricsService = new MetricsService(metricsPort);
                metricsService.start();
                LOGGER.info("Prometheus metrics started on 0.0.0.0:{}", metricsPort);
            } else {
                LOGGER.info("Prometheus metrics are disabled");
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down server...");
                stop();
            }));

            server.awaitTermination();
        } catch (Exception e) {
            LOGGER.error("Error starting server", e);
            System.exit(1);
        }
    }

    /**
     * Stops the server and releases resources.
     */
    public void stop() {
        if (metricsService != null) {
            metricsService.close();
            metricsService = null;
            LOGGER.info("Prometheus metrics stopped");
        }
        if (server != null) {
            server.shutdown();
            LOGGER.info("Flight SQL server stopped");
        }
        if (component != null) {
            try {
                component.duckDb().close();
            } catch (Exception e) {
                LOGGER.error("Error closing DuckDB adapter", e);
            }
            try {
                component.clusterService().close();
            } catch (Exception e) {
                LOGGER.error("Error closing cluster service", e);
            }
            try {
                component.allocator().close();
            } catch (Exception e) {
                LOGGER.error("Error closing allocator", e);
            }
        }
    }

    /**
     * Waits for all Hazelcast cluster nodes to connect.
     * @param hosts expected host addresses
     * @param timeoutSec max wait time in seconds
     * @throws IllegalStateException if timeout reached or interrupted
     */
    private void waitForCluster(String[] hosts, int timeoutSec) {
        HazelcastInstance hazelcast = component.clusterService().getHazelcastInstance();
        long t = LogUtil.mark();
        ReentrantLock membershipLock = new ReentrantLock();
        Condition membershipChanged = membershipLock.newCondition();

        LOGGER.info("Waiting up to {}s for {} nodes to connect (hosts: {})",
                timeoutSec, hosts.length, Arrays.toString(hosts));

        java.util.UUID listenerId = hazelcast.getCluster().addMembershipListener(
                new MembershipAdapter() {
                    @Override
                    public void memberAdded(MembershipEvent e) {
                        membershipLock.lock();
                        try {
                            membershipChanged.signalAll();
                        } finally {
                            membershipLock.unlock();
                        }
                    }
                });

        try {
            boolean joined = awaitMemberCount(
                    () -> hazelcast.getCluster().getMembers().size(),
                    membershipLock, membershipChanged, hosts.length,
                    TimeUnit.SECONDS.toMillis(timeoutSec));
            if (!joined) {
                long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t);
                int connected = hazelcast.getCluster().getMembers().size();
                String msg = String.format(
                        "Cluster join timeout after %ds: only %d of %d nodes connected. "
                                + "Expected hosts: %s.",
                        elapsed, connected, hosts.length, Arrays.toString(hosts));
                LOGGER.error(msg);
                throw new IllegalStateException(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for cluster join", e);
        } finally {
            hazelcast.getCluster().removeMembershipListener(listenerId);
        }

        LogUtil.logTiming(t, "cluster.waitForMembers", "expected=" + hosts.length);
        long totalSec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t);
        LOGGER.info("All {} nodes connected in {}s. Initializing...", hosts.length, totalSec);
    }

    /**
     * Waits until a changing member count reaches the expected value.
     *
     * @param memberCount current member-count supplier
     * @param lock lock guarding membership-change signals
     * @param membershipChanged condition signalled after membership changes
     * @param expected expected member count
     * @param timeoutMillis maximum wait time in milliseconds
     * @return true when expected count is reached, false on timeout
     * @throws InterruptedException if waiting is interrupted
     * @throws IllegalArgumentException if expected count or timeout is not positive
     */
    static boolean awaitMemberCount(IntSupplier memberCount, ReentrantLock lock,
            Condition membershipChanged, int expected, long timeoutMillis)
            throws InterruptedException {
        if (expected <= 0 || timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Expected member count and timeout must be positive");
        }

        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        lock.lockInterruptibly();
        try {
            while (memberCount.getAsInt() < expected) {
                if (remainingNanos <= 0) {
                    return false;
                }
                remainingNanos = membershipChanged.awaitNanos(remainingNanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the value of a named CLI argument.
     * @param args CLI arguments array
     * @param key argument name
     * @param defaultValue default if not found
     * @return argument value or default
     */
    private String getArgValue(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(key) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    /**
     * Main entry point.
     *
     * @param args CLI arguments
     * @throws Exception on startup failure
     */
    public static void main(String... args) throws Exception {
        HadoopArrowFlightServer server = new HadoopArrowFlightServer();
        server.start(args);
    }
}
