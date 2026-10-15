package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;

public class TestFlightServerHelper implements AutoCloseable {

    public final BufferAllocator allocator;
    public final HazelcastAdapter hazelcastAdapter;
    public final ParquetAdapter parquetAdapter;
    public final DuckDbAdapter duckDbAdapter;
    public final MetadataService metadataService;
    public final ClusterService clusterService;
    public final QueryPlanner queryPlanner;
    public final ExecutionService executionService;
    public final FlightSqlProducer flightSqlProducer;
    public final FlightServer server;
    public final FlightClient flightClient;
    public final FlightSqlClient sqlClient;
    public final Location location;

    private final String[] savedProps;

    private TestFlightServerHelper(Builder b) throws Exception {
        savedProps = saveProps();

        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        allocator = b.allocator != null ? b.allocator : new RootAllocator(Long.MAX_VALUE);

        int hazelcastPort = b.hazelcastPort != 0 ? b.hazelcastPort : findFreePort();

        String defaultDataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        String dataDir = b.dataDir != null ? b.dataDir : defaultDataDir;

        System.setProperty("dataDir", dataDir);
        System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));
        System.setProperty("duckDbWarmConnections", "1");
        System.setProperty("duckDbGroups", "1");
        System.setProperty("duckDbThreads", "1");
        System.setProperty("ioParallelism", "2");
        AppConfig appConfig = ConfigAdapter.getConfig();

        // Local filesystem adapter
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());

        // Adapters
        parquetAdapter = new ParquetAdapter(appConfig, localFs);
        duckDbAdapter = new DuckDbAdapter(appConfig, Executors.newCachedThreadPool());

        // Services
        metadataService = new MetadataService(parquetAdapter);

        hazelcastAdapter = new HazelcastAdapter(appConfig);
        // Pick the Flight port only after Hazelcast owns its port. Otherwise two
        // consecutive ephemeral-port probes can return the same released port.
        int flightPort = b.flightPort != 0 ? b.flightPort : findFreePort();
        location = Location.forGrpcInsecure("localhost", flightPort);
        clusterService = new ClusterService(hazelcastAdapter, appConfig,
                location.getUri().toString());
        queryPlanner = new QueryPlanner(parquetAdapter, clusterService);

        executionService = new ExecutionService(parquetAdapter, duckDbAdapter,
                metadataService, appConfig,
                Executors.newCachedThreadPool());

        flightSqlProducer = new FlightSqlProducer(location, allocator,
                metadataService, queryPlanner, executionService, clusterService);

        server = FlightServer.builder(allocator, location, flightSqlProducer).build();
        server.start();

        flightClient = FlightClient.builder(allocator, location).build();
        sqlClient = new FlightSqlClient(flightClient);
    }

    public FlightSqlClient sqlClient() {
        return sqlClient;
    }

    public FlightClient flightClient() {
        return flightClient;
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        try { if (sqlClient != null) sqlClient.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (flightClient != null) flightClient.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (server != null) server.shutdown(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (flightSqlProducer != null) flightSqlProducer.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (clusterService != null) clusterService.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (hazelcastAdapter != null) hazelcastAdapter.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (allocator != null) allocator.close(); }
        catch (Exception e) { if (first == null) first = e; }
        restoreProps(savedProps);
        if (first != null) throw first;
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private BufferAllocator allocator;
        private String dataDir;
        private int hazelcastPort;
        private int flightPort;

        public Builder allocator(BufferAllocator a) { this.allocator = a; return this; }
        public Builder dataDir(String d) { this.dataDir = d; return this; }
        public Builder hazelcastPort(int p) { this.hazelcastPort = p; return this; }
        public Builder flightPort(int p) { this.flightPort = p; return this; }

        public TestFlightServerHelper start() throws Exception {
            return new TestFlightServerHelper(this);
        }
    }

    private static String[] saveProps() {
        return new String[] {
                System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME),
                System.getProperty("dataDir"),
                System.getProperty("hazelcastPort"),
                System.getProperty("duckDbWarmConnections"),
                System.getProperty("duckDbGroups"),
                System.getProperty("duckDbThreads"),
                System.getProperty("ioParallelism")
        };
    }

    private static void restoreProps(String[] saved) {
        String[] keys = {
                ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                "dataDir", "hazelcastPort",
                "duckDbWarmConnections", "duckDbGroups", "duckDbThreads",
                "ioParallelism"
        };
        for (int i = 0; i < keys.length; i++) {
            if (saved[i] != null) {
                System.setProperty(keys[i], saved[i]);
            } else {
                System.clearProperty(keys[i]);
            }
        }
    }
}
