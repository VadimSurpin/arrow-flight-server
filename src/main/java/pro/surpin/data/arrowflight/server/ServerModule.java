package pro.surpin.data.arrowflight.server;

import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;

/**
 * Dagger module that wires all server components as singletons.
 * Runtime parameters (hosts, Hadoop config) are passed via constructor.
 */
@Module
public final class ServerModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerModule.class);

    private final String[] hazelcastHosts;
    private final String serverUri;
    private final String localStorageHost;
    private final Configuration hadoopConfig;

    /**
     * Creates module with runtime parameters.
     *
     * @param hazelcastHosts cluster seed hosts
     * @param serverUri      this server's URI
     * @param localStorageHost HDFS DataNode host colocated with this server
     * @param hadoopConfig   Hadoop configuration
     */
    public ServerModule(String[] hazelcastHosts, String serverUri,
            String localStorageHost, Configuration hadoopConfig) {
        this.hazelcastHosts = hazelcastHosts;
        this.serverUri = serverUri;
        this.localStorageHost = localStorageHost;
        this.hadoopConfig = hadoopConfig;
    }

    /**
     * Provide AppConfig singleton
     * @return application configuration
     */
    @Provides
    @Singleton
    AppConfig config() {
        System.setProperty(
            org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
            org.apache.arrow.memory.DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        return ConfigAdapter.getConfig();
    }

    /**
     * Provide HazelcastAdapter singleton
     * @param config application configuration
     * @return Hazelcast adapter
     */
    @Provides
    @Singleton
    HazelcastAdapter hazelcast(AppConfig config) {
        return new HazelcastAdapter(config, hazelcastHosts);
    }

    /**
     * Provide ClusterService singleton
     * @param hazelcast Hazelcast adapter
     * @param config application configuration
     * @return cluster service
     */
    @Provides
    @Singleton
    ClusterService cluster(HazelcastAdapter hazelcast, AppConfig config) {
        return new ClusterService(hazelcast, config, serverUri);
    }

    /**
     * Provide I/O thread pool singleton
     * @param config application configuration
     * @return executor service
     */
    @Provides
    @Singleton
    ExecutorService ioPool(AppConfig config) {
        int parallelism = config.ioParallelism();
        LOGGER.info("node={} pool=create threads={}", LogUtil.node(), parallelism);
        return new java.util.concurrent.AbstractExecutorService() {
            private final ExecutorService delegate = Executors.newFixedThreadPool(parallelism, r -> {
                Thread t = new Thread(r, "parquet-io");
                t.setDaemon(true);
                return t;
            });
            private final java.util.concurrent.atomic.AtomicInteger activeTasks = new java.util.concurrent.atomic.AtomicInteger(0);
            private final java.util.concurrent.atomic.AtomicLong submittedTasks = new java.util.concurrent.atomic.AtomicLong(0);

            @Override
            public void execute(Runnable command) {
                long submitTime = System.nanoTime();
                long taskId = submittedTasks.incrementAndGet();
                int active = activeTasks.incrementAndGet();
                if (active > parallelism) {
                    LOGGER.warn("node={} pool=queueGrowth active={} max={} queued={}",
                            LogUtil.node(), active, parallelism, active - parallelism);
                }
                delegate.execute(() -> {
                    long queueDelay = System.nanoTime() - submitTime;
                    if (queueDelay > 1_000_000_000L) {
                        LOGGER.warn("qid={} node={} pool=starvation taskId={} queueDelay={} active={}",
                                LogUtil.qid(), LogUtil.node(), taskId,
                                LogUtil.elapsedNanos(submitTime), activeTasks.get());
                    }
                    try {
                        command.run();
                    } finally {
                        activeTasks.decrementAndGet();
                    }
                });
            }

            @Override
            public void shutdown() {
                delegate.shutdown();
            }
            @Override
            public List<Runnable> shutdownNow() {
                return delegate.shutdownNow();
            }
            @Override
            public boolean isShutdown() {
                return delegate.isShutdown();
            }
            @Override
            public boolean isTerminated() {
                return delegate.isTerminated();
            }
            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return delegate.awaitTermination(timeout, unit);
            }
        };
    }

    /**
     * Provide Hadoop FileSystem singleton
     * @param config application configuration
     * @return Hadoop file system
     */
    @Provides
    @Singleton
    FileSystem fileSystem(AppConfig config) {
        hadoopConfig.setInt("io.file.buffer.size", config.ioFileBufferSize());
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit", true);
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit.skip.checksum", false);
        hadoopConfig.setBoolean("fs.file.impl.disable.cache", true);
        try {
            return new Path(config.dataDir()).getFileSystem(hadoopConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Provide ParquetAdapter singleton
     * @param config application configuration
     * @param fs Hadoop file system
     * @return parquet adapter
     */
    @Provides
    @Singleton
    ParquetAdapter parquetAdapter(AppConfig config, FileSystem fs) {
        return new ParquetAdapter(config, fs, localStorageHost);
    }

    /**
     * Provide DuckDbAdapter singleton
     * @param config application configuration
     * @param ioPool I/O thread pool
     * @return DuckDB adapter
     */
    @Provides
    @Singleton
    DuckDbAdapter duckDb(AppConfig config, ExecutorService ioPool) {
        return new DuckDbAdapter(config, ioPool);
    }

    /**
     * Provide MetadataService singleton
     * @param parquetAdapter parquet adapter
     * @return metadata service
     */
    @Provides
    @Singleton
    MetadataService metadata(ParquetAdapter parquetAdapter) {
        return new MetadataService(parquetAdapter);
    }

    /**
     * Provide QueryPlanner singleton
     * @param parquetAdapter parquet adapter
     * @param clusterService cluster service
     * @return query planner
     */
    @Provides
    @Singleton
    QueryPlanner queryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService) {
        return new QueryPlanner(parquetAdapter, clusterService);
    }

    /**
     * Provide BufferAllocator singleton
     * @return buffer allocator
     */
    @Provides
    @Singleton
    BufferAllocator allocator() {
        return new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Provide FlightSqlProducer singleton
     * @param config application configuration
     * @param allocator buffer allocator
     * @param metadataService metadata service
     * @param queryPlanner query planner
     * @param executionService execution service
     * @param clusterService cluster service
     * @return Flight SQL producer
     */
    @Provides
    @Singleton
    FlightSqlProducer producer(AppConfig config, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService) {
        int port = config.port();
        String localhost = "0.0.0.0";
        Location location = Location.forGrpcInsecure(localhost, port);
        return new FlightSqlProducer(location, allocator, metadataService,
                queryPlanner, executionService, clusterService);
    }

    /**
     * Provide ExecutionService singleton
     * @param parquetAdapter parquet adapter
     * @param duckDbAdapter DuckDB adapter
     * @param metadataService metadata service
     * @param appConfig application configuration
     * @param ioPool I/O thread pool
     * @return execution service
     */
    @Provides
    @Singleton
    ExecutionService execution(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool) {
        return new ExecutionService(parquetAdapter, duckDbAdapter,
                metadataService, appConfig, ioPool);
    }
}
