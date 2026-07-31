package pro.surpin.data.arrowflight.server;

import dagger.Component;
import javax.inject.Singleton;

import org.apache.arrow.memory.BufferAllocator;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.services.ClusterService;

/**
 * Dagger component for the Flight server.
 * Provides singleton access to all services and the Flight SQL producer.
 */
@Singleton
@Component(modules = {ServerModule.class})
public interface ServerComponent {

    /**
     * Returns the Flight SQL producer for gRPC handling.
     *
     * @return producer instance
     */
    FlightSqlProducer producer();

    /**
     * Returns the cluster service for lifecycle management.
     *
     * @return cluster service
     */
    ClusterService clusterService();

    /**
     * Returns the DuckDB adapter for lifecycle management.
     *
     * @return duckdb adapter
     */
    DuckDbAdapter duckDb();

    /**
     * Returns the Arrow buffer allocator.
     *
     * @return allocator
     */
    BufferAllocator allocator();
}
