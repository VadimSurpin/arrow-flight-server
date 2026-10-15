package pro.surpin.data.arrowflight.server.adapters;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

import pro.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Wraps HazelcastInstance lifecycle and distributed map access.
 */
public final class HazelcastAdapter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastAdapter.class);

    private static final String SERVER_REGISTRY_MAP = "server-registry";
    private static final String STATEMENT_CACHE_MAP = "statement-cache";
    private static final String SERVER_HEARTBEAT_MAP = "server-heartbeats";
    private static final String SERVER_FILES_MAP = "server-files";

    private final HazelcastInstance instance;
    private final IMap<String, Long> serverRegistry;
    private final IMap<String, Serializable> statementCache;
    private final IMap<String, Long> serverHeartbeats;
    private final IMap<String, Map<String, Long>> serverFiles;

    /**
     * Creates a new Hazelcast instance using TCP/IP join on the given hosts.
     *
     * @param appConfig server configuration
     * @param hosts cluster seed hosts
     */
    public HazelcastAdapter(AppConfig appConfig, String... hosts) {
        Config config = new Config();
        NetworkConfig network = config.getNetworkConfig();
        network.setPort(appConfig.hazelcastPort());
        network.setPortAutoIncrement(false);
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (String host : hosts) {
            tcpIpConfig.addMember(host);
        }
        tcpIpConfig.setEnabled(true);

        this.instance = Hazelcast.newHazelcastInstance(config);
        this.serverRegistry = instance.getMap(SERVER_REGISTRY_MAP);
        this.statementCache = instance.getMap(STATEMENT_CACHE_MAP);
        this.serverHeartbeats = instance.getMap(SERVER_HEARTBEAT_MAP);
        this.serverFiles = instance.getMap(SERVER_FILES_MAP);
    }

    /**
     * Returns the distributed server registry (URI → load).
     */
    public IMap<String, Long> serverRegistry() {
        return serverRegistry;
    }

    /**
     * Returns the distributed statement cache (handle → state).
     */
    public IMap<String, Serializable> statementCache() {
        return statementCache;
    }

    /**
     * Returns the distributed heartbeat map (URI → timestamp).
     */
    public IMap<String, Long> serverHeartbeats() {
        return serverHeartbeats;
    }

    /**
     * Returns the distributed local-file inventory (server URI → relative path/size).
     */
    public IMap<String, Map<String, Long>> serverFiles() {
        return serverFiles;
    }

    /**
     * Registers an entry-expired listener on the statement cache.
     *
     * @param listener the listener to attach
     */
    public void onStatementExpired(EntryExpiredListener<String, Serializable> listener) {
        statementCache.addLocalEntryListener(listener);
    }

    /**
     * Returns the underlying HazelcastInstance for advanced operations.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance instance() {
        return instance;
    }

    @Override
    public void close() {
        instance.shutdown();
        LOGGER.info("Hazelcast instance shut down");
    }
}
