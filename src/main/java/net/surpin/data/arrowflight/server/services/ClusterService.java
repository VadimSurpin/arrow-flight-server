package net.surpin.data.arrowflight.server.services;

import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;

import net.surpin.data.arrowflight.server.LogUtil;
import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import net.surpin.data.arrowflight.server.model.HandleState;

/**
 * Manages server registration, heartbeats, load tracking, and statement cache lifecycle.
 * Coordinates with other cluster members via Hazelcast distributed maps.
 */
public final class ClusterService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

    private static final long HEARTBEAT_INTERVAL_SEC = 15;
    private static final long HEARTBEAT_TIMEOUT_SEC = 45;
    private static final long HANDLE_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);
    private static final int TICKET_SECRET_BYTES = 32;
    private static final String TICKET_SECRET_KEY = "__arrowflight-ticket-secret-v1";
    private static final String TICKET_LOAD_PREFIX = "__arrowflight-ticket-load-v1:";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final HazelcastAdapter hazelcast;
    private final AppConfig appConfig;
    private final String serverUri;
    private final ScheduledExecutorService heartbeatExecutor;
    private final EndpointTicketCodec endpointTicketCodec;
    private final Map<String, LocalHandle> localHandles = new ConcurrentHashMap<>();

    /**
     * Associates locally cached state with its expiration deadline.
     *
     * @param state handle state
     * @param expiresAtNanos monotonic expiration deadline
     */
    private record LocalHandle(HandleState state, long expiresAtNanos) {
    }

    /**
     * Creates a ClusterService for the given server URI.
     * Registers the server in the cluster and starts periodic heartbeat.
     *
     * @param hazelcast  Hazelcast adapter
     * @param appConfig  server configuration
     * @param serverUri  this server's URI string
     */
    public ClusterService(HazelcastAdapter hazelcast, AppConfig appConfig, String serverUri) {
        this.hazelcast = hazelcast;
        this.appConfig = appConfig;
        this.serverUri = serverUri;
        this.endpointTicketCodec = new EndpointTicketCodec(ticketSecret(hazelcast));

        hazelcast.serverRegistry().put(serverUri, 0L);
        hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());

        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flight-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                hazelcast.serverRegistry().putIfAbsent(serverUri, 0L);
                hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
                removeExpiredLocalEntries();
            } catch (Exception e) {
                LOGGER.warn("Failed to update heartbeat for {}: {}", serverUri, e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);

        hazelcast.onStatementExpired((EntryExpiredListener<String, Serializable>) event -> {
            Serializable value = event.getOldValue();
            if (value instanceof HandleState state && state.serverUri() != null
                    && state.loadTracked()) {
                hazelcast.serverRegistry().compute(state.serverUri(), (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    long updated = v - state.bytes();
                    return updated <= 0 ? 0L : updated;
                });
            }
        });
    }

    /**
     * Filters out servers that haven't sent heartbeat within timeout.
     *
     * @param serverUris set of URIs to check
     * @return set of live server URIs
     */
    public Set<String> filterLiveServers(Set<String> serverUris) {
        long t = LogUtil.mark();
        long now = System.currentTimeMillis();
        long deadline = now - HEARTBEAT_TIMEOUT_SEC * 1000;

        Map<String, Long> heartbeats = hazelcast.serverHeartbeats().getAll(serverUris);
        Set<String> live = new LinkedHashSet<>();
        for (String uri : serverUris) {
            Long lastHb = heartbeats.get(uri);
            if (lastHb == null) {
                // Registration and the first heartbeat are separate distributed-map writes.
                // Keep a newly registered node eligible during that short window.
                live.add(uri);
            } else if (lastHb >= deadline) {
                live.add(uri);
            } else {
                LOGGER.warn("Server {} is stale (last heartbeat {}ms ago), removing from pool",
                        uri, now - lastHb);
                hazelcast.serverRegistry().remove(uri);
                hazelcast.serverHeartbeats().remove(uri);
            }
        }
        LogUtil.logTiming(t, "hazelcast.filterLiveServers", "total=" + serverUris.size() + " live=" + live.size());
        return live;
    }

    /**
     * Returns all registered server URIs and their current loads.
     *
     * @return map of URI to load (bytes)
     */
    public Map<String, Long> allServerLoads() {
        long t = LogUtil.mark();
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : hazelcast.serverRegistry().entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        LogUtil.logTiming(t, "hazelcast.allServerLoads", "servers=" + result.size());
        return result;
    }

    /**
     * Returns load for a specific set of server URIs.
     *
     * @param serverUris URIs to query
     * @return map of URI to load
     */
    public Map<String, Long> getLoads(Set<String> serverUris) {
        return hazelcast.serverRegistry().getAll(serverUris);
    }

    /**
     * Publishes the files stored on this node. Every path is relative to the node's
     * configured data directory.
     *
     * @param files relative file path to byte size
     */
    public void registerLocalFiles(Map<String, Long> files) {
        hazelcast.serverFiles().put(this.serverUri, new LinkedHashMap<>(files));
        LOGGER.info("Registered {} local Parquet file(s) for {}", files.size(), this.serverUri);
    }

    /**
     * Merges all published node inventories into planner file assignments.
     * Duplicate relative paths are treated as replicas and retain every owner.
     *
     * @return relative path to size and owning servers
     */
    public Map<String, FileAssignment> fileLocations() {
        long t = LogUtil.mark();
        Map<String, Map<String, Long>> inventories = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Long>> entry : hazelcast.serverFiles().entrySet()) {
            inventories.put(entry.getKey(), entry.getValue());
        }
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Long>> server : inventories.entrySet()) {
            String serverAddress = server.getKey();
            for (Map.Entry<String, Long> file : server.getValue().entrySet()) {
                result.compute(file.getKey(), (path, current) -> {
                    Set<String> owners = new LinkedHashSet<>();
                    long size = file.getValue();
                    if (current != null) {
                        if (current.size() != size) {
                            throw new IllegalStateException(
                                    "Conflicting sizes for replicated file " + path);
                        }
                        owners.addAll(current.hosts());
                    }
                    owners.add(serverAddress);
                    return new FileAssignment(size, owners);
                });
            }
        }
        LogUtil.logTiming(t, "hazelcast.fileLocations", "files=" + result.size());
        return result;
    }

    /**
     * Checks whether a server published its local inventory, including an empty one.
     */
    public boolean hasFileInventory(String serverUri) {
        return hazelcast.serverFiles().containsKey(serverUri);
    }

    /**
     * Adds delta bytes to server's tracked load.
     *
     * @param uri   server URI
     * @param delta bytes to add (positive or negative)
     */
    public void addLoad(String uri, long delta) {
        long t = LogUtil.mark();
        hazelcast.serverRegistry().compute(uri, (k, v) -> {
            if (v == null) {
                return delta;
            }
            long updated = v + delta;
            return updated <= 0 ? 0L : updated;
        });
        LogUtil.logTiming(t, "hazelcast.addLoad", "uri=" + uri + " delta=" + delta);
    }

    /**
     * Stores a query handle in the distributed cache with 10-minute TTL.
     *
     * @param handle handle string
     * @param state  handle state
     */
    public void storeHandle(String handle, HandleState state) {
        long t = LogUtil.mark();
        localHandles.put(handle, new LocalHandle(
                state, System.nanoTime() + HANDLE_TTL_NANOS));
        hazelcast.statementCache().put(handle, state, 10, TimeUnit.MINUTES);
        LogUtil.logTiming(t, "hazelcast.storeHandle");
    }

    /**
     * Retrieves a handle state from the distributed cache.
     *
     * @param handle handle string
     * @return handle state, or null if not found
     */
    public HandleState getHandle(String handle) {
        long t = LogUtil.mark();
        LocalHandle local = localHandles.get(handle);
        if (local != null) {
            if (local.expiresAtNanos() > System.nanoTime()) {
                LogUtil.logTiming(t, "local.getHandle", "found=true");
                return local.state();
            }
            localHandles.remove(handle, local);
        }
        HandleState state = (HandleState) hazelcast.statementCache().get(handle);
        if (state != null) {
            localHandles.put(handle, new LocalHandle(
                    state, System.nanoTime() + HANDLE_TTL_NANOS));
        }
        LogUtil.logTiming(t, "hazelcast.getHandle", "found=" + (state != null));
        return state;
    }

    /**
     * Creates a signed self-contained handle for a server-directed Flight endpoint.
     *
     * @param state endpoint execution state
     * @return authenticated handle bytes
     */
    public byte[] createEndpointHandle(HandleState state) {
        byte[] handle = endpointTicketCodec.encode(state);
        if (state.loadTracked()) {
            hazelcast.statementCache().put(
                    loadReservationKey(handle), state, 10, TimeUnit.MINUTES);
        }
        return handle;
    }

    /**
     * Resolves a signed endpoint handle or a legacy distributed UUID handle.
     *
     * @param handle ticket statement-handle bytes
     * @return endpoint state, or null for an unknown legacy handle
     */
    public HandleState resolveEndpointHandle(byte[] handle) {
        if (endpointTicketCodec.isEncoded(handle)) {
            return endpointTicketCodec.decode(handle);
        }
        return getHandle(new String(handle, StandardCharsets.UTF_8));
    }

    /**
     * Releases distributed load once for a completed signed endpoint ticket.
     *
     * @param handle ticket statement-handle bytes
     * @param state completed endpoint state
     */
    public void releaseEndpointLoad(byte[] handle, HandleState state) {
        if (!state.loadTracked() || state.serverUri() == null || state.bytes() == 0L) {
            return;
        }
        Serializable reservation = hazelcast.statementCache()
                .remove(loadReservationKey(handle));
        if (reservation instanceof HandleState reserved) {
            addLoad(reserved.serverUri(), -reserved.bytes());
        }
    }

    /**
     * Removes a handle from the distributed cache.
     *
     * @param handle handle string
     */
    public void removeHandle(String handle) {
        localHandles.remove(handle);
        hazelcast.statementCache().remove(handle);
    }

    /**
     * Obtains the cluster-shared secret used to authenticate endpoint tickets.
     *
     * @param hazelcast Hazelcast adapter
     * @return cluster-shared secret bytes
     */
    private static byte[] ticketSecret(HazelcastAdapter hazelcast) {
        byte[] candidate = new byte[TICKET_SECRET_BYTES];
        SECURE_RANDOM.nextBytes(candidate);
        Serializable existing = hazelcast.statementCache()
                .putIfAbsent(TICKET_SECRET_KEY, candidate);
        if (existing == null) {
            return candidate;
        }
        if (existing instanceof byte[] secret && secret.length >= TICKET_SECRET_BYTES) {
            return secret.clone();
        }
        throw new IllegalStateException("Invalid cluster Flight ticket secret");
    }

    /**
     * Creates a compact stable key for a distributed load reservation.
     *
     * @param handle ticket statement-handle bytes
     * @return base64-encoded SHA-256 digest
     */
    private static String loadReservationKey(byte[] handle) {
        try {
            return TICKET_LOAD_PREFIX + Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-256").digest(handle));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Removes expired local handle and idempotency entries.
     */
    private void removeExpiredLocalEntries() {
        long now = System.nanoTime();
        localHandles.entrySet().removeIf(
                entry -> entry.getValue().expiresAtNanos() <= now);
    }

    /**
     * Returns this server's URI.
     *
     * @return server URI string
     */
    public String serverUri() {
        return serverUri;
    }

    /**
     * Returns the underlying Hazelcast instance for cluster management.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance getHazelcastInstance() {
        return hazelcast.instance();
    }

    @Override
    public void close() {
        try {
            hazelcast.serverRegistry().remove(serverUri);
            hazelcast.serverHeartbeats().remove(serverUri);
        } catch (Exception e) {
            LOGGER.warn("Failed to deregister server from cluster: {}", e.getMessage());
        }
        heartbeatExecutor.shutdownNow();
        localHandles.clear();
        try {
            hazelcast.close();
        } catch (Exception e) {
            LOGGER.error("Error closing Hazelcast adapter", e);
        }
    }
}
