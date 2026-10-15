package pro.surpin.data.arrowflight.server;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/** Enforces Ranger schema and table policies for authenticated Flight users. */
public final class RangerAuthorization implements FlightAuthorization {
    private static final Logger LOGGER = LoggerFactory.getLogger(RangerAuthorization.class);
    private static final String SERVICE_TYPE = "arrowflight";
    private final RangerBasePlugin plugin;
    private final Function<String, Set<String>> groups;

    /**
     * Creates an authorization gate around a Ranger plugin.
     *
     * @param plugin initialized Ranger plugin
     * @param groups user group resolver
     */
    RangerAuthorization(RangerBasePlugin plugin, Function<String, Set<String>> groups) {
        this.plugin = plugin;
        this.groups = groups;
    }

    /**
     * Creates the optional Ranger plugin from deployment environment.
     *
     * @return authorization gate
     */
    public static RangerAuthorization fromEnvironment() {
        if (!System.getenv().containsKey("RANGER_SERVICE_NAME")) {
            return disabled();
        }
        return fromSettings(System.getenv());
    }

    /**
     * Creates a compatibility policy that allows access when Ranger is disabled.
     *
     * @return unrestricted policy
     */
    public static RangerAuthorization disabled() {
        return new RangerAuthorization(null, principal -> Set.of());
    }

    /**
     * Creates a Ranger plugin from deployment settings.
     *
     * @param environment deployment settings
     * @return Ranger authorization provider
     */
    static RangerAuthorization fromSettings(Map<String, String> environment) {
        String serviceName = environment.get("RANGER_SERVICE_NAME");
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException("RANGER_SERVICE_NAME is required for Ranger policies");
        }
        String adminUrl = environment.get("RANGER_ADMIN_URL");
        if (adminUrl == null || adminUrl.isBlank()) {
            throw new IllegalArgumentException("RANGER_ADMIN_URL is required with RANGER_SERVICE_NAME");
        }
        Properties properties = new Properties();
        String prefix = "ranger.plugin." + SERVICE_TYPE;
        properties.setProperty(prefix + ".service.name", serviceName);
        properties.setProperty(prefix + ".policy.rest.url", adminUrl);
        properties.setProperty(prefix + ".policy.pollIntervalMs", "30000");
        String cacheDir = environment.get("RANGER_POLICY_CACHE_DIR");
        if (cacheDir != null && !cacheDir.isBlank()) {
            properties.setProperty(prefix + ".policy.cache.dir", cacheDir);
        }
        RangerPluginConfig config = new RangerPluginConfig(
                SERVICE_TYPE, serviceName, "arrow-flight-server", properties);
        RangerBasePlugin plugin = new RangerBasePlugin(config);
        plugin.setResultProcessor(new RangerDefaultAuditHandler());
        plugin.init();
        return new RangerAuthorization(plugin, principal -> Set.copyOf(
                UserGroupInformation.createRemoteUser(principal).getGroups()));
    }

    /**
     * Checks whether a user may discover a schema.
     *
     * @param principal authenticated Kerberos principal
     * @param schema schema name
     * @return whether Ranger grants schema use
     */
    public boolean canSeeSchema(String principal, String schema) {
        return allowed(principal, schema, null, "use");
    }

    /**
     * Checks whether a user may select a table and use its schema.
     *
     * @param principal authenticated Kerberos principal
     * @param schema schema name
     * @param table table name
     * @return whether Ranger grants both permissions
     */
    public boolean canReadTable(String principal, String schema, String table) {
        return canSeeSchema(principal, schema)
                && allowed(principal, schema, table, "select");
    }

    /**
     * Requires access to every table named by a SQL query.
     *
     * @param principal authenticated Kerberos principal
     * @param query SQL query
     */
    public void requireQuery(String principal, String query) {
        if (plugin == null) {
            return;
        }
        FlightAuthorization.super.requireQuery(principal, query);
    }

    /**
     * Evaluates one Ranger access request and denies unavailable policy data.
     *
     * @param principal authenticated Kerberos principal
     * @param schema schema name
     * @param table optional table name
     * @param access Ranger access type
     * @return whether access is allowed
     */
    private boolean allowed(String principal, String schema, String table, String access) {
        if (plugin == null) {
            return true;
        }
        if (principal == null || principal.isBlank() || schema == null || schema.isBlank()
                || (table != null && table.isBlank())) {
            return false;
        }
        try {
            if (plugin.getServiceDef() == null) {
                return false;
            }
            UserGroupInformation user = UserGroupInformation.createRemoteUser(principal);
            RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
            resource.setValue("schema", schema);
            if (table != null) {
                resource.setValue("table", table);
            }
            RangerAccessRequestImpl request = new RangerAccessRequestImpl();
            request.setResource(resource);
            request.setAccessType(access);
            request.setUser(user.getShortUserName());
            request.setUserGroups(groups.apply(principal));
            RangerAccessResult result = plugin.isAccessAllowed(request);
            return result != null && result.getIsAllowed();
        } catch (RuntimeException e) {
            LOGGER.warn("Ranger authorization failed closed for schema={} table={}",
                    schema, table, e);
            return false;
        }
    }

    @Override
    public void close() {
        if (plugin != null) {
            plugin.cleanup();
        }
    }
}
