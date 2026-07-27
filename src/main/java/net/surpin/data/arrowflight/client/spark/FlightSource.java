package net.surpin.data.arrowflight.client.spark;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

/**
 * Define the flight-source which supports both read from and write to remote flight-service
 */
public class FlightSource implements TableProvider, DataSourceRegister, RelationProvider {
    //the option keys for connection
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String TLS_ENABLED = "tls.enabled";
    private static final String TLS_VERIFY_SERVER = "tls.verifyServer";
    private static final String TLS_TRUSTSTORE_JKS = "tls.truststore.jksFile";
    private static final String TLS_TRUSTSTORE_PASS = "tls.truststore.pass";
    //the option keys for account
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String BEARER_TOKEN = "bearerToken";

    //the option keys for table
    private static final String TABLE = "table";
    //the quote for field in case a field containing irregular characters, such as -
    private static final String COLUMN_QUOTE = "column.quote";

    //Managing Workloads
    public static final String KEY_DEFAULT_SCHEMA = "default.schema";
    public static final String KEY_ROUTING_TAG = "routing.tag";
    public static final String KEY_ROUTING_QUEUE = "routing.queue";

    //Retry & timeout
    private static final String KEY_MAX_RETRIES = "client.maxRetries";
    private static final String KEY_RETRY_BACKOFF_MS = "client.retryBackoffMs";
    private static final String KEY_CONNECT_TIMEOUT_MS = "client.connectTimeoutMs";

    //the service configuration
    private Configuration configuration = null;
    //the name of the table
    private Table tableName = null;

    /**
     * Infer schema from the options
     * @param options - the options container
     * @return - the schema inferred
     */
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        this.probeOptions(options, true);
        return this.tableName.getSparkSchema();
    }

    /**
     * Extract and validate connection options from the provided options map
     * @param options - the case-insensitive options map
     */
    private void probeOptions(CaseInsensitiveStringMap options, boolean initialize) {
        //host & port
        String host = options.getOrDefault(FlightSource.HOST, "");
        int port = Integer.parseInt(options.getOrDefault(FlightSource.PORT, "32010"));
        //account
        String user = options.getOrDefault(FlightSource.USER, "");
        String password = options.getOrDefault(FlightSource.PASSWORD, "");
        String bearerToken = options.getOrDefault(FlightSource.BEARER_TOKEN, "");
        //validation - host, user & password cannot be empty
        if (host.isEmpty() || user.isEmpty() || (password.isEmpty() && bearerToken.isEmpty())) {
            throw new RuntimeException("The host, user and (password or access-token) are all mandatory.");
        }
        //tls configuration
        boolean tlsEnabled = Boolean.parseBoolean(options.getOrDefault(FlightSource.TLS_ENABLED, "false"));
        boolean tlsVerify = Boolean.parseBoolean(options.getOrDefault(FlightSource.TLS_VERIFY_SERVER, "true"));
        String truststoreJks = options.getOrDefault(FlightSource.TLS_TRUSTSTORE_JKS, "");
        String truststorePass = options.getOrDefault(FlightSource.TLS_TRUSTSTORE_PASS, "");
        //set up the configuration object
        this.configuration = (truststoreJks != null && !truststoreJks.isEmpty())
            ? new Configuration(host, port, truststoreJks, truststorePass, user, password, bearerToken) : new Configuration(host, port, tlsEnabled, tlsEnabled && tlsVerify, user, password, bearerToken);

        //set the schema path, routing tag & queue if any to manage work-loads
        this.configuration.setDefaultSchema(options.getOrDefault(FlightSource.KEY_DEFAULT_SCHEMA, ""));
        this.configuration.setRoutingTag(options.getOrDefault(FlightSource.KEY_ROUTING_TAG, ""));
        this.configuration.setRoutingQueue(options.getOrDefault(FlightSource.KEY_ROUTING_QUEUE, ""));

        //retry and timeout settings
        String maxRetries = options.get(FlightSource.KEY_MAX_RETRIES);
        if (maxRetries != null) {
            this.configuration.setMaxRetries(Integer.parseInt(maxRetries));
        }
        String retryBackoff = options.get(FlightSource.KEY_RETRY_BACKOFF_MS);
        if (retryBackoff != null) {
            this.configuration.setRetryBackoffMs(Long.parseLong(retryBackoff));
        }
        String connectTimeout = options.get(FlightSource.KEY_CONNECT_TIMEOUT_MS);
        if (connectTimeout != null) {
            this.configuration.setConnectTimeoutMs(Long.parseLong(connectTimeout));
        }

        //the table name
        String requestedTableName = options.getOrDefault(FlightSource.TABLE, "");
        //table name cannot empty
        if (requestedTableName == null || requestedTableName.isEmpty()) {
            throw new RuntimeException("The table is mandatory.");
        }
        //set up the flight-table with the column quote-character. By default, columns are not quoted
        this.tableName = Table.forTable(requestedTableName,
                options.getOrDefault(FlightSource.COLUMN_QUOTE, ""));
        if (initialize) {
            this.tableName.initializeSchema(this.configuration);
        }
    }

    /**
     * Get the table of the DataSource
     * @param schema - the schema of the table
     * @param partitioning - the partitioning for the table
     * @param properties - the properties of the table
     * @return - a Table object
     */
    @Override
    public org.apache.spark.sql.connector.catalog.Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new FlightTable(this.configuration, this.tableName);
    }

    /**
     * Restores a persisted Flight table from Spark's session catalog without
     * issuing a schema-only SELECT * request. The real query is planned once,
     * after Spark has supplied projection/filter/aggregate pushdown.
     */
    FlightTable getTableFromCatalog(CaseInsensitiveStringMap options, StructType schema) {
        this.probeOptions(options, false);
        this.tableName.setSparkSchema(schema);
        return new FlightTable(this.configuration, this.tableName);
    }

    /**
     * Spark Thrift Server resolves persisted Hive data-source tables through DataSource V1.
     */
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters) {
        Map<String, String> options = new HashMap<>(JavaConverters.mapAsJavaMap(parameters));
        this.probeOptions(new CaseInsensitiveStringMap(options), true);
        return new FlightRelation(sqlContext, this.configuration, this.tableName);
    }

    /**
     * Get the short-name of the DataSource
     * @return - the short name of the DataSource
     */
    @Override
    public String shortName() {
        return "flight";
    }
}
