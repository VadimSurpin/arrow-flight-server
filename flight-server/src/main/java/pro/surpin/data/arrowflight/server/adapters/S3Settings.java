package pro.surpin.data.arrowflight.server.adapters;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;

/**
 * Connection settings for an S3-compatible object store (AWS S3, MinIO, ...).
 *
 * <p>One set of values configures both readers: the Hadoop {@code s3a} filesystem, which lists files and reads
 * Parquet footers without DuckDB, and the DuckDB {@code httpfs} extension, which scans the data. Values come from
 * {@code FLIGHT_S3_*} environment variables or the matching system properties ({@code flight.s3.*}); anything
 * left unset falls back to the AWS default credential chain.
 */
public final class S3Settings {

    private static final String DEFAULT_REGION = "us-east-1";

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final boolean pathStyle;

    private S3Settings(String endpoint, String accessKey, String secretKey, String region, boolean pathStyle) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.pathStyle = pathStyle;
    }

    /**
     * Reads settings from the process environment and system properties.
     *
     * @return settings
     */
    public static S3Settings fromEnvironment() {
        return fromLookup(name -> {
            String value = System.getenv(name);
            return value != null ? value : System.getProperty(name.toLowerCase().replace('_', '.'));
        });
    }

    /**
     * Reads settings through an arbitrary lookup, mainly for tests.
     *
     * @param lookup returns the value of a {@code FLIGHT_S3_*} name or null
     * @return settings
     */
    static S3Settings fromLookup(Function<String, String> lookup) {
        String endpoint = blankToNull(lookup.apply("FLIGHT_S3_ENDPOINT"));
        String region = blankToNull(lookup.apply("FLIGHT_S3_REGION"));
        String style = blankToNull(lookup.apply("FLIGHT_S3_PATH_STYLE"));
        // A custom endpoint (MinIO) needs path-style addressing unless it is told otherwise.
        boolean pathStyle = style != null ? Boolean.parseBoolean(style) : endpoint != null;
        return new S3Settings(endpoint, blankToNull(lookup.apply("FLIGHT_S3_ACCESS_KEY")),
                blankToNull(lookup.apply("FLIGHT_S3_SECRET_KEY")),
                region, pathStyle);
    }

    /**
     * Tells whether a data directory lives in an object store.
     *
     * @param dataDir data directory URI or path
     * @return true for {@code s3a://}, {@code s3n://} and {@code s3://}
     */
    public static boolean isRemote(String dataDir) {
        return dataDir != null && (dataDir.startsWith("s3a://") || dataDir.startsWith("s3n://")
                || dataDir.startsWith("s3://"));
    }

    /**
     * Rewrites a Hadoop object-store URI into the {@code s3://} form DuckDB understands.
     *
     * @param path Hadoop-qualified path or URI
     * @return DuckDB path; other paths are returned unchanged
     */
    public static String duckDbPath(String path) {
        if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
            return "s3://" + path.substring("s3a://".length());
        }
        return path;
    }

    /**
     * Applies the settings to a Hadoop configuration for the {@code s3a} filesystem.
     *
     * @param conf configuration to update
     */
    public void applyTo(Configuration conf) {
        if (endpoint != null) {
            conf.set("fs.s3a.endpoint", endpoint);
            conf.setBoolean("fs.s3a.connection.ssl.enabled", useSsl());
            conf.setBoolean("fs.s3a.path.style.access", pathStyle);
        }
        if (accessKey != null && secretKey != null) {
            conf.set("fs.s3a.access.key", accessKey);
            conf.set("fs.s3a.secret.key", secretKey);
        }
        if (region != null || endpoint != null) {
            conf.set("fs.s3a.endpoint.region", regionOrDefault());
        }
    }

    /**
     * Statements that make a DuckDB database able to read the object store.
     *
     * @return SQL statements, in order
     */
    public List<String> duckDbStatements() {
        List<String> sql = new ArrayList<>();
        sql.add("INSTALL httpfs");
        sql.add("LOAD httpfs");
        if (endpoint != null) {
            sql.add("SET s3_endpoint = " + literal(endpointHost()));
            sql.add("SET s3_use_ssl = " + useSsl());
            sql.add("SET s3_url_style = " + literal(pathStyle ? "path" : "vhost"));
        }
        sql.add("SET s3_region = " + literal(regionOrDefault()));
        if (accessKey != null && secretKey != null) {
            sql.add("SET s3_access_key_id = " + literal(accessKey));
            sql.add("SET s3_secret_access_key = " + literal(secretKey));
        }
        return sql;
    }

    /**
     * Returns the endpoint without its scheme, as DuckDB expects it.
     *
     * @return host and optional port, or null when no endpoint is set
     */
    public String endpointHost() {
        if (endpoint == null) {
            return null;
        }
        int scheme = endpoint.indexOf("://");
        return scheme >= 0 ? endpoint.substring(scheme + 3) : endpoint;
    }

    /**
     * Tells whether the endpoint is reached over TLS.
     *
     * @return false only for an explicit {@code http://} endpoint
     */
    public boolean useSsl() {
        return endpoint == null || !endpoint.startsWith("http://");
    }

    /**
     * Tells whether path-style bucket addressing is used.
     *
     * @return true for path-style requests
     */
    public boolean pathStyle() {
        return pathStyle;
    }

    private String regionOrDefault() {
        return region != null ? region : DEFAULT_REGION;
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
