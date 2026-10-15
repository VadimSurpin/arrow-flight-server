package pro.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Session-catalog bridge that rehydrates persisted Flight data-source tables as
 * DataSource V2 tables while delegating every other catalog operation to Spark.
 *
 * <p>Spark's built-in session catalog loads persisted {@code CREATE TABLE USING}
 * metadata as {@link V1Table}, even when the provider implements TableProvider.
 * Spark Thrift therefore cannot otherwise reach Flight's V2 projection, filter,
 * aggregate and columnar scan interfaces.</p>
 */
public final class FlightSessionCatalog extends DelegatingCatalogExtension {
    private final ConcurrentMap<String, StructType> inferredSchemas = new ConcurrentHashMap<>();

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident)
            throws NoSuchTableException {
        org.apache.spark.sql.connector.catalog.Table loaded = super.loadTable(ident);
        if (!(loaded instanceof V1Table v1Table) || !isFlightProvider(v1Table)) {
            return loaded;
        }

        Map<String, String> options = new HashMap<>(
                JavaConverters.mapAsJavaMap(v1Table.options()));
        FlightSource source = new FlightSource();
        CaseInsensitiveStringMap sourceOptions = new CaseInsensitiveStringMap(options);
        StructType schema = v1Table.schema();
        if (schema.isEmpty()) {
            // V1 CREATE TABLE may persist an empty placeholder schema. Resolve it
            // once per remote table without allocating Flight endpoints/tickets.
            schema = this.inferredSchemas.computeIfAbsent(
                    schemaCacheKey(sourceOptions), ignored -> {
                        FlightSource schemaSource = new FlightSource();
                        StructType inferred = schemaSource.inferSchema(sourceOptions);
                        if (inferred.isEmpty()) {
                            throw new IllegalStateException(
                                    "Flight returned an empty schema for "
                                            + sourceOptions.getOrDefault("table", "<unknown>")
                                            + "; verify that the configured Flight node has a local Parquet shard");
                        }
                        return inferred;
                    });
        }
        return source.getTableFromCatalog(sourceOptions, schema);
    }

    private static String schemaCacheKey(CaseInsensitiveStringMap options) {
        return String.join("\u0000",
                options.getOrDefault("host", ""),
                options.getOrDefault("port", "32010"),
                options.getOrDefault("tls.enabled", "false"),
                options.getOrDefault(FlightSource.KEY_DEFAULT_SCHEMA, ""),
                options.getOrDefault("table", ""));
    }

    private static boolean isFlightProvider(V1Table table) {
        Option<String> provider = table.catalogTable().provider();
        if (provider.isEmpty()) {
            return false;
        }
        String name = provider.get();
        return FlightSource.class.getName().equals(name) || "flight".equalsIgnoreCase(name);
    }
}
