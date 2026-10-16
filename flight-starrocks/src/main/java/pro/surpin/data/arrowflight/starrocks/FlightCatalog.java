package pro.surpin.data.arrowflight.starrocks;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/** Reads Flight SQL catalog metadata for a StarRocks frontend adapter. */
public final class FlightCatalog implements AutoCloseable {
    private final FlightClient client;
    private final FlightSqlClient sqlClient;
    private final BufferAllocator allocator;
    private final CallOption[] callOptions;

    /** A table advertised by the Flight SQL server. */
    public record Table(String catalog, String schema, String name, String type) { }

    /**
     * Creates a metadata client around supplied Arrow clients.
     *
     * @param client Flight transport
     * @param sqlClient Flight SQL transport
     * @param allocator Arrow memory allocator
     * @param callOptions authentication and request options
     */
    FlightCatalog(FlightClient client, FlightSqlClient sqlClient,
            BufferAllocator allocator, CallOption[] callOptions) {
        this.client = Objects.requireNonNull(client, "client");
        this.sqlClient = Objects.requireNonNull(sqlClient, "sqlClient");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.callOptions = callOptions.clone();
    }

    /**
     * Opens a Flight SQL metadata connection.
     *
     * @param location Flight server location
     * @param callOptions authentication and request options
     * @return connected metadata client
     */
    public static FlightCatalog connect(Location location, CallOption... callOptions) {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(callOptions, "callOptions");
        BufferAllocator allocator = new RootAllocator();
        try {
            FlightClient client = FlightClient.builder(allocator, location).build();
            return new FlightCatalog(client, new FlightSqlClient(client), allocator,
                    callOptions);
        } catch (RuntimeException e) {
            allocator.close();
            throw e;
        }
    }

    /**
     * Lists catalogs visible to the supplied Flight identity.
     *
     * @return catalog names
     */
    public List<String> catalogs() {
        return readRows(sqlClient.getCatalogs(callOptions),
                (root, row) -> value(root, row, "catalog_name"));
    }

    /**
     * Lists schemas in a catalog visible to the supplied Flight identity.
     *
     * @param catalog catalog name, or null for all catalogs
     * @return schema names
     */
    public List<String> schemas(String catalog) {
        return readRows(sqlClient.getSchemas(catalog, null, callOptions), (root, row) -> {
            if (catalog != null && !catalog.equals(value(root, row, "catalog_name"))) {
                return null;
            }
            return value(root, row, "db_schema_name");
        });
    }

    /**
     * Lists tables in one schema visible to the supplied Flight identity.
     *
     * @param catalog catalog name
     * @param schema schema name
     * @return table metadata
     */
    public List<Table> tables(String catalog, String schema) {
        Objects.requireNonNull(schema, "schema");
        return readRows(sqlClient.getTables(catalog, null, null, List.of("TABLE"),
                false, callOptions), (root, row) -> {
                    String rowCatalog = value(root, row, "catalog_name");
                    String rowSchema = value(root, row, "db_schema_name");
                    if (!schema.equals(rowSchema)
                            || catalog != null && !catalog.equals(rowCatalog)) {
                        return null;
                    }
                    return new Table(rowCatalog, rowSchema,
                            value(root, row, "table_name"),
                            value(root, row, "table_type"));
                });
    }

    /**
     * Gets a table's Arrow schema without creating scan tickets.
     *
     * @param schema schema name
     * @param table table name
     * @return Arrow schema
     */
    public Schema tableSchema(String schema, String table) {
        return sqlClient.getExecuteSchema(ScanSql.fullTable(schema, table),
                callOptions).getSchema();
    }

    /**
     * Plans one query and returns endpoint tickets for backend distribution.
     *
     * @param schema schema name
     * @param table table name
     * @param columns selected columns, or empty for all columns
     * @return Flight schema and endpoints
     */
    public FlightInfo planScan(String schema, String table, List<String> columns) {
        return sqlClient.execute(ScanSql.select(schema, table, columns), callOptions);
    }

    /**
     * Reads all metadata batches from every Flight endpoint.
     *
     * @param info Flight endpoint information
     * @param mapper row mapper
     * @return mapped rows
     */
    private <T> List<T> readRows(FlightInfo info,
            BiFunction<VectorSchemaRoot, Integer, T> mapper) {
        List<T> result = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            boolean remote = !endpoint.getLocations().isEmpty();
            FlightClient streamClient = remote
                    ? FlightClient.builder(allocator, endpoint.getLocations().get(0)).build()
                    : client;
            try {
                try (FlightStream stream = streamClient.getStream(endpoint.getTicket(),
                        callOptions)) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        for (int row = 0; row < root.getRowCount(); row++) {
                            T item = mapper.apply(root, row);
                            if (item != null) {
                                result.add(item);
                            }
                        }
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException("Flight metadata stream failed", e);
                }
            } finally {
                if (remote) {
                    try {
                        streamClient.close();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted closing Flight stream", e);
                    }
                }
            }
        }
        return List.copyOf(result);
    }

    /**
     * Reads a nullable text value from a metadata row.
     *
     * @param root metadata batch
     * @param row row index
     * @param column column name
     * @return text value or null
     */
    private static String value(VectorSchemaRoot root, int row, String column) {
        Object value = root.getVector(column).getObject(row);
        return value == null ? null : value.toString();
    }

    @Override
    public void close() throws Exception {
        try {
            client.close();
        } finally {
            allocator.close();
        }
    }

}
