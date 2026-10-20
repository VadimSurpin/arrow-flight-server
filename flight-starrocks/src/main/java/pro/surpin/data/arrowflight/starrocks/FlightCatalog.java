package pro.surpin.data.arrowflight.starrocks;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
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
        return planScan(schema, table, columns, null);
    }

    /**
     * Plans one query with a pushed-down filter and returns endpoint tickets.
     *
     * @param schema schema name
     * @param table table name
     * @param columns selected columns, or empty for all columns
     * @param where filter to push down, or null for none
     * @return Flight schema and endpoints
     */
    public FlightInfo planScan(String schema, String table, List<String> columns,
            ScanPredicate where) {
        return sqlClient.execute(ScanSql.select(schema, table, columns, where), callOptions);
    }

    /**
     * Plans a partial-aggregate query with an optional pushed-down filter.
     *
     * @param schema schema name
     * @param table table name
     * @param where filter to push down, or null for none
     * @param aggregate group keys and partial aggregates
     * @return Flight schema and endpoints
     */
    public FlightInfo planScan(String schema, String table, ScanPredicate where, ScanAggregate aggregate) {
        return sqlClient.execute(ScanSql.select(schema, table, aggregate, where), callOptions);
    }

    /**
     * Plans an arbitrary query; for contract tests against a live server.
     *
     * @param sql query text
     * @return Flight schema and endpoints
     */
    FlightInfo rawQuery(String sql) {
        return sqlClient.execute(sql, callOptions);
    }

    /**
     * Streams every endpoint of a planned query and counts the rows.
     *
     * @param info planned query
     * @return total row count
     */
    long countRows(FlightInfo info) {
        long total = 0;
        for (long n : readRows(info, (root, row) -> 1L)) {
            total += n;
        }
        return total;
    }

    /**
     * Streams every endpoint and returns the schema of each batch actually received, which can differ from the
     * schema announced when the query was planned.
     *
     * @param info planned query
     * @return streamed schemas, one per endpoint
     */
    List<String> streamSchemas(FlightInfo info) {
        return readRows(info, (root, row) -> row == 0 ? root.getSchema().toString() : null).stream()
                .filter(Objects::nonNull).distinct().toList();
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
        forEachBatch(info, (root, stream) -> {
            for (int row = 0; row < root.getRowCount(); row++) {
                T item = mapper.apply(root, row);
                if (item != null) {
                    result.add(item);
                }
            }
            return true;
        });
        return List.copyOf(result);
    }

    /** Receives each batch of a planned query; returns false to stop reading and cancel the stream. */
    private interface BatchVisitor {
        boolean visit(VectorSchemaRoot root, FlightStream stream);
    }

    /**
     * Streams every endpoint of a planned query batch by batch until the visitor asks to stop.
     *
     * @param info planned query
     * @param visitor batch visitor
     */
    private void forEachBatch(FlightInfo info, BatchVisitor visitor) {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            boolean remote = !endpoint.getLocations().isEmpty();
            FlightClient streamClient = remote
                    ? FlightClient.builder(allocator, endpoint.getLocations().get(0)).build()
                    : client;
            try {
                try (FlightStream stream = streamClient.getStream(endpoint.getTicket(),
                        callOptions)) {
                    while (stream.next()) {
                        if (!visitor.visit(stream.getRoot(), stream)) {
                            stream.cancel("enough rows", null);
                            return;
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
    }

    /**
     * Lists the distinct non-null values of an integer column, giving up as soon as there are more than a limit.
     * Used to turn a selective dimension filter into an IN list on the table it joins.
     *
     * @param schema schema name
     * @param table table name
     * @param column key column
     * @param where filter to push down, or null for none
     * @param maxKeys most keys worth returning
     * @return the keys in first-seen order, or empty when there are more than {@code maxKeys} or the column is
     *         not an integer type
     */
    public Optional<List<Long>> distinctLongKeys(String schema, String table, String column, ScanPredicate where,
            int maxKeys) {
        FlightInfo info = sqlClient.execute(ScanSql.distinctKeys(schema, table, column, where), callOptions);
        Set<Long> keys = new LinkedHashSet<>();
        boolean[] usable = {true};
        forEachBatch(info, (root, stream) -> {
            FieldVector vector = root.getFieldVectors().get(0);
            if (!(vector instanceof BigIntVector || vector instanceof IntVector || vector instanceof SmallIntVector
                    || vector instanceof TinyIntVector)) {
                usable[0] = false;
                return false;
            }
            for (int row = 0; row < root.getRowCount(); row++) {
                if (!vector.isNull(row)) {
                    keys.add(((Number) vector.getObject(row)).longValue());
                    if (keys.size() > maxKeys) {
                        return false;
                    }
                }
            }
            return true;
        });
        return usable[0] && keys.size() <= maxKeys ? Optional.of(List.copyOf(keys)) : Optional.empty();
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
