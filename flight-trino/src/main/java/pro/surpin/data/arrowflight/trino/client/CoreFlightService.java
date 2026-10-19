package pro.surpin.data.arrowflight.trino.client;

import pro.surpin.data.arrowflight.trino.read.FlightSplit;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import pro.surpin.data.arrowflight.client.query.QueryStatistics;
import pro.surpin.data.arrowflight.client.read.EndpointReader;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/** Flight server access through the shared flight-client-core client. */
public final class CoreFlightService implements FlightService {
    private static final String SCHEMA_NAME = "db_schema_name";

    private final Configuration configuration;
    private Client connectedClient;
    private boolean closed;

    /**
     * Creates the service; the connection opens on first use.
     *
     * @param configuration client configuration
     */
    public CoreFlightService(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
    }

    @Override
    public List<String> listSchemas() {
        Client client = client();
        FlightSqlClient sql = new FlightSqlClient(client.getFlightClient());
        FlightInfo info = client.retryWithBackoff(
                () -> sql.getSchemas(null, null, client.callOptions()), "getSchemas");
        List<String> schemas = new ArrayList<>();
        readAll(client, info, root -> {
            for (int row = 0; row < root.getRowCount(); row++) {
                schemas.add(text(root, SCHEMA_NAME, row));
            }
        });
        return List.copyOf(schemas);
    }

    @Override
    public Map<String, Schema> listTables(String schema) {
        Objects.requireNonNull(schema, "schema");
        Client client = client();
        FlightSqlClient sql = new FlightSqlClient(client.getFlightClient());
        FlightInfo info = client.retryWithBackoff(
                () -> sql.getTables(null, schema, null, null, true, client.callOptions()), "getTables");
        Map<String, Schema> tables = new LinkedHashMap<>();
        readAll(client, info, root -> {
            for (int row = 0; row < root.getRowCount(); row++) {
                if (schema.equals(text(root, SCHEMA_NAME, row))) {
                    byte[] bytes = (byte[]) root.getVector("table_schema").getObject(row);
                    tables.put(text(root, "table_name", row), decodeSchema(bytes));
                }
            }
        });
        return tables;
    }

    @Override
    public QueryEndpoints plan(String sql) {
        return client().getQueryEndpoints(sql);
    }

    @Override
    public QueryStatistics estimate(String sql) {
        return client().getQueryStatistics(sql);
    }

    @Override
    public BatchReader read(FlightSplit split) {
        URI[] uris = split.uris().stream().map(URI::create).toArray(URI[]::new);
        EndpointReader reader = EndpointReader.forEndpoint(client(), configuration,
                new Endpoint(uris, split.ticket()), null);
        return new BatchReader() {
            @Override
            public boolean next() throws IOException {
                return reader.nextBatch();
            }

            @Override
            public VectorSchemaRoot root() {
                return reader.root();
            }

            @Override
            public void close() {
                reader.close();
            }
        };
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (connectedClient != null) {
            connectedClient.close();
            connectedClient = null;
        }
    }

    /**
     * Decodes an Arrow schema encoded as an IPC schema message.
     *
     * @param bytes encoded schema
     * @return Arrow schema
     * @throws IllegalStateException if the server sent no schema
     */
    static Schema decodeSchema(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalStateException("Flight server returned a table without a schema");
        }
        return MessageSerializer.deserializeSchema(Message.getRootAsMessage(ByteBuffer.wrap(bytes)));
    }

    /**
     * Returns the shared client for this configuration.
     *
     * @return connected client
     */
    private synchronized Client client() {
        if (closed) {
            throw new IllegalStateException("Flight connector is closed");
        }
        if (connectedClient == null) {
            connectedClient = Client.getOrCreate(configuration);
        }
        return connectedClient;
    }

    /**
     * Reads every batch of every endpoint of a metadata result.
     *
     * @param client connected client
     * @param info planned metadata result
     * @param consumer batch consumer
     * @throws IllegalStateException if a stream fails
     */
    private static void readAll(Client client, FlightInfo info, Consumer<VectorSchemaRoot> consumer) {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.openStream(endpoint)) {
                while (stream.next()) {
                    consumer.accept(stream.getRoot());
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted reading Flight metadata", e);
            } catch (Exception e) {
                throw new IllegalStateException("Flight metadata stream failed", e);
            }
        }
    }

    /**
     * Reads a nullable text cell.
     *
     * @param root batch
     * @param column column name
     * @param row row index
     * @return text, or null
     */
    private static String text(VectorSchemaRoot root, String column, int row) {
        Object value = root.getVector(column).getObject(row);
        return value == null ? null : value.toString();
    }
}
