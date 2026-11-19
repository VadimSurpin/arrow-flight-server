package pro.surpin.data.arrowflight.trino.client;

import pro.surpin.data.arrowflight.trino.read.FlightSplit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;
import pro.surpin.data.arrowflight.client.read.EndpointReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/** Flight server access through the shared flight-client-core client. */
public final class CoreFlightService implements FlightService {
    private static final String SCHEMA_NAME = "db_schema_name";
    private static final ObjectMapper JSON = new ObjectMapper();

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
                () -> sql.getSchemas(null, null, callOptions(client)), "getSchemas");
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
                () -> sql.getTables(null, schema, null, null, true, callOptions(client)), "getTables");
        Map<String, Schema> tables = new LinkedHashMap<>();
        readAll(client, info, root -> {
            for (int row = 0; row < root.getRowCount(); row++) {
                if (schema.equals(text(root, SCHEMA_NAME, row))) {
                    byte[] bytes = (byte[]) root.getVector("table_schema").getObject(row);
                    tables.put(text(root, "table_name", row), TableSchemaDecoder.decode(bytes));
                }
            }
        });
        return tables;
    }

    @Override
    public QueryEndpoints plan(String sql) {
        Client client = client();
        return client.retryWithBackoff(() -> {
            FlightInfo info = new FlightSqlClient(client.getFlightClient()).execute(sql, callOptions(client));
            Endpoint[] endpoints = info.getEndpoints().stream()
                    .map(endpoint -> new Endpoint(endpoint.getLocations().stream().map(Location::getUri)
                            .toArray(URI[]::new), endpoint.getTicket().getBytes()))
                    .toArray(Endpoint[]::new);
            return new QueryEndpoints(info.getSchema(), endpoints, info.getBytes(), info.getRecords());
        }, "planTrinoQuery");
    }

    @Override
    public long estimate(String schema, String table) {
        Client client = client();
        try {
            return client.retryWithBackoff(() -> {
                byte[] request = JSON.writeValueAsBytes(Map.of("schema", schema, "table", table, "columns", List.of()));
                var results = client.getFlightClient().doAction(
                        new Action("arrowflight.table_statistics", request), callOptions(client));
                if (!results.hasNext()) {
                    return -1L;
                }
                var rows = JSON.readTree(results.next().getBody()).path("rows");
                if (results.hasNext()) {
                    throw new IllegalStateException("Flight returned multiple table statistics results");
                }
                return rows.isIntegralNumber() && rows.canConvertToLong() && rows.longValue() >= 0
                        ? rows.longValue() : -1L;
            }, "estimateTrinoTable");
        } catch (FlightRuntimeException e) {
            if (e.status().code() == CallStatus.UNIMPLEMENTED.code()
                    || e.status().code() == CallStatus.NOT_FOUND.code()) {
                return -1L;
            }
            throw e;
        }
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
    private void readAll(Client client, FlightInfo info, Consumer<VectorSchemaRoot> consumer) {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = endpoint.getLocations().isEmpty()
                    || endpoint.getLocations().stream().anyMatch(location ->
                            configuration.getFlightHost().equalsIgnoreCase(location.getUri().getHost())
                                    && configuration.getFlightPort() == location.getUri().getPort())
                    ? client.getFlightClient().getStream(endpoint.getTicket(), callOptions(client))
                    : client.openStream(endpoint)) {
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
     * Combines shared authentication and timeout options with Trino request routing.
     *
     * @param client shared Flight client
     * @return options for a metadata or planning request
     */
    private CallOption[] callOptions(Client client) {
        List<CallOption> options = new ArrayList<>(List.of(client.callOptions()));
        FlightCallHeaders headers = new FlightCallHeaders();
        String[][] properties = {{"SCHEMA", configuration.getDefaultSchema()},
                {"ROUTING_TAG", configuration.getRoutingTag()}, {"ROUTING_QUEUE", configuration.getRoutingQueue()}};
        for (String[] property : properties) {
            if (property[1] != null && !property[1].isEmpty()) {
                headers.insert(property[0], property[1]);
            }
        }
        if (!headers.keys().isEmpty()) {
            options.add(new HeaderCallOption(headers));
        }
        return options.toArray(CallOption[]::new);
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
