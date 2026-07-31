package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.query.Endpoint;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Describes the data-structure of flight input-partitions
 */
public class FlightInputPartition implements InputPartition, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightInputPartition.class);

    /**
     * The input-partition with end-points
     */
    public static class FlightEndpointInputPartition extends FlightInputPartition {
        private final Endpoint ep;
        private final String[] preferredLocations;

        /**
         * Construct a flight end-point input-partition
         * @param schema - the schema of the partition
         * @param ep - the end-point of the partition
         */
        public FlightEndpointInputPartition(Schema schema, Endpoint ep) {
            super(schema);
            this.ep = ep;
            this.preferredLocations = endpointHosts(ep);
        }

        /**
         * Get the end-point of the input-partition
         * @return - the end-point of the input partition
         */
        public Endpoint getEndpoint() {
            return this.ep;
        }

        @Override
        public String[] preferredLocations() {
            return this.preferredLocations.clone();
        }

        /**
         * Resolves Spark locality hints from Flight endpoint host names.
         *
         * @param endpoint Flight endpoint assigned to the partition
         * @return unique endpoint and colocated Spark worker host names
         */
        private static String[] endpointHosts(Endpoint endpoint) {
            Set<String> hosts = new LinkedHashSet<>();
            if (endpoint == null || endpoint.getURIs() == null) {
                return new String[0];
            }
            for (URI uri : endpoint.getURIs()) {
                if (uri == null || uri.getHost() == null || uri.getHost().isBlank()) {
                    continue;
                }
                String host = uri.getHost();
                hosts.add(host);
                if (host.startsWith("flight-server-")) {
                    hosts.add("server-node-" + host.substring("flight-server-".length()));
                }
            }
            return hosts.toArray(new String[0]);
        }
    }

    /**
     * The input-partition with query
     */
    public static class FlightQueryInputPartition extends FlightInputPartition {
        private final String query;

        /**
         * Construct a flight query input-partition
         * @param schema - the schema of the partition
         * @param query - the query for the partition
         */
        public FlightQueryInputPartition(Schema schema, String query) {
            super(schema);
            this.query = query;
        }

        /**
         * Get the query of the input-partition
         * @return - the query of the input-partition
         */
        public String getQuery() {
            return this.query;
        }
    }

    //the schema of the input partition
    private final String schema;
    private transient volatile Schema parsedSchema;

    /**
     * Construct a flight-input-partition
     * @param schema - the schema of the partition
     */
    protected FlightInputPartition(Schema schema) {
        this.schema = schema.toJson();
        this.parsedSchema = schema;
    }

    /**
     * Get the schema of the partition
     * @return - the schema of the partition
     * @throws IOException - thrown when the schema is in invalid json format.
     */
    public Schema getSchema() throws IOException {
        Schema cached = this.parsedSchema;
        if (cached == null) {
            cached = Schema.fromJSON(this.schema);
            this.parsedSchema = cached;
        }
        return cached;
    }
}
