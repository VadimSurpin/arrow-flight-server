package pro.surpin.data.arrowflight.client.query;

import org.apache.arrow.vector.types.pojo.Schema;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Describes the data-structure from executing a query on remote flight service
 */
public class QueryEndpoints implements Serializable {
    //the schema
    private final transient Schema schema;
    //the collection of end-points exposed for the query
    private final Endpoint[] endpoints;
    //estimated bytes reported by FlightInfo
    private final long totalBytes;
    //estimated rows reported by FlightInfo
    private final long totalRecords;

    /**
     * Construct a QueryEndpoints
     * @param schema - the schema of the query result
     * @param endpoints - end end-points exposed on the remote flight-service for fetching data
     */
    public QueryEndpoints(Schema schema, Endpoint[] endpoints) {
        this(schema, endpoints, -1L, -1L);
    }

    /**
     * Construct query endpoints with Flight dataset statistics.
     *
     * @param schema result schema
     * @param endpoints endpoints exposed by the remote Flight service
     * @param totalBytes estimated bytes in the dataset
     * @param totalRecords estimated records in the dataset
     */
    public QueryEndpoints(Schema schema, Endpoint[] endpoints,
            long totalBytes, long totalRecords) {
        this.schema = schema;
        this.endpoints = endpoints;
        this.totalBytes = totalBytes;
        this.totalRecords = totalRecords;
    }

    /**
     * Get the Schema
     * @return - the schema of the QueryEndpoints
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the end-points
     * @return - the end-points of the QueryEndpoints
     */
    public Endpoint[] getEndpoints() {
        return this.endpoints;
    }

    /**
     * Returns the estimated dataset size reported by FlightInfo.
     *
     * @return estimated bytes, or a negative value when unavailable
     */
    public long getTotalBytes() {
        return totalBytes;
    }

    /**
     * Returns the estimated row count reported by FlightInfo.
     *
     * @return estimated records, or a negative value when unavailable
     */
    public long getTotalRecords() {
        return totalRecords;
    }

    @Override
    public String toString() {
        return "QueryEndpoints{" +
                "schema=" + schema +
                ", endpoints=" + Arrays.toString(endpoints) +
                ", totalBytes=" + totalBytes +
                ", totalRecords=" + totalRecords +
                '}';
    }
}
