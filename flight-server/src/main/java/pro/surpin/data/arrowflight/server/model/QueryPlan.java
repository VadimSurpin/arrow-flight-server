package pro.surpin.data.arrowflight.server.model;

import org.apache.arrow.flight.FlightEndpoint;

import java.util.List;

/**
 * Contains Flight endpoints and estimated input statistics for a query.
 *
 * @param endpoints Flight endpoints assigned to the query
 * @param totalBytes total bytes of assigned Parquet files
 * @param totalRecords estimated rows in assigned Parquet files
 */
public record QueryPlan(List<FlightEndpoint> endpoints, long totalBytes, long totalRecords) {

    /**
     * Creates an immutable query plan.
     *
     * @param endpoints Flight endpoints assigned to the query
     * @param totalBytes total bytes of assigned Parquet files
     * @param totalRecords estimated rows in assigned Parquet files
     */
    public QueryPlan {
        endpoints = List.copyOf(endpoints);
    }
}
