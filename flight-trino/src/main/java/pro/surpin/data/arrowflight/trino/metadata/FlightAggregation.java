package pro.surpin.data.arrowflight.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Partial aggregate contracts and their grouping columns.
 *
 * @param groupingColumns grouping columns
 * @param aggregates partial aggregate specifications
 */
public record FlightAggregation(@JsonProperty("groupingColumns") List<FlightColumnHandle> groupingColumns,
                                @JsonProperty("aggregates") List<AggregateSpec> aggregates) {
    /**
     * Copies the aggregation columns.
     *
     * @param groupingColumns grouping columns
     * @param aggregates partial aggregate specifications
     */
    @JsonCreator
    public FlightAggregation {
        groupingColumns = List.copyOf(groupingColumns);
        aggregates = List.copyOf(aggregates);
    }

    /**
     * Returns the columns produced by the scan.
     *
     * @return grouping columns followed by native aggregate states
     */
    public List<FlightColumnHandle> outputColumns() {
        List<FlightColumnHandle> columns = new ArrayList<>(groupingColumns);
        aggregates.forEach(aggregate -> columns.add(aggregate.output()));
        return List.copyOf(columns);
    }

}
