package pro.surpin.data.arrowflight.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Partial count states and their grouping columns.
 *
 * @param groupingColumns grouping columns
 * @param counts partial counts
 */
public record FlightAggregation(@JsonProperty("groupingColumns") List<FlightColumnHandle> groupingColumns,
                                @JsonProperty("counts") List<Count> counts) {
    /**
     * Copies the aggregation columns.
     *
     * @param groupingColumns grouping columns
     * @param counts partial counts
     */
    @JsonCreator
    public FlightAggregation {
        groupingColumns = List.copyOf(groupingColumns);
        counts = List.copyOf(counts);
    }

    /**
     * Returns the columns produced by the scan.
     *
     * @return grouping columns followed by count states
     */
    public List<FlightColumnHandle> outputColumns() {
        List<FlightColumnHandle> columns = new ArrayList<>(groupingColumns);
        counts.forEach(count -> columns.add(count.output()));
        return List.copyOf(columns);
    }

    /**
     * A partial count with an optional input column.
     *
     * @param input input column, or empty for count of all rows
     * @param output output state column
     */
    public record Count(@JsonProperty("input") Optional<FlightColumnHandle> input,
                        @JsonProperty("output") FlightColumnHandle output) {
        /**
         * Validates the count columns.
         *
         * @param input input column, or empty for count of all rows
         * @param output output state column
         */
        @JsonCreator
        public Count {
            Objects.requireNonNull(input, "input");
            Objects.requireNonNull(output, "output");
        }
    }
}
