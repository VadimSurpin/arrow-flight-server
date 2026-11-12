package pro.surpin.data.arrowflight.trino.read;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One Flight endpoint of a planned query, read by one Trino task.
 *
 * @param uris endpoint locations, empty to read from the coordinator connection
 * @param ticket opaque Flight ticket
 * @param emptyAggregation whether the split supplies default aggregate states for empty input
 */
public record FlightSplit(@JsonProperty("uris") List<String> uris,
                          @JsonProperty("ticket") byte[] ticket,
                          @JsonProperty("emptyAggregation") boolean emptyAggregation) implements ConnectorSplit {
    private static final long BASE_SIZE = 64L;

    /**
     * Validates and copies the components.
     *
     * @param uris endpoint locations
     * @param ticket Flight ticket
     * @param emptyAggregation whether the split supplies empty global states
     */
    @JsonCreator
    public FlightSplit {
        uris = List.copyOf(Objects.requireNonNull(uris, "uris"));
        ticket = Objects.requireNonNull(ticket, "ticket").clone();
        if (emptyAggregation && (!uris.isEmpty() || ticket.length != 0)) {
            throw new IllegalArgumentException("An empty aggregation split cannot have a Flight endpoint");
        }
    }

    /**
     * Creates a split for a Flight endpoint.
     *
     * @param uris endpoint locations
     * @param ticket Flight ticket
     */
    public FlightSplit(List<String> uris, byte[] ticket) {
        this(uris, ticket, false);
    }

    /**
     * Creates a split supplying default states for an empty global aggregation.
     *
     * @return empty aggregate split
     */
    public static FlightSplit emptyAggregate() {
        return new FlightSplit(List.of(), new byte[0], true);
    }

    @Override
    public byte[] ticket() {
        return ticket.clone();
    }

    @Override
    public Object getInfo() {
        return Map.of("uris", uris);
    }

    @Override
    public Map<String, String> getSplitInfo() {
        return Map.of("uris", String.join(",", uris));
    }

    @Override
    public long getRetainedSizeInBytes() {
        long size = BASE_SIZE + ticket.length;
        for (String uri : uris) {
            size += BASE_SIZE + 2L * uri.length();
        }
        return size;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FlightSplit split
                && uris.equals(split.uris) && Arrays.equals(ticket, split.ticket)
                && emptyAggregation == split.emptyAggregation;
    }

    @Override
    public int hashCode() {
        return 31 * (31 * uris.hashCode() + Arrays.hashCode(ticket)) + Boolean.hashCode(emptyAggregation);
    }

    @Override
    public String toString() {
        return "FlightSplit{uris=" + uris + ", ticketBytes=" + ticket.length + "}";
    }
}
