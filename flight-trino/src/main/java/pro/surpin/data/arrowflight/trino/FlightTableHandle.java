package pro.surpin.data.arrowflight.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Flight table with the projection and filter pushed into its scan.
 *
 * @param schemaTableName Trino table name
 * @param remoteSchema schema name on the Flight server
 * @param remoteTable table name on the Flight server
 * @param constraint filter pushed into the scan
 * @param columns projected columns, or empty when every column is read
 */
public record FlightTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                                @JsonProperty("remoteSchema") String remoteSchema,
                                @JsonProperty("remoteTable") String remoteTable,
                                @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
                                @JsonProperty("columns") Optional<List<FlightColumnHandle>> columns)
        implements ConnectorTableHandle {
    /**
     * Validates the components.
     *
     * @param schemaTableName Trino table name
     * @param remoteSchema remote schema name
     * @param remoteTable remote table name
     * @param constraint pushed filter
     * @param columns projected columns
     */
    @JsonCreator
    public FlightTableHandle {
        Objects.requireNonNull(schemaTableName, "schemaTableName");
        Objects.requireNonNull(remoteSchema, "remoteSchema");
        Objects.requireNonNull(remoteTable, "remoteTable");
        Objects.requireNonNull(constraint, "constraint");
        columns = Objects.requireNonNull(columns, "columns").map(List::copyOf);
    }

    /**
     * Creates a handle that scans the whole table.
     *
     * @param schemaTableName Trino table name
     * @param remoteSchema remote schema name
     * @param remoteTable remote table name
     * @return handle without pushdown
     */
    public static FlightTableHandle of(SchemaTableName schemaTableName, String remoteSchema, String remoteTable) {
        return new FlightTableHandle(schemaTableName, remoteSchema, remoteTable, TupleDomain.all(),
                Optional.empty());
    }

    /**
     * Returns a copy with another filter.
     *
     * @param newConstraint filter
     * @return new handle
     */
    public FlightTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new FlightTableHandle(schemaTableName, remoteSchema, remoteTable, newConstraint, columns);
    }

    /**
     * Returns a copy with another projection.
     *
     * @param newColumns projected columns
     * @return new handle
     */
    public FlightTableHandle withColumns(List<FlightColumnHandle> newColumns) {
        return new FlightTableHandle(schemaTableName, remoteSchema, remoteTable, constraint,
                Optional.of(newColumns));
    }
}
