package pro.surpin.data.arrowflight.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

/**
 * Column of a Flight table.
 *
 * @param name Trino column name, lower case
 * @param remoteName column name on the Flight server
 * @param type Trino type
 * @param nullFreeOrder whether required composite members have compatible ordering
 * @param expression validated source expression, or null for a physical column
 */
public record FlightColumnHandle(@JsonProperty("name") String name,
                                 @JsonProperty("remoteName") String remoteName,
                                 @JsonProperty("type") Type type,
                                 @JsonProperty("nullFreeOrder") boolean nullFreeOrder,
                                 @JsonProperty("expression") String expression) implements ColumnHandle {
    /**
     * Creates a scalar column without a composite ordering guarantee.
     *
     * @param name Trino column name
     * @param remoteName remote column name
     * @param type Trino type
     */
    public FlightColumnHandle(String name, String remoteName, Type type) {
        this(name, remoteName, type, false, null);
    }

    /**
     * Creates a physical column with a composite ordering guarantee.
     *
     * @param name Trino column name
     * @param remoteName remote column name
     * @param type Trino type
     * @param nullFreeOrder whether required composite members have compatible ordering
     */
    public FlightColumnHandle(String name, String remoteName, Type type, boolean nullFreeOrder) {
        this(name, remoteName, type, nullFreeOrder, null);
    }
    /**
     * Validates the components.
     *
     * @param name Trino column name
     * @param remoteName remote column name
     * @param type Trino type
     * @param nullFreeOrder whether composite members have compatible ordering
     * @param expression validated source expression, or null
     */
    @JsonCreator
    public FlightColumnHandle {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(remoteName, "remoteName");
        Objects.requireNonNull(type, "type");
    }

    /**
     * Builds the Trino column metadata.
     *
     * @return column metadata
     */
    public ColumnMetadata toMetadata() {
        return new ColumnMetadata(name, type);
    }
}
