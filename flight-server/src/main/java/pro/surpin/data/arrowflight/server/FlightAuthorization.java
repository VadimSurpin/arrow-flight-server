package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallStatus;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;

import java.util.HashSet;
import java.util.Set;

/** Decides access to Flight schemas, tables, and SQL statements. */
public interface FlightAuthorization extends AutoCloseable {
    /**
     * Checks schema discovery access.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @return true when visible
     */
    boolean canSeeSchema(String principal, String schema);

    /**
     * Checks table read access.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @param table table name
     * @return true when readable
     */
    boolean canReadTable(String principal, String schema, String table);

    /**
     * Requires access to each table read by a query.
     *
     * @param principal authenticated caller identity
     * @param query SQL statement
     */
    default void requireQuery(String principal, String query) {
        ParquetQueryParser parsed = ParquetQueryParser.parse(query);
        Set<String> checked = new HashSet<>();
        if (parsed.isJoin) {
            if (parsed.joinTables.isEmpty()) {
                throw denied();
            }
            for (ParquetQueryParser.JoinTable table : parsed.joinTables) {
                requireTable(principal, table.schema(), table.table(), checked);
            }
        } else {
            requireTable(principal, parsed.schema, parsed.table, checked);
        }
    }

    /**
     * Requires access to one query table.
     *
     * @param principal authenticated caller identity
     * @param schema schema name
     * @param table table name
     * @param checked already checked table keys
     */
    private void requireTable(String principal, String schema, String table,
            Set<String> checked) {
        if (schema == null || table == null) {
            throw denied();
        }
        String key = schema + "\u0000" + table;
        if (checked.add(key) && !canReadTable(principal, schema, table)) {
            throw denied();
        }
    }

    /**
     * Creates a generic denial status.
     *
     * @return unauthorized Flight error
     */
    private static RuntimeException denied() {
        return CallStatus.UNAUTHORIZED
                .withDescription("Schema or table access denied")
                .toRuntimeException();
    }

    @Override
    default void close() {
    }
}
