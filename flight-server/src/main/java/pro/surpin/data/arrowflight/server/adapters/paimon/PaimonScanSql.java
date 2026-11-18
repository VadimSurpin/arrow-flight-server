package pro.surpin.data.arrowflight.server.adapters.paimon;

import pro.surpin.data.arrowflight.server.TableProvider;

import java.net.URI;
import java.nio.file.Path;

/** Builds DuckDB SQL that reads a pinned Paimon snapshot. */
public final class PaimonScanSql {

    /** Creates no instances of the helper. */
    private PaimonScanSql() {
    }

    /**
     * Builds a DuckDB FROM clause for a Paimon snapshot.
     *
     * @param snapshot planned Paimon scan source
     * @return SQL table expression reading the snapshot
     */
    public static String fromClause(TableProvider.PaimonSnapshot snapshot) {
        return fromClause(snapshot.tablePath(), snapshot.snapshotId());
    }

    /**
     * Builds a DuckDB FROM clause for a table directory and snapshot identifier.
     *
     * @param tablePath URI of the Paimon table directory
     * @param snapshotId snapshot identifier to read
     * @return SQL table expression reading the snapshot
     */
    public static String fromClause(URI tablePath, long snapshotId) {
        String directory = Path.of(tablePath).toString();
        return "paimon_scan(" + literal(directory) + ", snapshot_from_id = " + snapshotId + ")";
    }

    /**
     * Builds a DuckDB FROM clause for a Paimon table directory without a pinned snapshot.
     *
     * @param tableDir Paimon table directory
     * @return SQL table expression reading the latest table state
     */
    public static String fromClause(Path tableDir) {
        return "paimon_scan(" + literal(tableDir.toString()) + ")";
    }

    /**
     * Quotes a SQL string literal.
     *
     * @param value raw value
     * @return literal with doubled single quotes
     */
    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
