package pro.surpin.data.arrowflight.server;

import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/** Plans reads from tables managed by one table format. */
public interface TableProvider {

    /**
     * Resolves a table and plans a read against its selected state.
     *
     * @param table logical table identifier
     * @return the schema and source for the selected table state
     * @throws IOException if table metadata cannot be read
     */
    TableScanPlan planScan(TableIdentifier table) throws IOException;

    /**
     * Identifies a table within the configured catalog.
     *
     * @param schema SQL schema name
     * @param table SQL table name
     */
    record TableIdentifier(String schema, String table) {
    }

    /**
     * Pairs the Arrow schema of a selected table state with the source for reading its rows.
     *
     * @param tableSchema Arrow schema of the planned table scan
     * @param source source to open during execution
     */
    record TableScanPlan(Schema tableSchema, ScanSource source) {
    }

    /** Describes the source the executor opens to read a selected table state. */
    interface ScanSource {
    }

    /**
     * Identifies a specific Iceberg snapshot.
     *
     * @param metadataFile URI of the selected Iceberg metadata JSON file
     * @param snapshotId ID of the snapshot to read
     */
    record IcebergSnapshot(URI metadataFile, long snapshotId) implements ScanSource {
    }

    /**
     * Identifies a specific Paimon snapshot.
     *
     * @param tablePath URI of the Paimon table directory holding the snapshot
     * @param snapshotId ID of the snapshot to read
     */
    record PaimonSnapshot(URI tablePath, long snapshotId) implements ScanSource {
    }

    /**
     * Lists physical files of a table whose rows are read directly by the executor.
     *
     * @param files file paths relative to the configured data directory
     */
    record FileListSource(List<String> files) implements ScanSource {
    }

    /** Represents a table scan that produces no rows. */
    record EmptyScan() implements ScanSource {
    }
}
