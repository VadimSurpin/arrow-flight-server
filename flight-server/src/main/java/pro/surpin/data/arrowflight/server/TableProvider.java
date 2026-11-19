package pro.surpin.data.arrowflight.server;

import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

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
    interface ScanSource extends Serializable {
    }

    /**
     * Identifies a specific Iceberg snapshot.
     *
     * @param metadataFile URI of the selected Iceberg metadata JSON file
     * @param snapshotId ID of the snapshot to read
     */
    record IcebergSnapshot(URI metadataFile, long snapshotId) implements ScanSource {
    }

    /** Represents a table scan that produces no rows. */
    record EmptyScan() implements ScanSource {
    }
}
