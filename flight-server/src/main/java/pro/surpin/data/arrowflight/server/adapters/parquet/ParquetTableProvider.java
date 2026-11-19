package pro.surpin.data.arrowflight.server.adapters.parquet;

import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.server.TableProvider;

import java.io.IOException;
import java.util.List;

/** Plans reads of Parquet tables stored below the configured data directory. */
public final class ParquetTableProvider implements TableProvider {

    private final ParquetTableFiles tableFiles;

    /**
     * Plans Parquet tables through a file inventory port.
     *
     * @param tableFiles port listing the files of one table
     */
    public ParquetTableProvider(ParquetTableFiles tableFiles) {
        this.tableFiles = tableFiles;
    }

    /**
     * Resolves the schema and files of one Parquet table.
     *
     * @param table logical table identifier
     * @return table schema and the relative file list to read
     * @throws IOException if the table files cannot be listed
     */
    @Override
    public TableScanPlan planScan(TableIdentifier table) throws IOException {
        Schema schema = tableFiles.tableSchema(table.schema(), table.table());
        List<String> files = tableFiles.tableFiles(table.schema(), table.table());
        return new TableScanPlan(schema, new FileListSource(files));
    }

    /** Supplies the Parquet schema and file inventory of a table. */
    public interface ParquetTableFiles {

        /**
         * Returns the Arrow schema of a table.
         *
         * @param schema schema name
         * @param table table name
         * @return Arrow schema of the table
         */
        Schema tableSchema(String schema, String table);

        /**
         * Lists the Parquet files of a table relative to the data directory.
         *
         * @param schema schema name
         * @param table table name
         * @return relative Parquet file paths
         * @throws IOException on file-system access failure
         */
        List<String> tableFiles(String schema, String table) throws IOException;
    }
}
