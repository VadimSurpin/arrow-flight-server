package pro.surpin.data.arrowflight.trino;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Calls the connector makes to the Arrow Flight SQL server. */
public interface FlightService extends AutoCloseable {

    /**
     * Lists schemas visible to the configured identity.
     *
     * @return remote schema names
     */
    List<String> listSchemas();

    /**
     * Lists tables of one schema with their Arrow schemas.
     *
     * @param schema remote schema name
     * @return remote table name to Arrow schema, in server order
     */
    Map<String, Schema> listTables(String schema);

    /**
     * Plans a query on the server.
     *
     * @param sql query text
     * @return result schema and endpoints
     */
    QueryEndpoints plan(String sql);

    /**
     * Opens a reader for one planned endpoint.
     *
     * @param split endpoint to read
     * @return batch reader; the stream opens on the first next call
     */
    BatchReader read(FlightSplit split);

    /** Releases client resources. */
    @Override
    void close();

    /** Sequential reader of Arrow batches from one endpoint. */
    interface BatchReader extends AutoCloseable {
        /**
         * Advances to the next non-empty batch.
         *
         * @return true when root holds a batch, false at the end of data
         * @throws IOException if the stream fails
         */
        boolean next() throws IOException;

        /**
         * Returns the current batch.
         *
         * @return batch valid until the next call to next or close
         */
        VectorSchemaRoot root();

        /** Closes the stream. */
        @Override
        void close();
    }
}
