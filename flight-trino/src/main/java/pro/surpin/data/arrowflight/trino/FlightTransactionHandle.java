package pro.surpin.data.arrowflight.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

/** Transaction handle of the read-only connector. */
public enum FlightTransactionHandle implements ConnectorTransactionHandle {
    /** The only transaction state; reads are not transactional. */
    INSTANCE
}
