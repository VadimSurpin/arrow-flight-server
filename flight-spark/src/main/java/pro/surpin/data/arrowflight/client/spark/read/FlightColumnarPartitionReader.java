package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * DataSource V2 reader that exposes Flight's Arrow batches directly to Spark.
 * The FlightStream remains the sole owner of Arrow vectors and buffers.
 */
public final class FlightColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
    private final FlightPartitionReader streamReader;
    private final Schema expectedSchema;

    private ColumnarBatch current;
    private VectorSchemaRoot currentRoot;
    private boolean schemaValidated;

    public FlightColumnarPartitionReader(Configuration configuration, InputPartition partition) {
        if (!(partition instanceof FlightInputPartition flightPartition)) {
            throw new IllegalArgumentException("Unsupported Flight input partition: " + partition);
        }
        try {
            this.expectedSchema = flightPartition.getSchema();
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid Flight partition schema", e);
        }
        this.streamReader = new FlightPartitionReader(configuration, partition);
    }

    FlightColumnarPartitionReader(FlightPartitionReader streamReader, Schema expectedSchema) {
        this.streamReader = streamReader;
        this.expectedSchema = expectedSchema;
    }

    @Override
    public boolean next() throws IOException {
        boolean hasBatch = this.streamReader.nextBatch();
        if (!hasBatch) {
            this.current = null;
            this.currentRoot = null;
            return false;
        }

        VectorSchemaRoot root = this.streamReader.currentBatch();
        if (!this.schemaValidated) {
            Schema actualSchema = root.getSchema();
            if (!compatible(this.expectedSchema, actualSchema)) {
                this.streamReader.close();
                throw new IOException("FlightInfo schema differs from Flight stream schema. Expected "
                        + this.expectedSchema + ", got " + actualSchema);
            }
            this.schemaValidated = true;
        }

        if (this.current == null || this.currentRoot != root) {
            ColumnVector[] columns = root.getFieldVectors().stream()
                    .map(FlightArrowColumnVector::new)
                    .toArray(ColumnVector[]::new);
            this.current = new FlightOwnedColumnarBatch(columns, root.getRowCount());
            this.currentRoot = root;
        } else {
            this.current.setNumRows(root.getRowCount());
        }
        return true;
    }

    @Override
    public ColumnarBatch get() {
        if (this.current == null) {
            throw new IllegalStateException("No current Arrow batch. Call next() first.");
        }
        return this.current;
    }

    @Override
    public void close() {
        this.current = null;
        this.currentRoot = null;
        this.streamReader.close();
    }

    private static boolean compatible(Schema expected, Schema actual) {
        List<Field> expectedFields = expected.getFields();
        List<Field> actualFields = actual.getFields();
        if (expectedFields.size() != actualFields.size()) {
            return false;
        }
        for (int i = 0; i < expectedFields.size(); i++) {
            if (!compatible(expectedFields.get(i), actualFields.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean compatible(Field expected, Field actual) {
        if (!expected.getName().equalsIgnoreCase(actual.getName())
                || !expected.getType().equals(actual.getType())) {
            return false;
        }
        List<Field> expectedChildren = expected.getChildren();
        List<Field> actualChildren = actual.getChildren();
        if (expectedChildren.size() != actualChildren.size()) {
            return false;
        }
        for (int i = 0; i < expectedChildren.size(); i++) {
            if (!compatible(expectedChildren.get(i), actualChildren.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Spark may close a consumed batch. The underlying vectors belong to the
     * FlightStream, so only the partition reader closes them.
     */
    private static final class FlightOwnedColumnarBatch extends ColumnarBatch {
        private FlightOwnedColumnarBatch(ColumnVector[] columns, int numRows) {
            super(columns, numRows);
        }

        @Override
        public void close() {
            // Ownership remains with FlightPartitionReader/FlightStream.
        }
    }
}
