package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Narrow wire form of DECIMAL columns. Arrow decimals are always 128 bits wide, but a DECIMAL(p, s) with p up to 18
 * fits an unscaled 64-bit integer (p up to 9: 32-bit), so a client that knows the column's declared type can read
 * half or a quarter of the bytes. The stream schema says so: the field becomes an integer carrying
 * {@code decimal.precision} and {@code decimal.scale} metadata. Columns wider than 18 digits are left as they are.
 */
final class DecimalNarrowing {
    static final String PRECISION_KEY = "decimal.precision";
    static final String SCALE_KEY = "decimal.scale";
    private static final int MAX_LONG_PRECISION = 18;
    private static final int MAX_INT_PRECISION = 9;

    private DecimalNarrowing() { }

    private static boolean narrowable(Field field) {
        return field.getType() instanceof ArrowType.Decimal decimal
                && decimal.getBitWidth() == 128 && decimal.getPrecision() <= MAX_LONG_PRECISION
                && decimal.getScale() >= 0;
    }

    /**
     * Whether any column of the schema has a narrow wire form.
     *
     * @param schema source schema
     * @return true when at least one DECIMAL column of up to 18 digits exists
     */
    static boolean applicable(Schema schema) {
        return schema.getFields().stream().anyMatch(DecimalNarrowing::narrowable);
    }

    private static Field narrowedField(Field field) {
        ArrowType.Decimal decimal = (ArrowType.Decimal) field.getType();
        ArrowType integer = new ArrowType.Int(decimal.getPrecision() <= MAX_INT_PRECISION ? 32 : 64, true);
        Map<String, String> metadata = Map.of(PRECISION_KEY, Integer.toString(decimal.getPrecision()),
                SCALE_KEY, Integer.toString(decimal.getScale()));
        return new Field(field.getName(), new FieldType(field.isNullable(), integer, null, metadata), null);
    }

    /** Copies each batch of a source root into a root whose narrowable decimal columns are integers. */
    static final class Narrower implements AutoCloseable {
        private final VectorSchemaRoot source;
        private final VectorSchemaRoot narrowed;
        private final boolean[] isNarrow;

        Narrower(BufferAllocator allocator, VectorSchemaRoot source) {
            this.source = source;
            List<Field> fields = new ArrayList<>();
            List<FieldVector> vectors = new ArrayList<>();
            this.isNarrow = new boolean[source.getFieldVectors().size()];
            for (int i = 0; i < isNarrow.length; i++) {
                FieldVector vector = source.getFieldVectors().get(i);
                if (narrowable(vector.getField())) {
                    isNarrow[i] = true;
                    Field field = narrowedField(vector.getField());
                    fields.add(field);
                    vectors.add(field.createVector(allocator));
                } else {
                    fields.add(vector.getField());
                    vectors.add(vector);
                }
            }
            this.narrowed = new VectorSchemaRoot(new Schema(fields), vectors, 0);
        }

        /** @return the root to hand to the Flight stream */
        VectorSchemaRoot root() {
            return narrowed;
        }

        /** Fills the narrowed root from the source root's current batch. */
        void copy() {
            int rows = source.getRowCount();
            for (int i = 0; i < isNarrow.length; i++) {
                if (isNarrow[i]) {
                    narrow((DecimalVector) source.getFieldVectors().get(i), narrowed.getFieldVectors().get(i), rows);
                }
            }
            narrowed.setRowCount(rows);
        }

        private static void narrow(DecimalVector from, FieldVector to, int rows) {
            to.clear();
            to.setInitialCapacity(Math.max(rows, 1));
            to.allocateNew();
            int validityBytes = (rows + 7) / 8;
            if (validityBytes > 0) {
                to.getValidityBuffer().setBytes(0, from.getValidityBuffer(), 0, validityBytes);
            }
            if (to instanceof BigIntVector) {
                for (int r = 0; r < rows; r++) {
                    // the low 64 bits of the little-endian 128-bit value are the unscaled integer for p <= 18
                    to.getDataBuffer().setLong((long) r * Long.BYTES, from.getDataBuffer().getLong((long) r * 16));
                }
            } else {
                for (int r = 0; r < rows; r++) {
                    to.getDataBuffer().setInt((long) r * Integer.BYTES,
                            (int) from.getDataBuffer().getLong((long) r * 16));
                }
            }
            to.setValueCount(rows);
        }

        @Override
        public void close() {
            for (int i = 0; i < isNarrow.length; i++) {
                if (isNarrow[i]) {
                    narrowed.getFieldVectors().get(i).close();
                }
            }
        }
    }
}
