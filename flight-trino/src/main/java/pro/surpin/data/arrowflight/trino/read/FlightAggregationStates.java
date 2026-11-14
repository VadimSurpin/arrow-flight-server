package pro.surpin.data.arrowflight.trino.read;

import pro.surpin.data.arrowflight.trino.metadata.AggregateSpec;
import pro.surpin.data.arrowflight.trino.type.ArrowToTrinoBlockConverter;
import pro.surpin.data.arrowflight.trino.type.ArrowTypes;

import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.DecimalType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;

/** Assembles validated Arrow aggregate results into native Trino states. */
final class FlightAggregationStates {
    /** Prevents construction of a utility class. */
    private FlightAggregationStates() { }

    /**
     * Reads one aggregate state column.
     *
     * @param specification partial aggregate contract
     * @param batch current Arrow batch
     * @return native state block
     * @throws IllegalStateException if remote states violate the contract
     */
    static Block read(AggregateSpec specification, VectorSchemaRoot batch) {
        var remote = specification.remoteAggregates();
        FieldVector value = batch.getVector(remote.getFirst().output().remoteName());
        if (value == null || !ArrowTypes.toTrino(value.getField())
                .filter(remote.getFirst().output().type()::equals).isPresent()
                || value.getValueCount() != batch.getRowCount()) {
            throw new IllegalStateException("Flight aggregate state has an incompatible column: " + remote.getFirst().output().remoteName());
        }
        return switch (specification.stateFormat()) {
            case COUNT -> {
                validateCounts(value, batch.getRowCount());
                yield ArrowToTrinoBlockConverter.toBlock(BIGINT, value, batch.getRowCount());
            }
            case SCALAR -> {
                if (specification.output().type() instanceof DecimalType decimal) {
                    validateDecimal((DecimalVector) value, decimal, batch.getRowCount());
                }
                yield ArrowToTrinoBlockConverter.toBlock(specification.output().type(), value, batch.getRowCount());
            }
            case LONG_SUM -> longSum((DecimalVector) value, batch.getRowCount());
            case AVERAGE -> average(specification, batch, (Float8Vector) value);
            case NEGATED_EXTREMUM -> negateExtremum(specification, value, batch.getRowCount());
        };
    }

    /**
     * Restores floating-point maxima from negated minima.
     *
     * @param specification extrema contract
     * @param value negated minimum vector
     * @param rows row count
     * @return native maximum state block
     */
    private static Block negateExtremum(AggregateSpec specification, FieldVector value, int rows) {
        var type = specification.output().type();
        var builder = type.createBlockBuilder(null, rows);
        for (int row = 0; row < rows; row++) {
            if (value.isNull(row)) {
                builder.appendNull();
            } else if (type.equals(REAL)) {
                REAL.writeLong(builder, Float.floatToRawIntBits(-((Float4Vector) value).get(row)));
            } else {
                DOUBLE.writeDouble(builder, -((Float8Vector) value).get(row));
            }
        }
        return builder.build();
    }

    /**
     * Validates exact decimal extrema against their declared precision.
     *
     * @param values remote decimal values
     * @param type declared decimal type
     * @param rows row count
     * @throws IllegalStateException if a value exceeds the declared precision
     */
    private static void validateDecimal(DecimalVector values, DecimalType type, int rows) {
        for (int row = 0; row < rows; row++) {
            if (!values.isNull(row) && values.getObject(row).precision() > type.getPrecision()) {
                throw new IllegalStateException("Flight decimal extremum exceeds declared precision");
            }
        }
    }

    /**
     * Creates a native state for an empty global input.
     *
     * @param specification partial aggregate contract
     * @return one-position native state block
     */
    static Block empty(AggregateSpec specification) {
        var builder = specification.output().type().createBlockBuilder(null, 1);
        switch (specification.emptyState()) {
            case ZERO -> BIGINT.writeLong(builder, 0L);
            case NULL -> builder.appendNull();
            case AVERAGE_ZERO -> ((RowBlockBuilder) builder).buildEntry(fields -> {
                DOUBLE.writeDouble(fields.get(0), 0.0);
                BIGINT.writeLong(fields.get(1), 0L);
            });
            default -> throw new IllegalStateException("Unsupported empty aggregate state");
        }
        return builder.build();
    }

    /**
     * Converts exact decimal partial sums to checked BIGINT states.
     *
     * @param sums exact remote sums
     * @param rows row count
     * @return nullable BIGINT states
     * @throws ArithmeticException if a partial sum exceeds BIGINT range
     */
    private static Block longSum(DecimalVector sums, int rows) {
        var builder = BIGINT.createFixedSizeBlockBuilder(rows);
        for (int row = 0; row < rows; row++) {
            if (sums.isNull(row)) {
                builder.appendNull();
            } else {
                BIGINT.writeLong(builder, sums.getObject(row).longValueExact());
            }
        }
        return builder.build();
    }

    /**
     * Builds the serialized average state in Trino 446 field order.
     *
     * @param specification average contract
     * @param batch current Arrow batch
     * @param sums partial sums
     * @return native row states
     * @throws IllegalStateException if sums and counts are inconsistent
     */
    private static Block average(AggregateSpec specification, VectorSchemaRoot batch, Float8Vector sums) {
        FieldVector countVector = batch.getVector(specification.remoteAggregates().get(1).output().remoteName());
        validateCounts(countVector, batch.getRowCount());
        BigIntVector counts = (BigIntVector) countVector;
        RowBlockBuilder builder = (RowBlockBuilder) specification.output().type().createBlockBuilder(null, batch.getRowCount());
        for (int row = 0; row < batch.getRowCount(); row++) {
            long count = counts.get(row);
            if (count > 0 && sums.isNull(row) || count == 0 && !sums.isNull(row) && sums.get(row) != 0.0) {
                throw new IllegalStateException("Flight AVG sum and count are inconsistent");
            }
            double sum = sums.isNull(row) ? 0.0 : sums.get(row);
            builder.buildEntry(fields -> {
                DOUBLE.writeDouble(fields.get(0), sum);
                BIGINT.writeLong(fields.get(1), count);
            });
        }
        return builder.build();
    }

    /**
     * Checks signed, non-null and nonnegative count values.
     *
     * @param value remote count vector
     * @param rows expected row count
     * @throws IllegalStateException if a count state is malformed
     */
    private static void validateCounts(FieldVector value, int rows) {
        if (!(value instanceof BigIntVector counts) || counts.getValueCount() != rows) {
            throw new IllegalStateException("Flight count state must be a signed BIGINT");
        }
        for (int row = 0; row < rows; row++) {
            if (counts.isNull(row) || counts.get(row) < 0) {
                throw new IllegalStateException("Flight count state must be non-null and nonnegative");
            }
        }
    }
}
