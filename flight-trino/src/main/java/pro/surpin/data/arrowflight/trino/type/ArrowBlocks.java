package pro.surpin.data.arrowflight.trino.type;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;

/** Converts Arrow vectors to Trino blocks. */
public final class ArrowBlocks {
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final int MICROS_PER_MILLI = 1_000;
    private static final int NANOS_PER_MICRO = 1_000;
    private static final int PICOS_PER_NANO = 1_000;
    private static final int PICOS_PER_MICRO = 1_000_000;
    private static final long PICOS_PER_MILLI = 1_000_000_000L;
    private static final long PICOS_PER_SECOND = 1_000_000_000_000L;

    /** Writes one non-null cell into a block builder. */
    @FunctionalInterface
    private interface CellWriter {
        /**
         * Writes the cell at a row.
         *
         * @param builder target builder
         * @param row row index
         */
        void write(BlockBuilder builder, int row);
    }

    /** Prevents construction of a utility class. */
    private ArrowBlocks() { }

    /**
     * Converts the first rows of a vector into a block of the given Trino type.
     *
     * @param type Trino column type
     * @param vector Arrow vector; stream batches may use a narrower type than planned
     * @param rows number of rows
     * @return block
     * @throws IllegalArgumentException if the vector cannot be read as the type
     */
    public static Block toBlock(Type type, FieldVector vector, int rows) {
        BlockBuilder builder = type.createBlockBuilder(null, rows);
        CellWriter writer = writer(type, vector);
        for (int row = 0; row < rows; row++) {
            if (vector.isNull(row)) {
                builder.appendNull();
            } else {
                writer.write(builder, row);
            }
        }
        return builder.build();
    }

    /**
     * Chooses the cell writer for a type and vector pair.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     * @throws IllegalArgumentException if the pair is not supported
     */
    private static CellWriter writer(Type type, FieldVector vector) {
        if (type instanceof BooleanType) {
            BitVector bits = cast(vector, BitVector.class, type);
            return (builder, row) -> type.writeBoolean(builder, bits.get(row) != 0);
        }
        if (type instanceof BigintType || type instanceof IntegerType
                || type instanceof SmallintType || type instanceof TinyintType) {
            BaseIntVector ints = cast(vector, BaseIntVector.class, type);
            return (builder, row) -> type.writeLong(builder, ints.getValueAsLong(row));
        }
        if (type instanceof RealType) {
            return realWriter(type, vector);
        }
        if (type instanceof DoubleType) {
            return doubleWriter(type, vector);
        }
        if (type instanceof DecimalType decimal) {
            return decimalWriter(decimal, vector);
        }
        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            return (builder, row) -> type.writeSlice(builder, Slices.wrappedBuffer(bytes(vector, row)));
        }
        if (type instanceof DateType) {
            return dateWriter(type, vector);
        }
        if (type instanceof TimeType) {
            return (builder, row) -> type.writeLong(builder, picosOfDay(vector, row));
        }
        if (type instanceof TimestampType timestamp) {
            return timestampWriter(timestamp, cast(vector, TimeStampVector.class, type));
        }
        if (type instanceof TimestampWithTimeZoneType timestamp) {
            return timestampTzWriter(timestamp, cast(vector, TimeStampVector.class, type));
        }
        throw new IllegalArgumentException("Unsupported Trino type: " + type);
    }

    /**
     * Builds a REAL writer.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     */
    private static CellWriter realWriter(Type type, FieldVector vector) {
        if (vector instanceof Float4Vector floats) {
            return (builder, row) -> type.writeLong(builder, Float.floatToRawIntBits(floats.get(row)));
        }
        FloatingPointVector floats = cast(vector, FloatingPointVector.class, type);
        return (builder, row) -> type.writeLong(builder,
                Float.floatToRawIntBits((float) floats.getValueAsDouble(row)));
    }

    /**
     * Builds a DOUBLE writer.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     */
    private static CellWriter doubleWriter(Type type, FieldVector vector) {
        if (vector instanceof BaseIntVector ints) {
            return (builder, row) -> type.writeDouble(builder, ints.getValueAsLong(row));
        }
        FloatingPointVector floats = cast(vector, FloatingPointVector.class, type);
        return (builder, row) -> type.writeDouble(builder, floats.getValueAsDouble(row));
    }

    /**
     * Builds a DECIMAL writer.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     */
    private static CellWriter decimalWriter(DecimalType type, FieldVector vector) {
        return (builder, row) -> {
            BigInteger unscaled = decimal(vector, row)
                    .setScale(type.getScale(), RoundingMode.HALF_UP).unscaledValue();
            if (type.isShort()) {
                type.writeLong(builder, unscaled.longValueExact());
            } else {
                type.writeObject(builder, Int128.valueOf(unscaled));
            }
        };
    }

    /**
     * Builds a DATE writer.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     */
    private static CellWriter dateWriter(Type type, FieldVector vector) {
        if (vector instanceof DateMilliVector millis) {
            return (builder, row) -> type.writeLong(builder, Math.floorDiv(millis.get(row), MILLIS_PER_DAY));
        }
        DateDayVector days = cast(vector, DateDayVector.class, type);
        return (builder, row) -> type.writeLong(builder, days.get(row));
    }

    /**
     * Builds a TIMESTAMP writer.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     */
    private static CellWriter timestampWriter(TimestampType type, TimeStampVector vector) {
        TimeUnit unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
        return (builder, row) -> {
            long raw = vector.get(row);
            long micros = epochMicros(raw, unit);
            if (type.isShort()) {
                type.writeLong(builder, micros);
            } else {
                type.writeObject(builder, new LongTimestamp(micros, picosOfMicro(raw, unit)));
            }
        };
    }

    /**
     * Builds a TIMESTAMP WITH TIME ZONE writer; values are UTC instants.
     *
     * @param type Trino type
     * @param vector Arrow vector
     * @return cell writer
     */
    private static CellWriter timestampTzWriter(TimestampWithTimeZoneType type, TimeStampVector vector) {
        TimeUnit unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
        return (builder, row) -> {
            long raw = vector.get(row);
            long micros = epochMicros(raw, unit);
            long millis = Math.floorDiv(micros, MICROS_PER_MILLI);
            if (type.isShort()) {
                type.writeLong(builder, packDateTimeWithZone(millis, TimeZoneKey.UTC_KEY));
            } else {
                int picosOfMilli = Math.floorMod(micros, MICROS_PER_MILLI) * PICOS_PER_MICRO
                        + picosOfMicro(raw, unit);
                type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        millis, picosOfMilli, TimeZoneKey.UTC_KEY));
            }
        };
    }

    /**
     * Converts a raw Arrow timestamp to epoch microseconds, rounding down.
     *
     * @param raw raw value
     * @param unit Arrow unit
     * @return epoch microseconds
     */
    static long epochMicros(long raw, TimeUnit unit) {
        return switch (unit) {
            case SECOND -> Math.multiplyExact(raw, MICROS_PER_SECOND);
            case MILLISECOND -> Math.multiplyExact(raw, (long) MICROS_PER_MILLI);
            case MICROSECOND -> raw;
            case NANOSECOND -> Math.floorDiv(raw, NANOS_PER_MICRO);
        };
    }

    /**
     * Returns the picoseconds below one microsecond of a raw Arrow timestamp.
     *
     * @param raw raw value
     * @param unit Arrow unit
     * @return picoseconds of the microsecond
     */
    private static int picosOfMicro(long raw, TimeUnit unit) {
        return unit == TimeUnit.NANOSECOND ? Math.floorMod(raw, NANOS_PER_MICRO) * PICOS_PER_NANO : 0;
    }

    /**
     * Reads a time-of-day cell as picoseconds since midnight.
     *
     * @param vector Arrow time vector
     * @param row row index
     * @return picoseconds of day
     * @throws IllegalArgumentException if the vector is not a time vector
     */
    private static long picosOfDay(FieldVector vector, int row) {
        return switch (vector) {
            case TimeSecVector v -> v.get(row) * PICOS_PER_SECOND;
            case TimeMilliVector v -> v.get(row) * PICOS_PER_MILLI;
            case TimeMicroVector v -> v.get(row) * PICOS_PER_MICRO;
            case TimeNanoVector v -> v.get(row) * PICOS_PER_NANO;
            default -> throw new IllegalArgumentException("Not a time vector: " + vector.getField());
        };
    }

    /**
     * Reads a numeric cell as an exact decimal.
     *
     * @param vector Arrow vector
     * @param row row index
     * @return decimal value
     * @throws IllegalArgumentException if the vector is not numeric
     */
    private static BigDecimal decimal(FieldVector vector, int row) {
        return switch (vector) {
            case UInt8Vector v -> new BigDecimal(UInt8Vector.getNoOverflow(v.getDataBuffer(), row));
            case BaseIntVector v -> BigDecimal.valueOf(v.getValueAsLong(row));
            default -> {
                Object value = vector.getObject(row);
                if (value instanceof BigDecimal d) {
                    yield d;
                }
                throw new IllegalArgumentException("Not a decimal vector: " + vector.getField());
            }
        };
    }

    /**
     * Reads a string or binary cell as bytes.
     *
     * @param vector Arrow vector
     * @param row row index
     * @return UTF-8 text or raw binary bytes
     */
    private static byte[] bytes(FieldVector vector, int row) {
        return switch (vector) {
            case VarCharVector v -> v.get(row);
            case LargeVarCharVector v -> v.get(row);
            case VarBinaryVector v -> v.get(row);
            case LargeVarBinaryVector v -> v.get(row);
            case FixedSizeBinaryVector v -> v.get(row);
            default -> {
                Object value = vector.getObject(row);
                yield value instanceof byte[] b ? b : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            }
        };
    }

    /**
     * Casts a vector to the class a type needs.
     *
     * @param vector Arrow vector
     * @param expected expected class
     * @param type Trino type
     * @return cast vector
     * @throws IllegalArgumentException if the vector has another class
     */
    private static <T> T cast(FieldVector vector, Class<T> expected, Type type) {
        if (!expected.isInstance(vector)) {
            throw new IllegalArgumentException("Cannot read Arrow " + vector.getField().getType()
                    + " as Trino " + type);
        }
        return expected.cast(vector);
    }
}
