package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.model.FieldType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;

/**
 * Spark column adapter backed by the Arrow vectors owned by a Flight stream.
 *
 * <p>This deliberately does not use Spark's {@code ArrowColumnVector}. Spark
 * 3.5.1 was compiled against Arrow 12, while this connector and Flight use
 * Arrow 18. Passing an Arrow 18 {@code ValueVector} into Spark's adapter crosses
 * class-loader/version boundaries and fails at runtime even though the source
 * compiles. This adapter keeps every Arrow type on the connector side and
 * exposes only Spark's stable scalar {@link ColumnVector} API.</p>
 */
final class FlightArrowColumnVector extends ColumnVector {
    private final FieldVector vector;

    FlightArrowColumnVector(FieldVector vector) {
        super(toSparkType(vector));
        this.vector = vector;
    }

    /**
     * The FlightStream owns the vector and releases it after the batch is consumed.
     */
    @Override
    public void close() {
        // no-op
    }

    @Override
    public boolean hasNull() {
        return this.vector.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
        return this.vector.getNullCount();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return this.vector.isNull(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return require(BitVector.class).get(rowId) != 0;
    }

    @Override
    public byte getByte(int rowId) {
        return require(TinyIntVector.class).get(rowId);
    }

    @Override
    public short getShort(int rowId) {
        if (this.vector instanceof TinyIntVector v) {
            return v.get(rowId);
        }
        if (this.vector instanceof UInt1Vector uintVector) {
            return (short) (uintVector.get(rowId) & 0xFF);
        }
        return require(SmallIntVector.class).get(rowId);
    }

    @Override
    public int getInt(int rowId) {
        if (this.vector instanceof IntVector intVector) {
            return intVector.get(rowId);
        }
        if (this.vector instanceof DateDayVector dateVector) {
            return dateVector.get(rowId);
        }
        if (this.vector instanceof DateMilliVector dateVector) {
            return Math.toIntExact(Math.floorDiv(dateVector.get(rowId), 86_400_000L));
        }
        if (this.vector instanceof UInt2Vector uintVector) {
            return uintVector.get(rowId) & 0xFFFF;
        }
        throw unsupported("getInt");
    }

    @Override
    public long getLong(int rowId) {
        if (this.vector instanceof BigIntVector longVector) {
            return longVector.get(rowId);
        }
        if (this.vector instanceof TimeStampMicroTZVector timestampVector) {
            return timestampVector.get(rowId);
        }
        if (this.vector instanceof TimeStampMicroVector timestampVector) {
            return timestampVector.get(rowId);
        }
        if (this.vector instanceof TimeStampMilliVector timestampVector) {
            return Math.multiplyExact(timestampVector.get(rowId), 1_000L);
        }
        if (this.vector instanceof TimeStampMilliTZVector timestampVector) {
            return Math.multiplyExact(timestampVector.get(rowId), 1_000L);
        }
        if (this.vector instanceof TimeStampSecVector timestampVector) {
            return Math.multiplyExact(timestampVector.get(rowId), 1_000_000L);
        }
        if (this.vector instanceof TimeStampSecTZVector timestampVector) {
            return Math.multiplyExact(timestampVector.get(rowId), 1_000_000L);
        }
        if (this.vector instanceof TimeStampNanoVector timestampVector) {
            return Math.floorDiv(timestampVector.get(rowId), 1_000L);
        }
        if (this.vector instanceof TimeStampNanoTZVector timestampVector) {
            return Math.floorDiv(timestampVector.get(rowId), 1_000L);
        }
        if (this.vector instanceof UInt4Vector uintVector) {
            return Integer.toUnsignedLong(uintVector.get(rowId));
        }
        if (this.vector instanceof UInt8Vector uintVector) {
            return uintVector.get(rowId);
        }
        throw unsupported("getLong");
    }

    @Override
    public float getFloat(int rowId) {
        return require(Float4Vector.class).get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return require(Float8Vector.class).get(rowId);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (this.isNullAt(rowId)) {
            return null;
        }
        BigDecimal value = require(DecimalVector.class).getObject(rowId);
        return Decimal.apply(value, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        if (this.isNullAt(rowId)) {
            return null;
        }
        if (this.vector instanceof VarCharVector stringVector) {
            long offsetIndex = (long) rowId * Integer.BYTES;
            int start = stringVector.getOffsetBuffer().getInt(offsetIndex);
            int end = stringVector.getOffsetBuffer().getInt(offsetIndex + Integer.BYTES);
            return UTF8String.fromAddress(
                    null,
                    stringVector.getDataBuffer().memoryAddress() + start,
                    end - start);
        }
        if (this.vector instanceof LargeVarCharVector stringVector) {
            long offsetIndex = (long) rowId * Long.BYTES;
            long start = stringVector.getOffsetBuffer().getLong(offsetIndex);
            long end = stringVector.getOffsetBuffer().getLong(offsetIndex + Long.BYTES);
            return UTF8String.fromAddress(
                    null,
                    stringVector.getDataBuffer().memoryAddress() + start,
                    Math.toIntExact(end - start));
        }
        throw unsupported("getUTF8String");
    }

    @Override
    public byte[] getBinary(int rowId) {
        if (this.isNullAt(rowId)) {
            return null;
        }
        if (this.vector instanceof VarBinaryVector binaryVector) {
            return binaryVector.get(rowId);
        }
        if (this.vector instanceof LargeVarBinaryVector binaryVector) {
            return binaryVector.get(rowId);
        }
        if (this.vector instanceof FixedSizeBinaryVector binaryVector) {
            return binaryVector.get(rowId);
        }
        throw unsupported("getBinary");
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        throw unsupported("getArray");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
        throw unsupported("getMap");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        throw unsupported("getChild");
    }

    private static DataType toSparkType(FieldVector vector) {
        if (vector == null || !matchesArrowType(vector)) {
            String actual = vector == null ? "null" : vector.getClass().getName();
            throw new IllegalArgumentException("Unsupported Arrow vector for Spark columnar read: "
                    + actual);
        }
        return FieldType.toSpark(FieldType.fromArrow(
                vector.getField().getType(), vector.getField().getChildren()));
    }

    private static boolean matchesArrowType(FieldVector vector) {
        ArrowType type = vector.getField().getType();
        return switch (type.getTypeID()) {
            case Bool -> vector instanceof BitVector;
            case Utf8 -> vector instanceof VarCharVector;
            case LargeUtf8 -> vector instanceof LargeVarCharVector;
            case Binary -> vector instanceof VarBinaryVector;
            case LargeBinary -> vector instanceof LargeVarBinaryVector;
            case FixedSizeBinary -> vector instanceof FixedSizeBinaryVector;
            case Null -> vector instanceof NullVector;
            case FloatingPoint -> {
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
                yield (precision == FloatingPointPrecision.SINGLE && vector instanceof Float4Vector)
                        || (precision == FloatingPointPrecision.DOUBLE
                        && vector instanceof Float8Vector);
            }
            case Int -> {
                ArrowType.Int integer = (ArrowType.Int) type;
                if (integer.getIsSigned()) {
                    yield switch (integer.getBitWidth()) {
                        case 8 -> vector instanceof TinyIntVector;
                        case 16 -> vector instanceof SmallIntVector;
                        case 32 -> vector instanceof IntVector;
                        case 64 -> vector instanceof BigIntVector;
                        default -> false;
                    };
                }
                yield switch (integer.getBitWidth()) {
                    case 8 -> vector instanceof UInt1Vector;
                    case 16 -> vector instanceof UInt2Vector;
                    case 32 -> vector instanceof UInt4Vector;
                    case 64 -> vector instanceof UInt8Vector;
                    default -> false;
                };
            }
            case Decimal -> {
                ArrowType.Decimal decimal = (ArrowType.Decimal) type;
                int precision = decimal.getPrecision();
                int scale = decimal.getScale();
                yield decimal.getBitWidth() == 128
                        && precision >= 1 && precision <= 38
                        && scale >= 0 && scale <= precision
                        && vector instanceof DecimalVector;
            }
            case Date -> {
                DateUnit unit = ((ArrowType.Date) type).getUnit();
                yield (unit == DateUnit.DAY && vector instanceof DateDayVector)
                        || (unit == DateUnit.MILLISECOND && vector instanceof DateMilliVector);
            }
            case Timestamp -> {
                ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
                String tz = timestamp.getTimezone();
                yield switch (timestamp.getUnit()) {
                    case SECOND -> tz != null ? vector instanceof TimeStampSecTZVector
                            : vector instanceof TimeStampSecVector;
                    case MILLISECOND -> tz != null ? vector instanceof TimeStampMilliTZVector
                            : vector instanceof TimeStampMilliVector;
                    case MICROSECOND -> tz != null ? vector instanceof TimeStampMicroTZVector
                            : vector instanceof TimeStampMicroVector;
                    case NANOSECOND -> tz != null ? vector instanceof TimeStampNanoTZVector
                            : vector instanceof TimeStampNanoVector;
                };
            }
            default -> false;
        };
    }

    private <T extends FieldVector> T require(Class<T> expectedType) {
        if (!expectedType.isInstance(this.vector)) {
            throw unsupported(expectedType.getSimpleName());
        }
        return expectedType.cast(this.vector);
    }

    private UnsupportedOperationException unsupported(String operation) {
        return new UnsupportedOperationException(operation + " is not supported for Arrow vector "
                + this.vector.getClass().getName());
    }
}
