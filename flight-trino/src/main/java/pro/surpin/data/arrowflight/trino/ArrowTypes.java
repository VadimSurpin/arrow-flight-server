package pro.surpin.data.arrowflight.trino;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

/** Maps Arrow field types to Trino types. */
final class ArrowTypes {
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int UNSIGNED_BIGINT_PRECISION = 20;

    /** Prevents construction of a utility class. */
    private ArrowTypes() { }

    /**
     * Maps an Arrow field to a Trino type.
     *
     * @param field Arrow field
     * @return Trino type, or empty when the connector cannot read the field
     */
    static Optional<Type> toTrino(Field field) {
        return Optional.ofNullable(switch (field.getType()) {
            case ArrowType.Bool ignored -> BooleanType.BOOLEAN;
            case ArrowType.Int i -> integer(i);
            case ArrowType.FloatingPoint f -> switch (f.getPrecision()) {
                case SINGLE -> RealType.REAL;
                case DOUBLE -> DoubleType.DOUBLE;
                case HALF -> null;
            };
            case ArrowType.Utf8 ignored -> VarcharType.VARCHAR;
            case ArrowType.LargeUtf8 ignored -> VarcharType.VARCHAR;
            case ArrowType.Binary ignored -> VarbinaryType.VARBINARY;
            case ArrowType.LargeBinary ignored -> VarbinaryType.VARBINARY;
            case ArrowType.FixedSizeBinary ignored -> VarbinaryType.VARBINARY;
            case ArrowType.Decimal d -> d.getPrecision() <= MAX_DECIMAL_PRECISION
                    ? DecimalType.createDecimalType(d.getPrecision(), d.getScale()) : null;
            case ArrowType.Date ignored -> DateType.DATE;
            case ArrowType.Time t -> TimeType.createTimeType(precision(t.getUnit()));
            case ArrowType.Timestamp t -> t.getTimezone() == null
                    ? TimestampType.createTimestampType(precision(t.getUnit()))
                    : TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision(t.getUnit()));
            default -> null;
        });
    }

    /**
     * Maps an Arrow integer to the narrowest Trino type that holds every value.
     *
     * @param type Arrow integer type
     * @return Trino type
     */
    private static Type integer(ArrowType.Int type) {
        int bits = type.getIsSigned() ? type.getBitWidth() : type.getBitWidth() * 2;
        return switch (bits) {
            case 8 -> TinyintType.TINYINT;
            case 16 -> SmallintType.SMALLINT;
            case 32 -> IntegerType.INTEGER;
            case 64 -> BigintType.BIGINT;
            default -> DecimalType.createDecimalType(UNSIGNED_BIGINT_PRECISION, 0);
        };
    }

    /**
     * Returns the fractional-second precision of an Arrow time unit.
     *
     * @param unit Arrow time unit
     * @return digits after the decimal point
     */
    static int precision(TimeUnit unit) {
        return switch (unit) {
            case SECOND -> 0;
            case MILLISECOND -> 3;
            case MICROSECOND -> 6;
            case NANOSECOND -> 9;
        };
    }
}
