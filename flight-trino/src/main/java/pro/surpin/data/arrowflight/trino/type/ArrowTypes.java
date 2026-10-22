package pro.surpin.data.arrowflight.trino.type;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
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
import java.util.ArrayList;
import java.util.List;

/** Maps Arrow field types to Trino types. */
public final class ArrowTypes {
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int UNSIGNED_BIGINT_PRECISION = 20;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    /** Prevents construction of a utility class. */
    private ArrowTypes() { }

    /**
     * Maps an Arrow field to a Trino type.
     *
     * @param field Arrow field
     * @return Trino type, or empty when the connector cannot read the field
     */
    public static Optional<Type> toTrino(Field field) {
        if (field.getDictionary() != null) {
            return Optional.empty();
        }
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
            case ArrowType.Decimal d -> d.getPrecision() >= 1 && d.getPrecision() <= MAX_DECIMAL_PRECISION
                    && d.getScale() >= 0 && d.getScale() <= d.getPrecision()
                    ? DecimalType.createDecimalType(d.getPrecision(), d.getScale()) : null;
            case ArrowType.Date ignored -> DateType.DATE;
            case ArrowType.Time t -> TimeType.createTimeType(precision(t.getUnit()));
            case ArrowType.Timestamp t -> t.getTimezone() == null
                    ? TimestampType.createTimestampType(precision(t.getUnit()))
                    : TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision(t.getUnit()));
            case ArrowType.List ignored -> array(field);
            case ArrowType.LargeList ignored -> array(field);
            case ArrowType.FixedSizeList ignored -> array(field);
            case ArrowType.Struct ignored -> row(field);
            case ArrowType.Map ignored -> map(field);
            default -> null;
        });
    }

    /**
     * Maps a list whose element type is supported.
     * @param field list field
     * @return array type, or null for unsupported elements
     */
    private static Type array(Field field) {
        return field.getChildren().size() == 1
                ? toTrino(field.getChildren().getFirst()).map(ArrayType::new).orElse(null) : null;
    }

    /**
     * Maps a structure without dropping unsupported members.
     * @param field structure field
     * @return row type, or null for unsupported members
     */
    private static Type row(Field field) {
        List<RowType.Field> fields = new ArrayList<>();
        for (Field child : field.getChildren()) {
            Optional<Type> type = toTrino(child);
            if (type.isEmpty()) {
                return null;
            }
            fields.add(RowType.field(child.getName(), type.orElseThrow()));
        }
        return fields.isEmpty() ? null : RowType.from(fields);
    }

    /**
     * Maps a map with supported keys and values.
     * @param field map field
     * @return map type, or null for unsupported entries
     */
    private static Type map(Field field) {
        if (field.getChildren().size() != 1) {
            return null;
        }
        Field entries = field.getChildren().getFirst();
        if (!(entries.getType() instanceof ArrowType.Struct) || entries.getChildren().size() != 2) {
            return null;
        }
        Optional<Type> key = toTrino(entries.getChildren().get(0));
        Optional<Type> value = toTrino(entries.getChildren().get(1));
        if (key.isEmpty() || value.isEmpty() || !key.orElseThrow().isComparable()) {
            return null;
        }
        return new MapType(key.orElseThrow(), value.orElseThrow(), TYPE_OPERATORS);
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
