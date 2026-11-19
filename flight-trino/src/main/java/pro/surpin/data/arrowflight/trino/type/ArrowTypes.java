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
        return toTrino(field, null);
    }

    /**
     * Maps logical source types using the engine's registered type instances.
     *
     * @param field Arrow field
     * @param manager engine type manager, or null for SPI scalar types only
     * @return compatible logical type, or empty for unsupported representations
     */
    public static Optional<Type> toTrino(Field field, io.trino.spi.type.TypeManager manager) {
        if (field.getDictionary() != null || field.getMetadata().containsKey("ARROW:extension:name")) {
            return Optional.empty();
        }
        String logical = field.getMetadata().get("arrowflight:logical_type");
        if (logical != null) {
            if ("uuid".equals(logical) && field.getType() instanceof ArrowType.Utf8) {
                return Optional.of(io.trino.spi.type.UuidType.UUID);
            }
            if (logical.matches("char\\([0-9]{1,5}\\)") && field.getType() instanceof ArrowType.Utf8) {
                int length = Integer.parseInt(logical.substring(5, logical.length() - 1));
                return length <= 65_536 ? Optional.of(io.trino.spi.type.CharType.createCharType(length)) : Optional.empty();
            }
            boolean integer = field.getType() instanceof ArrowType.Int physical
                    && physical.getIsSigned() && physical.getBitWidth() == 64;
            String name = switch (logical) {
                case "interval_year_month" -> field.getType() instanceof ArrowType.Int month
                        && month.getIsSigned() && month.getBitWidth() == 32
                        ? io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH : null;
                case "interval_day_time" -> integer ? io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND : null;
                case "ipaddress" -> field.getType() instanceof ArrowType.FixedSizeBinary binary
                        && binary.getByteWidth() == 16 ? io.trino.spi.type.StandardTypes.IPADDRESS : null;
                default -> null;
            };
            return name == null || manager == null ? Optional.empty()
                    : Optional.ofNullable(manager.getType(new io.trino.spi.type.TypeSignature(name)));
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
                    : t.getUnit() == TimeUnit.NANOSECOND ? null
                            : TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision(t.getUnit()));
            case ArrowType.List ignored -> array(field, manager);
            case ArrowType.LargeList ignored -> array(field, manager);
            case ArrowType.FixedSizeList ignored -> array(field, manager);
            case ArrowType.Struct ignored -> row(field, manager);
            case ArrowType.Map ignored -> map(field, manager);

            default -> null;
        });
    }

    /**
     * Maps a list whose element type is supported.
     * @param field list field
     * @param manager engine type manager, or null
     * @return array type, or null for unsupported elements
     */
    private static Type array(Field field, io.trino.spi.type.TypeManager manager) {
        return field.getChildren().size() == 1
                ? toTrino(field.getChildren().getFirst(), manager).map(ArrayType::new).orElse(null) : null;
    }

    /**
     * Maps a structure without dropping unsupported members.
     * @param field structure field
     * @param manager engine type manager, or null
     * @return row type, or null for unsupported members
     */
    private static Type row(Field field, io.trino.spi.type.TypeManager manager) {
        List<RowType.Field> fields = new ArrayList<>();
        for (Field child : field.getChildren()) {
            Optional<Type> type = toTrino(child, manager);
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
     * @param manager engine type manager, or null
     * @return map type, or null for unsupported entries
     */
    private static Type map(Field field, io.trino.spi.type.TypeManager manager) {
        if (field.getChildren().size() != 1) {
            return null;
        }
        Field entries = field.getChildren().getFirst();
        if (!(entries.getType() instanceof ArrowType.Struct) || entries.getChildren().size() != 2) {
            return null;
        }
        Optional<Type> key = toTrino(entries.getChildren().get(0), manager);
        Optional<Type> value = toTrino(entries.getChildren().get(1), manager);
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
