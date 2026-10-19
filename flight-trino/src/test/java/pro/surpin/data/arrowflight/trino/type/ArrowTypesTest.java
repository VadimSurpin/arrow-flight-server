package pro.surpin.data.arrowflight.trino.type;

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
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class ArrowTypesTest {

    private static Optional<Type> map(ArrowType type) {
        return ArrowTypes.toTrino(new Field("c", FieldType.nullable(type), null));
    }

    @Test
    void mapsScalarTypes() {
        assertEquals(Optional.of(BooleanType.BOOLEAN), map(ArrowType.Bool.INSTANCE));
        assertEquals(Optional.of(TinyintType.TINYINT), map(new ArrowType.Int(8, true)));
        assertEquals(Optional.of(SmallintType.SMALLINT), map(new ArrowType.Int(16, true)));
        assertEquals(Optional.of(IntegerType.INTEGER), map(new ArrowType.Int(32, true)));
        assertEquals(Optional.of(BigintType.BIGINT), map(new ArrowType.Int(64, true)));
        assertEquals(Optional.of(RealType.REAL), map(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        assertEquals(Optional.of(DoubleType.DOUBLE), map(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        assertEquals(Optional.of(VarcharType.VARCHAR), map(ArrowType.Utf8.INSTANCE));
        assertEquals(Optional.of(VarcharType.VARCHAR), map(ArrowType.LargeUtf8.INSTANCE));
        assertEquals(Optional.of(VarbinaryType.VARBINARY), map(ArrowType.Binary.INSTANCE));
        assertEquals(Optional.of(VarbinaryType.VARBINARY), map(ArrowType.LargeBinary.INSTANCE));
        assertEquals(Optional.of(VarbinaryType.VARBINARY), map(new ArrowType.FixedSizeBinary(4)));
        assertEquals(Optional.of(DateType.DATE), map(new ArrowType.Date(DateUnit.DAY)));
        assertEquals(Optional.of(DateType.DATE), map(new ArrowType.Date(DateUnit.MILLISECOND)));
    }

    @Test
    void widensUnsignedIntegers() {
        assertEquals(Optional.of(SmallintType.SMALLINT), map(new ArrowType.Int(8, false)));
        assertEquals(Optional.of(IntegerType.INTEGER), map(new ArrowType.Int(16, false)));
        assertEquals(Optional.of(BigintType.BIGINT), map(new ArrowType.Int(32, false)));
        assertEquals(Optional.of(DecimalType.createDecimalType(20, 0)), map(new ArrowType.Int(64, false)));
    }

    @Test
    void mapsDecimalsWithinTrinoPrecision() {
        assertEquals(Optional.of(DecimalType.createDecimalType(12, 2)), map(new ArrowType.Decimal(12, 2, 128)));
        assertEquals(Optional.of(DecimalType.createDecimalType(38, 10)), map(new ArrowType.Decimal(38, 10, 128)));
        assertEquals(Optional.empty(), map(new ArrowType.Decimal(40, 2, 256)));
    }

    @Test
    void mapsTemporalPrecision() {
        assertEquals(Optional.of(TimeType.createTimeType(6)), map(new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
        assertEquals(Optional.of(TimestampType.createTimestampType(0)),
                map(new ArrowType.Timestamp(TimeUnit.SECOND, null)));
        assertEquals(Optional.of(TimestampType.createTimestampType(3)),
                map(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
        assertEquals(Optional.of(TimestampType.createTimestampType(9)),
                map(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));
        assertEquals(Optional.of(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6)),
                map(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")));
    }

    @Test
    void skipsUnsupportedTypes() {
        assertEquals(Optional.empty(), map(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
        assertEquals(Optional.empty(), map(ArrowType.Null.INSTANCE));
        Field list = new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        assertEquals(Optional.empty(), ArrowTypes.toTrino(list));
    }
}
