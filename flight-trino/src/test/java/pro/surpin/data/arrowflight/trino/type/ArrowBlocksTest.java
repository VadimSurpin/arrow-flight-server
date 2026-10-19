package pro.surpin.data.arrowflight.trino.type;

import io.trino.spi.block.Block;
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
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ArrowBlocksTest {
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void convertsBooleansAndNulls() {
        try (BitVector vector = new BitVector("b", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 1);
            vector.setNull(1);
            vector.set(2, 0);
            vector.setValueCount(3);
            Block block = ArrowBlocks.toBlock(BooleanType.BOOLEAN, vector, 3);
            assertTrue(BooleanType.BOOLEAN.getBoolean(block, 0));
            assertTrue(block.isNull(1));
            assertFalse(BooleanType.BOOLEAN.getBoolean(block, 2));
        }
    }

    @Test
    void readsNarrowerIntegerStreamsAsPlannedType() {
        try (TinyIntVector vector = new TinyIntVector("i", allocator)) {
            vector.allocateNew(2);
            vector.set(0, (byte) -7);
            vector.set(1, (byte) 100);
            vector.setValueCount(2);
            Block block = ArrowBlocks.toBlock(BigintType.BIGINT, vector, 2);
            assertEquals(-7L, BigintType.BIGINT.getLong(block, 0));
            assertEquals(100L, BigintType.BIGINT.getLong(block, 1));
        }
        try (IntVector vector = new IntVector("i", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 42);
            vector.setValueCount(1);
            assertEquals(42L, IntegerType.INTEGER.getLong(ArrowBlocks.toBlock(IntegerType.INTEGER, vector, 1), 0));
            assertEquals(42.0, DoubleType.DOUBLE.getDouble(ArrowBlocks.toBlock(DoubleType.DOUBLE, vector, 1), 0));
        }
    }

    @Test
    void convertsFloatingPoint() {
        try (Float4Vector floats = new Float4Vector("f", allocator);
             Float8Vector doubles = new Float8Vector("d", allocator)) {
            floats.allocateNew(1);
            floats.set(0, 1.5f);
            floats.setValueCount(1);
            doubles.allocateNew(1);
            doubles.set(0, 2.25);
            doubles.setValueCount(1);
            Block real = ArrowBlocks.toBlock(RealType.REAL, floats, 1);
            assertEquals(1.5f, Float.intBitsToFloat((int) RealType.REAL.getLong(real, 0)));
            Block realFromDouble = ArrowBlocks.toBlock(RealType.REAL, doubles, 1);
            assertEquals(2.25f, Float.intBitsToFloat((int) RealType.REAL.getLong(realFromDouble, 0)));
            assertEquals(1.5, DoubleType.DOUBLE.getDouble(ArrowBlocks.toBlock(DoubleType.DOUBLE, floats, 1), 0));
        }
    }

    @Test
    void convertsShortAndLongDecimals() {
        DecimalType shortType = DecimalType.createDecimalType(10, 2);
        DecimalType longType = DecimalType.createDecimalType(30, 5);
        try (DecimalVector small = new DecimalVector("s", allocator, 10, 2);
             DecimalVector large = new DecimalVector("l", allocator, 30, 5)) {
            small.allocateNew(1);
            small.set(0, new BigDecimal("123.45"));
            small.setValueCount(1);
            large.allocateNew(1);
            large.set(0, new BigDecimal("1234567890123456789012.34567"));
            large.setValueCount(1);
            assertEquals(12345L, shortType.getLong(ArrowBlocks.toBlock(shortType, small, 1), 0));
            Int128 value = (Int128) longType.getObject(ArrowBlocks.toBlock(longType, large, 1), 0);
            assertEquals(new BigInteger("123456789012345678901234567"), value.toBigInteger());
        }
    }

    @Test
    void convertsUnsignedBigintToDecimal() {
        DecimalType type = DecimalType.createDecimalType(20, 0);
        try (UInt8Vector vector = new UInt8Vector("u", allocator)) {
            vector.allocateNew(1);
            vector.set(0, -1L);
            vector.setValueCount(1);
            Int128 value = (Int128) type.getObject(ArrowBlocks.toBlock(type, vector, 1), 0);
            assertEquals(new BigInteger("18446744073709551615"), value.toBigInteger());
        }
    }

    @Test
    void convertsStringsAndBinary() {
        try (VarCharVector text = new VarCharVector("t", allocator);
             LargeVarCharVector large = new LargeVarCharVector("lt", allocator);
             VarBinaryVector binary = new VarBinaryVector("b", allocator)) {
            text.allocateNew(1);
            text.setSafe(0, "привет".getBytes(StandardCharsets.UTF_8));
            text.setValueCount(1);
            large.allocateNew(1);
            large.setSafe(0, "big".getBytes(StandardCharsets.UTF_8));
            large.setValueCount(1);
            binary.allocateNew(1);
            binary.setSafe(0, new byte[] {1, 2, 3});
            binary.setValueCount(1);
            assertEquals("привет", VarcharType.VARCHAR.getSlice(
                    ArrowBlocks.toBlock(VarcharType.VARCHAR, text, 1), 0).toStringUtf8());
            assertEquals("big", VarcharType.VARCHAR.getSlice(
                    ArrowBlocks.toBlock(VarcharType.VARCHAR, large, 1), 0).toStringUtf8());
            assertArrayEquals(new byte[] {1, 2, 3}, VarbinaryType.VARBINARY.getSlice(
                    ArrowBlocks.toBlock(VarbinaryType.VARBINARY, binary, 1), 0).getBytes());
        }
    }

    @Test
    void rendersOtherVectorsAsText() {
        try (BigIntVector vector = new BigIntVector("n", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 99L);
            vector.setValueCount(1);
            assertEquals("99", VarcharType.VARCHAR.getSlice(
                    ArrowBlocks.toBlock(VarcharType.VARCHAR, vector, 1), 0).toStringUtf8());
        }
    }

    @Test
    void convertsDates() {
        try (DateDayVector days = new DateDayVector("d", allocator);
             DateMilliVector millis = new DateMilliVector("m", allocator)) {
            days.allocateNew(1);
            days.set(0, 19_000);
            days.setValueCount(1);
            millis.allocateNew(1);
            millis.set(0, 19_000L * 86_400_000L + 5_000L);
            millis.setValueCount(1);
            assertEquals(19_000L, DateType.DATE.getLong(ArrowBlocks.toBlock(DateType.DATE, days, 1), 0));
            assertEquals(19_000L, DateType.DATE.getLong(ArrowBlocks.toBlock(DateType.DATE, millis, 1), 0));
        }
    }

    @Test
    void convertsTimesToPicosOfDay() {
        TimeType type = TimeType.createTimeType(9);
        try (TimeSecVector sec = new TimeSecVector("s", allocator);
             TimeMilliVector milli = new TimeMilliVector("ms", allocator);
             TimeMicroVector micro = new TimeMicroVector("us", allocator);
             TimeNanoVector nano = new TimeNanoVector("ns", allocator)) {
            sec.allocateNew(1);
            sec.set(0, 2);
            sec.setValueCount(1);
            milli.allocateNew(1);
            milli.set(0, 2);
            milli.setValueCount(1);
            micro.allocateNew(1);
            micro.set(0, 2L);
            micro.setValueCount(1);
            nano.allocateNew(1);
            nano.set(0, 2L);
            nano.setValueCount(1);
            assertEquals(2_000_000_000_000L, type.getLong(ArrowBlocks.toBlock(type, sec, 1), 0));
            assertEquals(2_000_000_000L, type.getLong(ArrowBlocks.toBlock(type, milli, 1), 0));
            assertEquals(2_000_000L, type.getLong(ArrowBlocks.toBlock(type, micro, 1), 0));
            assertEquals(2_000L, type.getLong(ArrowBlocks.toBlock(type, nano, 1), 0));
        }
    }

    @Test
    void convertsTimestamps() {
        TimestampType millisType = TimestampType.createTimestampType(3);
        TimestampType nanosType = TimestampType.createTimestampType(9);
        try (TimeStampMilliVector millis = new TimeStampMilliVector("m", allocator);
             TimeStampSecVector seconds = new TimeStampSecVector("s", allocator);
             TimeStampNanoVector nanos = new TimeStampNanoVector("n", allocator)) {
            millis.allocateNew(1);
            millis.set(0, 1_500L);
            millis.setValueCount(1);
            seconds.allocateNew(1);
            seconds.set(0, 3L);
            seconds.setValueCount(1);
            nanos.allocateNew(1);
            nanos.set(0, -1L);
            nanos.setValueCount(1);
            assertEquals(1_500_000L, millisType.getLong(ArrowBlocks.toBlock(millisType, millis, 1), 0));
            assertEquals(3_000_000L, millisType.getLong(ArrowBlocks.toBlock(millisType, seconds, 1), 0));
            LongTimestamp value = (LongTimestamp) nanosType.getObject(ArrowBlocks.toBlock(nanosType, nanos, 1), 0);
            assertEquals(-1L, value.getEpochMicros());
            assertEquals(999_000, value.getPicosOfMicro());
        }
    }

    @Test
    void convertsTimestampsWithTimeZone() {
        TimestampWithTimeZoneType shortType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);
        TimestampWithTimeZoneType longType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6);
        try (TimeStampMilliTZVector millis = new TimeStampMilliTZVector("m", allocator, "UTC");
             TimeStampMicroTZVector micros = new TimeStampMicroTZVector("u", allocator, "UTC")) {
            millis.allocateNew(1);
            millis.set(0, 1_234L);
            millis.setValueCount(1);
            micros.allocateNew(1);
            micros.set(0, 1_234_567L);
            micros.setValueCount(1);
            assertEquals(1_234L, unpackMillisUtc(shortType.getLong(ArrowBlocks.toBlock(shortType, millis, 1), 0)));
            LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) longType.getObject(
                    ArrowBlocks.toBlock(longType, micros, 1), 0);
            assertEquals(1_234L, value.getEpochMillis());
            assertEquals(567_000_000, value.getPicosOfMilli());
        }
    }

    @Test
    void epochMicrosRoundsNanosDown() {
        assertEquals(-1L, ArrowBlocks.epochMicros(-1L, TimeUnit.NANOSECOND));
        assertEquals(2_000_000L, ArrowBlocks.epochMicros(2L, TimeUnit.SECOND));
    }

    @Test
    void rejectsMismatchedVectors() {
        try (VarCharVector text = new VarCharVector("t", allocator)) {
            text.allocateNew(1);
            text.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
            text.setValueCount(1);
            assertThrows(IllegalArgumentException.class, () -> ArrowBlocks.toBlock(BigintType.BIGINT, text, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> ArrowBlocks.toBlock(TimeType.createTimeType(6), text, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> ArrowBlocks.toBlock(DecimalType.createDecimalType(5, 0), text, 1));
            FieldVector vector = text;
            assertThrows(IllegalArgumentException.class,
                    () -> ArrowBlocks.toBlock(io.trino.spi.type.UuidType.UUID, vector, 1));
        }
    }
}
