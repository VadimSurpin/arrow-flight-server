package pro.surpin.data.arrowflight.client.spark.write;

import pro.surpin.data.arrowflight.client.write.WriteStatement;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins the SQL produced when Spark rows are written through the literal-SQL protocol.
 */
class SparkWriteSqlTest {

    private static Field arrow(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    /** Builds a one-column insert and fills it with the given Spark values. */
    private static String insert(DataType sparkType, ArrowType arrowType, Object... values) {
        StructType schema = new StructType(new StructField[] {
            new StructField("c", sparkType, true, Metadata.empty())
        });
        Field field = arrow("c", arrowType);
        WriteStatement stmt = SparkWrites.statement("t", null, schema, new Schema(List.of(field)), "\"", Map.of());
        InternalRow[] rows = new InternalRow[values.length];
        for (int i = 0; i < values.length; i++) {
            rows[i] = new GenericInternalRow(new Object[] {values[i]});
        }
        return SparkWrites.fill(stmt, schema, rows, new Field[] {field});
    }

    @Test
    void integersAndNulls() {
        assertEquals("insert into t(\"c\") values(cast(1 as bigint)),(cast(null as bigint))",
                insert(DataTypes.IntegerType, new ArrowType.Int(64, true), 1, null));
        assertEquals("insert into t(\"c\") values(cast(7 as int))",
                insert(DataTypes.ByteType, new ArrowType.Int(8, true), (byte) 7));
    }

    @Test
    void floatingAndDecimal() {
        assertEquals("insert into t(\"c\") values(cast(1.5 as double))",
                insert(DataTypes.DoubleType, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), 1.5d));
        assertEquals("insert into t(\"c\") values(cast(12.34 as decimal))",
                insert(DataTypes.createDecimalType(10, 2), new ArrowType.Decimal(10, 2, 128),
                        Decimal.apply(new BigDecimal("12.34"))));
    }

    @Test
    void stringsAndBooleans() {
        assertEquals("insert into t(\"c\") values('it''s'),(cast(null as varchar))",
                insert(DataTypes.StringType, ArrowType.Utf8.INSTANCE, UTF8String.fromString("it's"), null));
        assertEquals("insert into t(\"c\") values(cast(true as boolean))",
                insert(DataTypes.BooleanType, ArrowType.Bool.INSTANCE, true));
        assertEquals("insert into t(\"c\") values(cast(true as boolean))",
                insert(DataTypes.StringType, ArrowType.Bool.INSTANCE, UTF8String.fromString("TRUE")));
        assertEquals("insert into t(\"c\") values(cast(false as boolean))",
                insert(DataTypes.LongType, ArrowType.Bool.INSTANCE, 0L));
    }

    @Test
    void dates() {
        int days = (int) LocalDate.of(2024, 2, 3).toEpochDay();
        assertEquals("insert into t(\"c\") values(cast('2024-02-03' as date))",
                insert(DataTypes.DateType, new ArrowType.Date(DateUnit.DAY), days));
        assertEquals("insert into t(\"c\") values(cast('2024-02-03 00:00:00.000000' as timestamp))",
                insert(DataTypes.DateType, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), days));
        assertEquals("insert into t(\"c\") values(cast('2024-02-03 00:00:00' as timestamp))",
                insert(DataTypes.DateType, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), days));
    }

    @Test
    void timestamps() {
        LocalDateTime wall = LocalDateTime.of(2024, 2, 3, 4, 5, 6, 789_000_000);
        long micros = wall.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 789_000L;
        assertEquals("insert into t(\"c\") values(cast('2024-02-03 04:05:06.789' as timestamp))",
                insert(DataTypes.TimestampType, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), micros));
        assertEquals("insert into t(\"c\") values(cast('2024-02-03' as date))",
                insert(DataTypes.TimestampType, new ArrowType.Date(DateUnit.DAY), micros));
        assertEquals("insert into t(\"c\") values(cast('04:05:06' as time))",
                insert(DataTypes.TimestampType, new ArrowType.Time(TimeUnit.SECOND, 32), micros));

        String expectedTz = wall.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneId.of("Asia/Tokyo"))
                .toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        assertEquals("insert into t(\"c\") values(cast('" + expectedTz + "' as timestamp))",
                insert(DataTypes.TimestampType, new ArrowType.Timestamp(TimeUnit.SECOND, "Asia/Tokyo"), micros));
    }

    @Test
    void timeFromString() {
        assertEquals("insert into t(\"c\") values(cast('01:02:03.000' as time))",
                insert(DataTypes.StringType, new ArrowType.Time(TimeUnit.MILLISECOND, 32),
                        UTF8String.fromString("01:02:03")));
    }

    @Test
    void unsupportedCombinationFails() {
        assertThrows(IllegalArgumentException.class,
                () -> insert(DataTypes.BooleanType, new ArrowType.Int(32, true), true));
    }

    @Test
    void mergeStatement() {
        StructType schema = new StructType(new StructField[] {
            new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("v", DataTypes.StringType, true, Metadata.empty())
        });
        Field id = arrow("id", new ArrowType.Int(32, true));
        Field v = arrow("v", ArrowType.Utf8.INSTANCE);
        WriteStatement stmt = SparkWrites.statement("t", new String[] {"id"}, schema,
                new Schema(List.of(id, v)), "\"", Map.of());
        assertEquals("merge into t t using (values(?,?)) s(\"id\",\"v\") on t.\"id\" = s.\"id\" "
                + "when matched then update set \"v\" = s.\"v\" when not matched then insert (\"id\",\"v\") "
                + "values(s.\"id\",s.\"v\")", stmt.getStatement());
        String sql = SparkWrites.fill(stmt, schema,
                new InternalRow[] {new GenericInternalRow(new Object[] {1, UTF8String.fromString("a")})},
                new Field[] {id, v});
        assertTrue(sql.startsWith("merge into t t using (values(cast(1 as int),'a')) s("), sql);
    }

    @Test
    void insertPlaceholders() {
        StructType schema = new StructType(new StructField[] {
            new StructField("a", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("b", DataTypes.IntegerType, true, Metadata.empty())
        });
        WriteStatement stmt = SparkWrites.statement("t", null, schema, new Schema(List.of(
                arrow("a", new ArrowType.Int(32, true)), arrow("b", new ArrowType.Int(32, true)))), "\"", Map.of());
        assertEquals("insert into t(\"a\",\"b\") values(?,?)", stmt.getStatement());
    }
}
