package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Checks extrema SQL, schema inference and logical Parquet statistics. */
@Tag("unit")
class MinMaxContractTest {
    /** Checks negated extrema retain their input type and bypass raw footer statistics. */
    @Test
    void preservesNegatedInputAndSchema() {
        var parsed = ParquetQueryParser.parse("SELECT MIN(-(v)) AS maximum FROM s.t");
        var expression = parsed.selectExprs.getFirst();
        assertEquals("v", expression.inputColumn);
        assertEquals(List.of("v"), expression.inputColumns);
        assertFalse(expression.isColumnReference());
        var adapter = mock(ParquetAdapter.class);
        var type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        when(adapter.getTableSchema("s", "t")).thenReturn(new Schema(List.of(
                new Field("v", FieldType.nullable(type), null))));
        var field = new MetadataService(adapter).buildAggregationSchema(parsed).getFields().getFirst();
        assertEquals("maximum", field.getName());
        assertEquals(type, field.getType());
        assertTrue(ParquetQueryParser.parse("SELECT MIN(v) FROM s.t")
                .selectExprs.getFirst().isColumnReference());
    }

    /**
     * Checks expression aggregates never consume statistics of the unmodified column.
     *
     * @throws Exception if statistics lookup fails
     */
    @Test
    void rejectsExpressionFooterStatistics() throws Exception {
        FileSystem fileSystem = mock(FileSystem.class);
        assertTrue(DuckDbAdapter.footerStats(fileSystem, new Path("/unused.parquet"),
                ParquetQueryParser.parse("SELECT MIN(-(v)) FROM s.t")).isEmpty());
        verifyNoInteractions(fileSystem);
    }

    /**
     * Checks inverted minima ignore NaN while retaining all-NaN and empty states.
     *
     * @throws Exception if DuckDB execution fails
     */
    @Test
    void computesNanSafeFloatingMaximum() throws Exception {
        var parsed = ParquetQueryParser.parse("SELECT MIN(-(v)) AS maximum FROM s.t");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.createStatement()) {
            String source = "(VALUES ('NaN'::DOUBLE), (4.0), (20.0), (NULL)) t(v)";
            try (var result = statement.executeQuery(DuckDbAdapter.buildDuckSql(parsed, source))) {
                assertTrue(result.next());
                assertEquals(20.0, -result.getDouble(1));
            }
            for (String type : List.of("FLOAT", "DOUBLE")) {
                try (var result = statement.executeQuery(DuckDbAdapter.buildDuckSql(parsed,
                        "(VALUES ('NaN'::" + type + ")) t(v)"))) {
                    assertTrue(result.next());
                    assertTrue(Double.isNaN(result.getDouble(1)));
                }
                try (var result = statement.executeQuery(DuckDbAdapter.buildDuckSql(parsed,
                        "(VALUES (NULL::" + type + ")) t(v)"))) {
                    assertTrue(result.next());
                    assertNull(result.getObject(1));
                }
            }
        }
    }

    /** Checks INT32, INT64 and binary decimal statistics preserve their exact scale. */
    @Test
    void decodesDecimalFooterStatistics() {
        var metadata = mock(ColumnChunkMetaData.class);
        when(metadata.getPrimitiveType()).thenReturn(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.decimalType(2, 9)).named("v"));
        assertEquals(new BigDecimal("123.45"), DuckDbAdapter.footerValue(metadata, 12345));
        when(metadata.getPrimitiveType()).thenReturn(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.decimalType(2, 18)).named("v"));
        assertEquals(new BigDecimal("90071992547409.93"),
                DuckDbAdapter.footerValue(metadata, 9007199254740993L));
        when(metadata.getPrimitiveType()).thenReturn(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(4, 38)).named("v"));
        BigInteger unscaled = new BigInteger("-99999999999999999999999999999999999999");
        assertEquals(new BigDecimal(unscaled, 4),
                DuckDbAdapter.footerValue(metadata, Binary.fromConstantByteArray(unscaled.toByteArray())));
        assertNull(DuckDbAdapter.footerValue(metadata, null));
        when(metadata.getPrimitiveType()).thenReturn(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("v"));
        assertEquals(42L, DuckDbAdapter.footerValue(metadata, 42L));
    }
}
