package pro.surpin.data.arrowflight.client.write;

import pro.surpin.data.arrowflight.client.model.FieldType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WriteStatementFillTest {

    private static Field arrow(ArrowType type) {
        return new Field("c", org.apache.arrow.vector.types.pojo.FieldType.nullable(type), null);
    }

    private static String fill(FieldType source, ArrowType target, Object... values) {
        Field field = arrow(target);
        WriteStatement stmt = new WriteStatement("t",
                new pro.surpin.data.arrowflight.client.model.Field[] {
                    new pro.surpin.data.arrowflight.client.model.Field("c", source)},
                new Schema(List.of(field)), "\"", Map.of());
        List<RowAccessor> rows = java.util.Arrays.stream(values).map(v -> (RowAccessor) c -> v).toList();
        return stmt.fillStatement(rows, new Field[] {field});
    }

    @Test
    void nullDateIsTypedNull() {
        assertEquals("insert into t(\"c\") values(cast(null as date))",
                fill(new FieldType(FieldType.IDs.DATE), new ArrowType.Date(DateUnit.DAY), (Object) null));
    }

    @Test
    void decimalToBitUsesNumericValue() {
        assertEquals("insert into t(\"c\") values(cast(true as boolean)),(cast(false as boolean))",
                fill(new FieldType.DecimalType(10, 2), ArrowType.Bool.INSTANCE,
                        new BigDecimal("0.50"), BigDecimal.ZERO));
    }

    @Test
    void dateAndTimestampToVarcharUseIsoText() {
        assertEquals("insert into t(\"c\") values('2024-02-03')",
                fill(new FieldType(FieldType.IDs.DATE), ArrowType.Utf8.INSTANCE, LocalDate.of(2024, 2, 3)));
        assertEquals("insert into t(\"c\") values('1970-01-01T00:00:01Z')",
                fill(new FieldType(FieldType.IDs.TIMESTAMP), ArrowType.Utf8.INSTANCE, Instant.ofEpochSecond(1)));
    }

    @Test
    void complexValuesRenderAsJsonWithQuotesEscapedOnce() {
        Map<String, FieldType> children = new LinkedHashMap<>();
        children.put("s", new FieldType(FieldType.IDs.VARCHAR));
        children.put("xs", new FieldType.ListType(new FieldType(FieldType.IDs.INT)));
        children.put("m", new FieldType.MapType(new FieldType(FieldType.IDs.VARCHAR), new FieldType(FieldType.IDs.INT)));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("s", "it's");
        value.put("xs", List.of(1, 2));
        value.put("m", Map.of("k", 3));
        assertEquals("insert into t(\"c\") values('{ \"s\": \"it''s\", \"xs\": [\"1\", \"2\"], "
                        + "\"m\": { \"map\": [{ \"key\": \"k\", \"value\": \"3\" }] } }')",
                fill(new FieldType.StructType(children), ArrowType.Utf8.INSTANCE, value));
    }

    @Test
    void unknownArrowFieldFails() {
        WriteStatement stmt = new WriteStatement("t",
                new pro.surpin.data.arrowflight.client.model.Field[] {
                    new pro.surpin.data.arrowflight.client.model.Field("c", new FieldType(FieldType.IDs.INT))},
                new Schema(List.of(arrow(new ArrowType.Int(32, true)))), "\"", Map.of());
        assertThrows(IllegalArgumentException.class, () -> stmt.fillStatement(List.of(c -> 1), new Field[0]));
    }

    @Test
    void typeMappingOverridesTargetType() {
        Field field = arrow(new ArrowType.Int(64, true));
        WriteStatement stmt = new WriteStatement("t",
                new pro.surpin.data.arrowflight.client.model.Field[] {
                    new pro.surpin.data.arrowflight.client.model.Field("c", new FieldType(FieldType.IDs.LONG))},
                new Schema(List.of(field)), "\"", Map.of("bigint", "hugeint"));
        assertEquals("insert into t(\"c\") values(cast(5 as hugeint))",
                stmt.fillStatement(List.of(c -> 5L), new Field[] {field}));
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        Field field = arrow(new ArrowType.Int(32, true));
        WriteStatement stmt = new WriteStatement("t",
                new pro.surpin.data.arrowflight.client.model.Field[] {
                    new pro.surpin.data.arrowflight.client.model.Field("c", new FieldType(FieldType.IDs.INT))},
                new Schema(List.of(field)), "\"", Map.of());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(stmt);
        }
        WriteStatement copy;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (WriteStatement) in.readObject();
        }
        assertEquals("insert into t(\"c\") values(cast(9 as int))", copy.fillStatement(List.of(c -> 9), new Field[] {field}));
        assertEquals(field, copy.getArrowSchema().getFields().getFirst());
        assertEquals("c", copy.getDataFields()[0].getName());
    }
}
