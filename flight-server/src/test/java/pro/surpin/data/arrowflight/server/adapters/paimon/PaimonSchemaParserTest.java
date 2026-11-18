package pro.surpin.data.arrowflight.server.adapters.paimon;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies Paimon schema JSON conversion to Arrow. */
class PaimonSchemaParserTest {

    @TempDir
    Path tempDir;

    /** Verifies primitive Paimon types map to the expected Arrow types. */
    @Test
    void parsesPrimitiveTypes() throws Exception {
        Schema schema = PaimonSchemaParser.parse("""
                {"version":3,"id":1,"fields":[
                  {"id":0,"name":"id","type":"BIGINT"},
                  {"id":1,"name":"flag","type":"BOOLEAN"},
                  {"id":2,"name":"small","type":"SMALLINT"},
                  {"id":3,"name":"price","type":"DOUBLE"},
                  {"id":4,"name":"label","type":"STRING"},
                  {"id":5,"name":"blob","type":"BYTES"},
                  {"id":6,"name":"day","type":"DATE"}
                ],"highestFieldId":6,"partitionKeys":[],"primaryKeys":[],"options":{}}
                """);

        assertEquals(7, schema.getFields().size());
        assertEquals(new ArrowType.Int(64, true), schema.getFields().get(0).getType());
        assertEquals(ArrowType.Bool.INSTANCE, schema.getFields().get(1).getType());
        assertEquals(new ArrowType.Int(16, true), schema.getFields().get(2).getType());
        assertEquals(ArrowType.FloatingPoint.class,
                schema.getFields().get(3).getType().getClass());
        assertEquals(ArrowType.Utf8.INSTANCE, schema.getFields().get(4).getType());
        assertEquals(ArrowType.Binary.INSTANCE, schema.getFields().get(5).getType());
        assertEquals(ArrowType.Date.class, schema.getFields().get(6).getType().getClass());
    }

    /** Verifies decimal, timestamp, and varchar types are recognised. */
    @Test
    void parsesParameterizedTypes() throws Exception {
        Schema schema = PaimonSchemaParser.parse("""
                {"version":3,"id":0,"fields":[
                  {"id":0,"name":"amount","type":"DECIMAL(12, 2)"},
                  {"id":1,"name":"created","type":"TIMESTAMP(3)"},
                  {"id":2,"name":"event","type":"TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)"},
                  {"id":3,"name":"code","type":"VARCHAR(10)"}
                ]}
                """);

        ArrowType decimal = schema.getFields().get(0).getType();
        assertEquals(new ArrowType.Decimal(12, 2, 128), decimal);
        assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
                schema.getFields().get(1).getType());
        assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
                schema.getFields().get(2).getType());
        assertEquals(ArrowType.Utf8.INSTANCE, schema.getFields().get(3).getType());
    }

    /** Verifies nullable defaults to true and honours an explicit false. */
    @Test
    void readsNullability() throws Exception {
        Schema schema = PaimonSchemaParser.parse("""
                {"fields":[
                  {"id":0,"name":"id","type":"BIGINT","nullable":false},
                  {"id":1,"name":"label","type":"STRING"}
                ]}
                """);

        Field id = schema.getFields().get(0);
        Field label = schema.getFields().get(1);
        assertFalse(id.isNullable());
        assertTrue(label.isNullable());
    }

    /** Verifies nested types are rejected with a readable message. */
    @Test
    void rejectsUnsupportedType() {
        IOException error = assertThrows(IOException.class,
                () -> PaimonSchemaParser.parse("""
                        {"fields":[{"id":0,"name":"tags","type":"ARRAY<STRING>"}]}
                        """));
        assertTrue(error.getMessage().contains("Unsupported Paimon type"));
    }

    /** Verifies a schema without fields is rejected. */
    @Test
    void rejectsSchemaWithoutFields() {
        assertThrows(IOException.class, () -> PaimonSchemaParser.parse("{\"version\":3}"));
    }

    /** Verifies the newest schema file is chosen by identifier. */
    @Test
    void findsLatestSchemaFile() throws Exception {
        Path schemaDir = Files.createDirectories(tempDir.resolve("schema"));
        Files.writeString(schemaDir.resolve("schema-0"), "{\"fields\":[]}");
        Files.writeString(schemaDir.resolve("schema-7"), "{\"fields\":[]}");
        Files.writeString(schemaDir.resolve("schema-2"), "{\"fields\":[]}");

        assertEquals(7L, PaimonSchemaParser.latestSchemaId(tempDir));
    }

    /** Verifies a missing schema directory fails with a readable message. */
    @Test
    void failsWithoutSchemaDirectory() {
        assertThrows(IOException.class, () -> PaimonSchemaParser.latestSchemaId(tempDir));
    }
}
