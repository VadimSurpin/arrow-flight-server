package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.SchemaConverter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class MetadataUtilsTest {

    static class TestableParquetAdapter extends ParquetAdapter {
        TestableParquetAdapter() throws Exception {
            super(new AppConfig(
                3, 4096, 4, 65536, 2, 2, 2, "", false, "", "", "", "", null, false,
                67108864, 67108864, 30000L,
                "/nonexistent-data-dir", null, 31001, 5701, 120, 3, 500, 3
            ), new RawLocalFileSystem() {{
                initialize(URI.create("file:///"), new Configuration());
            }});
        }
    }

    private MetadataService metadataService;

    private MetadataService metadataService() {
        if (metadataService == null) {
            try {
                metadataService = new MetadataService(new TestableParquetAdapter());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return metadataService;
    }

    // ─── createLikePredicate ───────────────────────────────────────────────

    @Test
    void nullPatternMatchesEverything() {
        Predicate<String> p = MetadataService.createLikePredicate(null);
        assertTrue(p.test("anything"));
        assertTrue(p.test(""));
        assertTrue(p.test("  spaces  "));
    }

    @Test
    void exactPatternMatchesOnlyItself() {
        Predicate<String> p = MetadataService.createLikePredicate("test_schema");
        assertTrue(p.test("test_schema"));
        assertFalse(p.test("test_schema2"));
        assertFalse(p.test("TEST_SCHEMA"));
        assertFalse(p.test(""));
    }

    @Test
    void percentWildcardMatchesAnySuffix() {
        Predicate<String> p = MetadataService.createLikePredicate("test%");
        assertTrue(p.test("test"));
        assertTrue(p.test("test_schema"));
        assertTrue(p.test("testing123"));
        assertFalse(p.test("my_test"));
        assertFalse(p.test(""));
    }

    @Test
    void percentWildcardMatchesAnyPrefix() {
        Predicate<String> p = MetadataService.createLikePredicate("%_table");
        assertTrue(p.test("my_table"));
        assertTrue(p.test("some_other_table"));
        assertFalse(p.test("table"));
    }

    @Test
    void percentOnlyMatchesAll() {
        Predicate<String> p = MetadataService.createLikePredicate("%");
        assertTrue(p.test("any_string"));
        assertTrue(p.test(""));
    }

    @Test
    void underscoreMatchesSingleCharacter() {
        Predicate<String> p = MetadataService.createLikePredicate("t_st");
        assertTrue(p.test("test"));
        assertTrue(p.test("tast"));
        assertFalse(p.test("ts"));
        assertFalse(p.test("teest"));
    }

    @Test
    void mixedWildcards() {
        Predicate<String> p = MetadataService.createLikePredicate("t%_col");
        assertTrue(p.test("tinyint_col"));
        assertTrue(p.test("t_col"));
        assertFalse(p.test("int_col"));
    }

    @Test
    void regexSpecialCharsInPatternAreEscaped() {
        Predicate<String> p = MetadataService.createLikePredicate("a.b");
        assertTrue(p.test("a.b"));
        assertFalse(p.test("axb"));
        assertFalse(p.test("a.bc"));
    }

    // ─── getCatalogsRoot ──────────────────────────────────────────────────

    @Test
    void getCatalogsRootReturnsSingleRow() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getCatalogsRoot(allocator)) {
            assertEquals(1, root.getRowCount());
        }
    }

    // ─── getSchemasRoot ───────────────────────────────────────────────────

    @Test
    void getSchemasRootReturnsOneRowPerSchema() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getSchemasRoot(
                     List.of("schema_a", "schema_b"), allocator)) {
            assertEquals(2, root.getRowCount());
        }
    }

    @Test
    void getSchemasRootHandlesEmptyCollection() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getSchemasRoot(List.of(), allocator)) {
            assertEquals(0, root.getRowCount());
        }
    }

    // ─── getTableTypesRoot ────────────────────────────────────────────────

    @Test
    void getTableTypesRootReturnsSingleRow() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getTableTypesRoot(allocator)) {
            assertEquals(1, root.getRowCount());
        }
    }

    // ─── buildTypeInfo ────────────────────────────────────────────────────

    @Test
    void buildTypeInfoContainsExpectedTypes() {
        var types = SchemaConverter.buildTypeInfo();
        assertFalse(types.isEmpty());

        List<String> typeNames = types.stream()
                .map(SchemaConverter.TypeInfo::typeName)
                .toList();
        assertTrue(typeNames.contains("BIGINT"));
        assertTrue(typeNames.contains("VARCHAR"));
        assertTrue(typeNames.contains("BOOLEAN"));
        assertTrue(typeNames.contains("TIMESTAMP"));
        assertTrue(typeNames.contains("DATE"));
        assertTrue(typeNames.contains("TINYINT"));
        assertTrue(typeNames.contains("SMALLINT"));
        assertTrue(typeNames.contains("INTEGER"));
        assertTrue(typeNames.contains("DOUBLE"));
        assertTrue(typeNames.contains("REAL"));
    }

    @Test
    void buildTypeInfoIsSortedByDataType() {
        var types = SchemaConverter.buildTypeInfo();
        for (int i = 1; i < types.size(); i++) {
            int prev = types.get(i - 1).dataType();
            int curr = types.get(i).dataType();
            assertTrue(prev <= curr,
                    "Types should be sorted by dataType: " + types.get(i - 1).typeName()
                            + "(" + prev + ") > " + types.get(i).typeName() + "(" + curr + ")");
        }
    }

    // ─── getTablesRoot ────────────────────────────────────────────────────

    @Test
    void getTablesRootReturnsRows() {
        Schema dummySchema = new Schema(List.of(), null);
        Map<String, Map<String, Schema>> tables = Map.of(
                "schema1", Map.of(
                        "table_a", dummySchema,
                        "table_b", dummySchema));

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getTablesRoot(
                     tables, allocator, false, null, null)) {
            assertEquals(2, root.getRowCount());
        }
    }

    @Test
    void getTablesRootWithPatterns() {
        Schema dummySchema = new Schema(List.of(), null);
        Map<String, Map<String, Schema>> tables = Map.of(
                "s", Map.of("a", dummySchema, "b", dummySchema));

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getTablesRoot(
                     tables, allocator, false, "s", "a")) {
            assertEquals(1, root.getRowCount());
        }
    }

    @Test
    void getTablesRootEmptyInput() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = metadataService().getTablesRoot(
                     Map.of(), allocator, false, null, null)) {
            assertEquals(0, root.getRowCount());
        }
    }

    // ─── buildAggregationSchema ───────────────────────────────────────────

    @Test
    void buildAggregationSchemaCountStar() throws Exception {
        Schema tableSchema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));

        MetadataService svc = createMetadataService(tableSchema);
        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT count(*) FROM s.t");
        Schema aggSchema = svc.buildAggregationSchema(pq);

        assertEquals(1, aggSchema.getFields().size());
        assertEquals(new ArrowType.Int(64, true), aggSchema.getFields().get(0).getType(),
                "count(*) should produce Int64");
    }

    @Test
    void buildAggregationSchemaSumAndMin() throws Exception {
        Schema tableSchema = new Schema(List.of(
                new Field("amount", FieldType.nullable(new ArrowType.FloatingPoint(
                        FloatingPointPrecision.DOUBLE)), null),
                new Field("price", FieldType.nullable(new ArrowType.Int(32, true)), null)));

        MetadataService svc = createMetadataService(tableSchema);
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount), min(price) FROM s.t");
        Schema aggSchema = svc.buildAggregationSchema(pq);

        assertEquals(2, aggSchema.getFields().size());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                aggSchema.getFields().get(0).getType(), "sum should produce Float64");
        assertEquals(new ArrowType.Int(32, true),
                aggSchema.getFields().get(1).getType(), "min should preserve column type");
    }

    @Test
    void buildAggregationSchemaPreservesDecimalSumScale() throws Exception {
        Schema tableSchema = new Schema(List.of(
                new Field("l_extendedprice", FieldType.nullable(
                        new ArrowType.Decimal(15, 2, 128)), null),
                new Field("l_discount", FieldType.nullable(
                        new ArrowType.Decimal(15, 2, 128)), null)));

        MetadataService svc = createMetadataService(tableSchema);
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(cast(l_extendedprice * (1 - l_discount) "
                        + "as decimal(32,4))) FROM tpch.lineitem");
        Schema aggSchema = svc.buildAggregationSchema(pq);

        assertEquals(new ArrowType.Decimal(38, 4, 128),
                aggSchema.getFields().get(0).getType());
    }

    @Test
    void buildAggregationSchemaGroupsByColumn() throws Exception {
        Schema tableSchema = new Schema(List.of(
                new Field("region", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("sales", FieldType.nullable(new ArrowType.Int(64, true)), null)));

        MetadataService svc = createMetadataService(tableSchema);
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT region, max(sales) FROM s.t GROUP BY region");
        Schema aggSchema = svc.buildAggregationSchema(pq);

        assertEquals(2, aggSchema.getFields().size(), "GROUP BY + MAX should produce 2 fields");
    }

    private static MetadataService createMetadataService(Schema tableSchema) throws Exception {
        ParquetAdapter adapter = new ParquetAdapter(
                new AppConfig(3, 4096, 4, 65536, 2, 2, 2, "", false, "", "", "", "", null, false,
                        67108864, 67108864, 30000L, "/nonexistent-data-dir", null,
                        31001, 5701, 120,
                        3, 500, 3),
                new RawLocalFileSystem() {{
                    initialize(URI.create("file:///"), new Configuration());
                }}) {
            @Override
            public Schema getTableSchema(String schema, String table) {
                return tableSchema;
            }
        };
        return new MetadataService(adapter);
    }
}
