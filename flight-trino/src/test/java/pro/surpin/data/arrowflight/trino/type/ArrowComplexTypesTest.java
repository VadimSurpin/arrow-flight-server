package pro.surpin.data.arrowflight.trino.type;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks complex type mapping and actual nested Arrow values. */
@Tag("unit")
class ArrowComplexTypesTest {
    private static final Field ELEMENT = field("item", new ArrowType.Int(32, true));

    /**
     * Creates a nullable field.
     * @param name field name
     * @param type Arrow type
     * @param children child fields
     * @return field
     */
    private static Field field(String name, ArrowType type, Field... children) {
        return new Field(name, FieldType.nullable(type), List.of(children));
    }

    /** Verifies all supported list layouts with null elements and null arrays. */
    @Test
    void readsAllListLayouts() {
        for (ArrowType layout : List.of(ArrowType.List.INSTANCE, ArrowType.LargeList.INSTANCE,
                new ArrowType.FixedSizeList(2))) {
            Field field = field("items", layout, ELEMENT);
            Type type = ArrowTypes.toTrino(field).orElseThrow();
            try (RootAllocator allocator = new RootAllocator(); FieldVector vector = field.createVector(allocator)) {
                vector.allocateNew();
                FieldVector elements = vector.getChildrenFromFields().getFirst();
                switch (vector) {
                    case ListVector list -> {
                        list.startNewValue(0);
                        list.endValue(0, 2);
                        list.setNull(1);
                        list.startNewValue(2);
                        list.endValue(2, 0);
                    }
                    case LargeListVector list -> {
                        list.startNewValue(0);
                        list.endValue(0, 2);
                        list.setNull(1);
                        list.startNewValue(2);
                        list.endValue(2, 0);
                    }
                    case FixedSizeListVector list -> {
                        list.setNotNull(0);
                        list.setNull(1);
                        list.setNotNull(2);
                    }
                    default -> throw new AssertionError(layout);
                }
                ((IntVector) elements).setSafe(0, 7);
                elements.setNull(1);
                vector.setValueCount(3);
                Block block = ArrowBlocks.toBlock(type, vector, 3);
                assertEquals(Arrays.asList(7, null), type.getObjectValue(null, block, 0));
                assertTrue(block.isNull(1));
                assertEquals(layout instanceof ArrowType.FixedSizeList ? Arrays.asList(null, null) : List.of(),
                        type.getObjectValue(null, block, 2));
            }
        }
    }

    /** Verifies nested arrays in rows and distinguishes null rows from rows of nulls. */
    @Test
    void readsNestedRows() {
        Field field = field("record", ArrowType.Struct.INSTANCE,
                field("values", ArrowType.List.INSTANCE, ELEMENT), field("label", ArrowType.Utf8.INSTANCE));
        Type type = ArrowTypes.toTrino(field).orElseThrow();
        try (RootAllocator allocator = new RootAllocator(); StructVector vector = (StructVector) field.createVector(allocator)) {
            vector.allocateNew();
            ListVector values = (ListVector) vector.getChild("values");
            values.startNewValue(0);
            ((IntVector) values.getDataVector()).setSafe(0, 42);
            values.endValue(0, 1);
            ((VarCharVector) vector.getChild("label")).setSafe(0, "тест".getBytes(StandardCharsets.UTF_8));
            vector.setIndexDefined(0);
            vector.setNull(1);
            vector.setIndexDefined(2);
            values.setNull(2);
            vector.setValueCount(3);
            Block block = ArrowBlocks.toBlock(type, vector, 3);
            assertEquals(List.of(List.of(42), "тест"), type.getObjectValue(null, block, 0));
            assertTrue(block.isNull(1));
            assertEquals(Arrays.asList(null, null), type.getObjectValue(null, block, 2));
        }
    }

    /** Verifies maps with null values, empty maps, null maps and invalid keys. */
    @Test
    void readsMapsAndRejectsInvalidKeys() {
        Field entries = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Int(32, true)), null),
                field(MapVector.VALUE_NAME, ArrowType.Utf8.INSTANCE)));
        Field field = field("mapping", new ArrowType.Map(false), entries);
        Type type = ArrowTypes.toTrino(field).orElseThrow();
        try (RootAllocator allocator = new RootAllocator(); MapVector vector = (MapVector) field.createVector(allocator)) {
            vector.allocateNew();
            StructVector data = (StructVector) vector.getDataVector();
            IntVector keys = (IntVector) data.getChild(MapVector.KEY_NAME);
            vector.startNewValue(0);
            keys.setSafe(0, 1);
            keys.setSafe(1, 2);
            ((VarCharVector) data.getChild(MapVector.VALUE_NAME)).setSafe(0, new byte[] {'a'});
            data.setIndexDefined(0);
            data.setIndexDefined(1);
            vector.endValue(0, 2);
            vector.startNewValue(1);
            vector.endValue(1, 0);
            vector.setNull(2);
            vector.setValueCount(3);
            Block block = ArrowBlocks.toBlock(type, vector, 3);
            Map<?, ?> result = (Map<?, ?>) type.getObjectValue(null, block, 0);
            assertEquals("a", result.get(1));
            assertTrue(result.containsKey(2));
            assertEquals(null, result.get(2));
            assertEquals(Map.of(), type.getObjectValue(null, block, 1));
            assertTrue(block.isNull(2));
            keys.setSafe(1, 1);
            assertThrows(RuntimeException.class, () -> ArrowBlocks.toBlock(type, vector, 3));
            keys.setNull(1);
            assertThrows(IllegalArgumentException.class, () -> ArrowBlocks.toBlock(type, vector, 3));
        }
    }

    /** Ensures unsupported nested members reject the whole column. */
    @Test
    void doesNotDropUnsupportedNestedFields() {
        Field unsupported = field("unsupported", ArrowType.Null.INSTANCE);
        assertTrue(ArrowTypes.toTrino(field("a", ArrowType.List.INSTANCE, unsupported)).isEmpty());
        assertTrue(ArrowTypes.toTrino(field("r", ArrowType.Struct.INSTANCE, ELEMENT, unsupported)).isEmpty());
        assertTrue(ArrowTypes.toTrino(field("m", new ArrowType.Map(false))).isEmpty());
        assertTrue(ArrowTypes.toTrino(field("r", ArrowType.Struct.INSTANCE)).isEmpty());
    }
}
