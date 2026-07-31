package pro.surpin.data.arrowflight.client.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FieldTest {

    @Test
    void equalsSameName() {
        Field a = new Field("col", new FieldType(FieldType.IDs.INT));
        Field b = new Field("COL", new FieldType(FieldType.IDs.LONG));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void equalsDifferentName() {
        Field a = new Field("col_a", new FieldType(FieldType.IDs.INT));
        Field b = new Field("col_b", new FieldType(FieldType.IDs.INT));

        assertNotEquals(a, b);
    }

    @Test
    void equalsDifferentType() {
        Field a = new Field("col", new FieldType(FieldType.IDs.INT));
        assertNotEquals(a, "not-a-field");
    }

    @Test
    void findFieldByName() {
        Field[] fields = {
                new Field("id", new FieldType(FieldType.IDs.INT)),
                new Field("name", new FieldType(FieldType.IDs.VARCHAR)),
        };

        FieldType result = Field.find(fields, "name");
        assertEquals(FieldType.IDs.VARCHAR, result.getTypeID());
    }

    @Test
    void findCaseInsensitive() {
        Field[] fields = { new Field("Id", new FieldType(FieldType.IDs.INT)) };

        FieldType result = Field.find(fields, "id");
        assertEquals(FieldType.IDs.INT, result.getTypeID());
    }

    @Test
    void findThrowsIfNotFound() {
        Field[] fields = { new Field("a", new FieldType(FieldType.IDs.INT)) };

        assertThrows(IllegalArgumentException.class, () -> Field.find(fields, "missing"));
    }

    @Test
    void findThrowsIfEmptyArray() {
        assertThrows(IllegalArgumentException.class, () -> Field.find(new Field[0], "any"));
    }
}
