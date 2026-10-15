package pro.surpin.data.arrowflight.client.model;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RowSetTest {

    private static Schema emptySchema() {
        return new Schema(java.util.List.of());
    }

    @Test
    void addSingleRow() {
        RowSet rs = new RowSet(emptySchema());
        RowSet.Row row = new RowSet.Row();
        row.add(42);
        row.add("hello");

        rs.add(row);

        assertEquals(1, rs.getData().length);
        assertArrayEquals(new Object[] { 42, "hello" }, rs.getData()[0].getData());
    }

    @Test
    void addMultipleRows() {
        RowSet rs = new RowSet(emptySchema());
        RowSet.Row r1 = new RowSet.Row();
        r1.add(1);
        RowSet.Row r2 = new RowSet.Row();
        r2.add(2);

        rs.add(r1);
        rs.add(r2);

        assertEquals(2, rs.getData().length);
    }

    @Test
    void addRowSetSameSchema() {
        Schema schema = emptySchema();
        RowSet rs1 = new RowSet(schema);
        RowSet.Row r1 = new RowSet.Row();
        r1.add(1);
        rs1.add(r1);

        RowSet rs2 = new RowSet(schema);
        RowSet.Row r2 = new RowSet.Row();
        r2.add(2);
        rs2.add(r2);

        rs1.add(rs2);

        assertEquals(2, rs1.getData().length);
        assertEquals(1, rs2.getData().length);
    }

    @Test
    void addRowSetDifferentSchemaThrows() {
        RowSet rs1 = new RowSet(emptySchema());
        RowSet rs2 = new RowSet(new Schema(java.util.List.of()));

        assertThrows(IllegalArgumentException.class, () -> rs1.add(rs2),
                "Adding RowSet with different schema must throw");
    }

    @Test
    void addRowSetSameSchemaInstanceNoThrow() {
        Schema schema = emptySchema();
        RowSet rs1 = new RowSet(schema);
        RowSet rs2 = new RowSet(schema);

        assertDoesNotThrow(() -> rs1.add(rs2));
    }

}
