package pro.surpin.data.arrowflight.client.query;

import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.model.FieldType;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableScanTest {

    private static String norm(String sql) {
        return sql.trim().replaceAll(" +", " ");
    }

    @Test
    void wrapsSelectAsSubQuery() {
        assertEquals("(SELECT a FROM t) t", TableScan.forTable("SELECT a FROM t", "\"").getSource());
        assertEquals("t", TableScan.forTable("t", "\"").getSource());
    }

    @Test
    void defaultsToSelectStar() {
        assertEquals("select * from t", norm(TableScan.forTable("t", "\"").getQueryStatement()));
    }

    @Test
    void probeWithoutPushdownKeepsQuery() {
        TableScan scan = TableScan.forTable("t", "\"");
        assertFalse(scan.probe(null, List.of(), null, null, new Field[0]));
        assertFalse(scan.probe("", List.of(), null, null, new Field[0]));
    }

    @Test
    void probeAppliesProjectionAndFilter() {
        TableScan scan = TableScan.forTable("t", "\"");
        assertTrue(scan.probe("\"a\" > 1", List.of("a", "b"), null, null, new Field[0]));
        assertEquals("select \"a\",\"b\" from t where \"a\" > 1", norm(scan.getQueryStatement()));
        assertFalse(scan.probe("\"a\" > 1", List.of("a", "b"), null, null, new Field[0]));
    }

    @Test
    void probeAppliesAggregation() {
        TableScan scan = TableScan.forTable("t", "\"");
        PushAggregation agg = new PushAggregation(new String[] {"\"g\"", "count(*)"}, new String[] {"\"g\""});
        scan.probe(null, List.of(), agg, null, new Field[0]);
        assertEquals("select \"g\",count(*) from t group by \"g\"", norm(scan.getQueryStatement()));
    }

    @Test
    void buildsRangePartitionsFromFieldType() {
        TableScan scan = TableScan.forTable("t", "\"");
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 2, "0", "10", null);
        Field[] fields = {new Field("id", new FieldType(FieldType.IDs.LONG))};
        scan.probe("x = 1", List.of("id"), null, pb, fields);
        assertArrayEquals(new String[] {
            "select \"id\" from t where (x = 1) and (0 <= id and id < 5)",
            "select \"id\" from t where (x = 1) and (5 <= id and id < 10)"
        }, java.util.Arrays.stream(scan.getPartitionStatements()).map(TableScanTest::norm).toArray());
    }

    @Test
    void globalAggregationIsNotPartitioned() {
        TableScan scan = TableScan.forTable("t", "\"");
        PartitionBehavior pb = new PartitionBehavior("hash", "id", 2, null, null, null);
        scan.probe(null, List.of(), new PushAggregation(new String[] {"count(*)"}), pb, new Field[0]);
        assertEquals(0, scan.getPartitionStatements().length);
    }

    @Test
    void newScanResetsState() {
        TableScan scan = TableScan.forTable("t", "`");
        scan.probe("a = 1", List.of("a"), null, null, new Field[0]);
        TableScan fresh = scan.newScan();
        assertEquals("select * from t", norm(fresh.getQueryStatement()));
        assertEquals("`", fresh.getColumnQuote());
        assertEquals(0, fresh.getEndpoints().length);
        assertNull(fresh.getSchema());
        assertEquals(-1L, fresh.getEstimatedBytes());
        assertEquals(-1L, fresh.getEstimatedRows());
    }

    @Test
    void mergeFieldsLaterListWins() {
        Field first = new Field("a", new FieldType(FieldType.IDs.INT));
        Field second = new Field("a", new FieldType(FieldType.IDs.LONG));
        Field other = new Field("b", new FieldType(FieldType.IDs.VARCHAR));
        Field[] merged = TableScan.mergeFields(List.of(first, other), null, List.of(second));
        assertEquals(2, merged.length);
        assertEquals(FieldType.IDs.LONG, merged[0].getType().getTypeID());
        assertEquals("b", merged[1].getName());
    }
}
