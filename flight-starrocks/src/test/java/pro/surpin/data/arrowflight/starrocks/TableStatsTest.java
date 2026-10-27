package pro.surpin.data.arrowflight.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class TableStatsTest {
    @Test
    void parsesTheServerAnswerIncludingPendingDistinctCounts() {
        TableStats stats = TableStats.parse("{\"rows\":3000,\"columns\":{"
                + "\"id\":{\"min\":1.0,\"max\":3000.0,\"nulls\":0,\"bytes\":24000,\"ndv\":2990},"
                + "\"name\":{\"min\":null,\"max\":null,\"nulls\":300,\"bytes\":9000,\"ndv\":null}}}");
        assertEquals(3000, stats.rows());
        assertEquals(new TableStats.ColumnStats(1.0, 3000.0, 0, 24000, 2990L), stats.columns().get("id"));
        assertNull(stats.columns().get("name").min());
        assertNull(stats.columns().get("name").ndv());
    }

    @Test
    void parsesHowManyGroupsOfFilesShareNoValue() {
        TableStats stats = TableStats.parse("{\"rows\":10,\"columns\":{"
                + "\"k\":{\"min\":1.0,\"max\":9.0,\"nulls\":0,\"bytes\":80,\"ndv\":9,\"separableGroups\":20}}}");
        assertEquals(20, stats.columns().get("k").separableGroups());
        assertEquals(0, TableStats.parse("{\"rows\":1,\"columns\":{\"k\":{\"nulls\":0,\"bytes\":1}}}")
                .columns().get("k").separableGroups(), "an older server reports none");
    }

    @Test
    void ignoresFieldsAFutureServerAdds() {
        TableStats stats = TableStats.parse("{\"rows\":1,\"columns\":{},\"histogram\":[1,2]}");
        assertEquals(1, stats.rows());
    }

    @Test
    void rejectsGarbage() {
        assertThrows(IllegalArgumentException.class, () -> TableStats.parse("not json"));
    }

    @Test
    void buildsTheRequest() {
        assertEquals("{\"schema\":\"s\",\"table\":\"t\",\"columns\":[\"a\",\"b\"]}",
                TableStats.request("s", "t", List.of("a", "b")));
    }
}
