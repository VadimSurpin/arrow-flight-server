package pro.surpin.data.arrowflight.server.adapters.paimon;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.paimon.PaimonQueryPlanner.PlannedPaimonQuery;
import pro.surpin.data.arrowflight.server.services.MetadataService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Verifies Paimon SQL planning into DuckDB native scans. */
class PaimonQueryPlannerTest {

    private static final String TABLE_SCHEMA = """
            {"version":3,"id":0,"fields":[
              {"id":0,"name":"id","type":"BIGINT"},
              {"id":1,"name":"price","type":"BIGINT"}
            ]}
            """;

    @TempDir
    Path warehouse;

    private Path tableDir;
    private PaimonQueryPlanner planner;

    /** Creates a warehouse with one table and one snapshot. */
    @BeforeEach
    void setUp() throws IOException {
        Path schemaDir = Files.createDirectories(
                warehouse.resolve("testdb.db").resolve("orders").resolve("schema"));
        tableDir = schemaDir.getParent();
        Files.writeString(schemaDir.resolve("schema-0"), TABLE_SCHEMA);
        Path snapshotDir = Files.createDirectories(tableDir.resolve("snapshot"));
        Files.writeString(snapshotDir.resolve("LATEST"), "4");
        Files.writeString(snapshotDir.resolve("snapshot-4"),
                "{\"version\":3,\"id\":4,\"schemaId\":0}");
        planner = new PaimonQueryPlanner(new PaimonTableProvider(warehouse),
                new MetadataService(mock(ParquetAdapter.class)));
    }

    /** Verifies projection and filter survive the FROM rewrite. */
    @Test
    void rewritesFromAndKeepsFilter() throws Exception {
        PlannedPaimonQuery planned = planner.plan(
                "SELECT id, price FROM testdb.orders WHERE price >= 200 ORDER BY id");

        assertEquals("SELECT id, price FROM paimon_scan('" + tableDir
                        + "', snapshot_from_id = 4) WHERE price >= 200 ORDER BY id",
                planned.duckSql());
        assertEquals(2, planned.schema().getFields().size());
        assertEquals("id", planned.schema().getFields().get(0).getName());
    }

    /** Verifies aggregation keeps its select list and returns a count schema. */
    @Test
    void keepsAggregation() throws Exception {
        PlannedPaimonQuery planned = planner.plan(
                "SELECT count(*) AS c FROM testdb.orders");

        assertTrue(planned.duckSql().contains("SELECT count(*) AS c FROM paimon_scan("));
        assertEquals(1, planned.schema().getFields().size());
        assertEquals(new ArrowType.Int(64, true),
                planned.schema().getFields().get(0).getType());
    }

    /** Verifies a table without snapshots loses the snapshot argument. */
    @Test
    void plansEmptyTableWithoutSnapshot() throws Exception {
        Path schemaDir = Files.createDirectories(
                warehouse.resolve("testdb.db").resolve("empty").resolve("schema"));
        Files.writeString(schemaDir.resolve("schema-0"), TABLE_SCHEMA);

        PlannedPaimonQuery planned = planner.plan("SELECT * FROM testdb.empty");

        assertEquals("SELECT * FROM paimon_scan('" + schemaDir.getParent() + "')",
                planned.duckSql());
        assertEquals(2, planned.schema().getFields().size());
    }

    /** Verifies the schema follows the requested projection order. */
    @Test
    void projectsRequestedColumns() throws Exception {
        PlannedPaimonQuery planned = planner.plan("SELECT price, id FROM testdb.orders");

        Schema schema = planned.schema();
        assertEquals(2, schema.getFields().size());
        assertEquals("price", schema.getFields().get(0).getName());
        assertEquals("id", schema.getFields().get(1).getName());
        assertFalse(planned.duckSql().contains("snapshot_from_id = -1"));
    }

    /** Verifies joins are rejected instead of silently mis-planned. */
    @Test
    void rejectsJoins() {
        IOException error = assertThrows(IOException.class, () -> planner.plan(
                "SELECT o.id FROM testdb.orders o JOIN testdb.items i ON o.id = i.id"));
        assertTrue(error.getMessage().contains("Joins are not supported"));
    }

    /** Verifies a missing table is reported as not found. */
    @Test
    void rejectsMissingTable() {
        assertThrows(NoSuchFileException.class,
                () -> planner.plan("SELECT * FROM testdb.missing"));
    }

    /** Verifies unqualified tables are rejected. */
    @Test
    void rejectsUnqualifiedTable() {
        IOException error = assertThrows(IOException.class,
                () -> planner.plan("SELECT * FROM orders"));
        assertTrue(error.getMessage().contains("schema.table"));
    }

    /** Verifies the disabled planner rejects every query. */
    @Test
    void disabledPlannerRejectsQueries() {
        PaimonQueryPlanner disabled = PaimonQueryPlanner.disabled();

        assertFalse(disabled.enabled());
        assertThrows(IOException.class, () -> disabled.plan("SELECT * FROM testdb.orders"));
    }
}
