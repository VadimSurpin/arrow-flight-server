package pro.surpin.data.arrowflight.server.adapters.paimon;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TableProvider.EmptyScan;
import pro.surpin.data.arrowflight.server.TableProvider.PaimonSnapshot;
import pro.surpin.data.arrowflight.server.TableProvider.TableIdentifier;
import pro.surpin.data.arrowflight.server.TableProvider.TableScanPlan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies planning of Paimon filesystem tables. */
class PaimonTableProviderTest {

    @TempDir
    Path warehouse;

    private Path tableDir;
    private PaimonTableProvider provider;

    /** Creates a table layout with one schema and no snapshots. */
    @BeforeEach
    void setUp() throws IOException {
        provider = new PaimonTableProvider(warehouse);
        tableDir = Files.createDirectories(warehouse.resolve("testdb.db").resolve("orders"));
        Path schemaDir = Files.createDirectories(tableDir.resolve("schema"));
        Files.writeString(schemaDir.resolve("schema-0"), """
                {"version":3,"id":0,"fields":[{"id":0,"name":"id","type":"BIGINT"}]}
                """);
    }

    /** Verifies the latest snapshot is pinned together with its schema. */
    @Test
    void plansLatestSnapshot() throws Exception {
        writeSnapshot(2L, 0L);

        TableScanPlan plan = provider.planScan(new TableIdentifier("testdb", "orders"));

        assertEquals(1, plan.tableSchema().getFields().size());
        assertEquals("id", plan.tableSchema().getFields().get(0).getName());
        PaimonSnapshot snapshot = (PaimonSnapshot) plan.source();
        assertEquals(2L, snapshot.snapshotId());
        assertEquals(tableDir.toAbsolutePath().toUri(), snapshot.tablePath());
    }

    /** Verifies the schema referenced by the snapshot wins over the newest schema. */
    @Test
    void usesSnapshotSchemaVersion() throws Exception {
        Files.writeString(tableDir.resolve("schema").resolve("schema-1"), """
                {"version":3,"id":1,"fields":[
                  {"id":0,"name":"id","type":"BIGINT"},
                  {"id":1,"name":"price","type":"BIGINT"}
                ]}
                """);
        writeSnapshot(1L, 0L);

        TableScanPlan plan = provider.planScan(new TableIdentifier("testdb", "orders"));

        assertEquals(1, plan.tableSchema().getFields().size());
    }

    /** Verifies a table without snapshots plans an empty scan. */
    @Test
    void plansEmptyTable() throws Exception {
        TableScanPlan plan = provider.planScan(new TableIdentifier("testdb", "orders"));

        assertEquals(1, plan.tableSchema().getFields().size());
        assertTrue(plan.source() instanceof EmptyScan);
    }

    /** Verifies a missing table is reported as not found. */
    @Test
    void failsOnMissingTable() {
        assertThrows(NoSuchFileException.class,
                () -> provider.planScan(new TableIdentifier("testdb", "missing")));
    }

    /** Verifies a broken snapshot marker is reported instead of ignored. */
    @Test
    void failsOnMalformedLatestMarker() throws Exception {
        Path snapshotDir = Files.createDirectories(tableDir.resolve("snapshot"));
        Files.writeString(snapshotDir.resolve("LATEST"), "not-a-number");

        assertThrows(IOException.class,
                () -> provider.planScan(new TableIdentifier("testdb", "orders")));
    }

    /** Verifies a marker pointing to an absent snapshot file is reported. */
    @Test
    void failsOnMissingSnapshotFile() throws Exception {
        Path snapshotDir = Files.createDirectories(tableDir.resolve("snapshot"));
        Files.writeString(snapshotDir.resolve("LATEST"), "5");

        assertThrows(IOException.class,
                () -> provider.planScan(new TableIdentifier("testdb", "orders")));
    }

    /** Verifies a table without schema files is reported. */
    @Test
    void failsWithoutSchemaFiles() throws Exception {
        Files.delete(tableDir.resolve("schema").resolve("schema-0"));

        assertThrows(IOException.class,
                () -> provider.planScan(new TableIdentifier("testdb", "orders")));
    }

    private void writeSnapshot(long snapshotId, long schemaId) throws IOException {
        Path snapshotDir = Files.createDirectories(tableDir.resolve("snapshot"));
        Files.writeString(snapshotDir.resolve("LATEST"), Long.toString(snapshotId));
        Files.writeString(snapshotDir.resolve("snapshot-" + snapshotId), """
                {"version":3,"id":%d,"schemaId":%d,"totalRecordCount":3}
                """.formatted(snapshotId, schemaId));
    }
}
