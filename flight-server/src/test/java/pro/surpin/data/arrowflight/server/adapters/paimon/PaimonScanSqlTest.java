package pro.surpin.data.arrowflight.server.adapters.paimon;

import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.TableProvider.PaimonSnapshot;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies DuckDB SQL generation for pinned Paimon snapshots. */
class PaimonScanSqlTest {

    /** Verifies a plain directory produces a paimon_scan expression. */
    @Test
    void buildsPaimonScan() {
        Path table = Path.of("/data/paimon/testdb.db/orders");

        assertEquals("paimon_scan('/data/paimon/testdb.db/orders', snapshot_from_id = 7)",
                PaimonScanSql.fromClause(table.toUri(), 7L));
    }

    /** Verifies a quote inside the directory is escaped. */
    @Test
    void escapesSingleQuotes() {
        Path table = Path.of("/data/pa'i'mon/testdb.db/orders");

        assertEquals("paimon_scan('/data/pa''i''mon/testdb.db/orders', snapshot_from_id = 1)",
                PaimonScanSql.fromClause(table.toUri(), 1L));
    }

    /** Verifies the snapshot overload reads the planned source. */
    @Test
    void readsSnapshotSource() {
        Path table = Path.of("/data/paimon/testdb.db/orders");
        PaimonSnapshot snapshot = new PaimonSnapshot(table.toUri(), 3L);

        assertEquals(PaimonScanSql.fromClause(table.toUri(), 3L),
                PaimonScanSql.fromClause(snapshot));
    }
}
