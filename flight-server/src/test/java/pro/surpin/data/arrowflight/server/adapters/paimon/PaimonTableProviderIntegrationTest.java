package pro.surpin.data.arrowflight.server.adapters.paimon;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TableProvider.EmptyScan;
import pro.surpin.data.arrowflight.server.TableProvider.PaimonSnapshot;
import pro.surpin.data.arrowflight.server.TableProvider.TableIdentifier;
import pro.surpin.data.arrowflight.server.TableProvider.TableScanPlan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies Paimon planning and reads against a real DuckDB paimon extension.
 * The extension is taken from PAIMON_EXTENSION or the paimon.extension system
 * property when set, otherwise installed from the DuckDB community repository.
 * Tests are skipped when the extension cannot be loaded.
 */
@Tag("integration")
class PaimonTableProviderIntegrationTest {

    @TempDir
    Path warehouse;

    /** Verifies a planned snapshot is readable through the generated SQL. */
    @Test
    void readsPlannedSnapshot() throws Exception {
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", props);
                Statement statement = connection.createStatement()) {
            loadPaimon(statement);
            statement.execute("ATTACH '" + warehouse + "' AS paimon_cat (TYPE paimon)");
            statement.execute("CREATE SCHEMA paimon_cat.testdb");
            statement.execute("CREATE TABLE paimon_cat.testdb.orders (id BIGINT, price BIGINT)");
            statement.execute("INSERT INTO paimon_cat.testdb.orders"
                    + " VALUES (1, 100), (2, 200), (3, 300)");

            PaimonTableProvider provider = new PaimonTableProvider(warehouse);
            TableScanPlan plan = provider.planScan(new TableIdentifier("testdb", "orders"));

            assertEquals(2, plan.tableSchema().getFields().size());
            assertEquals("id", plan.tableSchema().getFields().get(0).getName());
            assertTrue(plan.source() instanceof PaimonSnapshot);
            PaimonSnapshot snapshot = (PaimonSnapshot) plan.source();
            assertTrue(snapshot.snapshotId() >= 1L);

            List<String> rows = new ArrayList<>();
            try (ResultSet result = statement.executeQuery("SELECT id, price FROM "
                    + PaimonScanSql.fromClause(snapshot) + " ORDER BY id")) {
                while (result.next()) {
                    rows.add(result.getLong(1) + ":" + result.getLong(2));
                }
            }
            assertEquals(List.of("1:100", "2:200", "3:300"), rows);
        }
    }

    /** Verifies a Paimon table without snapshots plans an empty scan. */
    @Test
    void plansEmptyTable() throws Exception {
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", props);
                Statement statement = connection.createStatement()) {
            loadPaimon(statement);
            statement.execute("ATTACH '" + warehouse + "' AS paimon_cat (TYPE paimon)");
            statement.execute("CREATE SCHEMA paimon_cat.testdb");
            statement.execute("CREATE TABLE paimon_cat.testdb.empty_orders (id BIGINT)");

            PaimonTableProvider provider = new PaimonTableProvider(warehouse);
            TableScanPlan plan = provider.planScan(new TableIdentifier("testdb", "empty_orders"));

            assertEquals(1, plan.tableSchema().getFields().size());
            assertTrue(plan.source() instanceof EmptyScan);
        }
    }

    /**
     * Loads the DuckDB Paimon extension from a configured file or the community repository.
     *
     * @param statement DuckDB statement
     * @throws SQLException if the extension cannot be loaded
     */
    private static void loadPaimon(Statement statement) throws SQLException {
        String configured = System.getProperty("paimon.extension");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("PAIMON_EXTENSION");
        }
        if (configured != null && !configured.isBlank()) {
            Path path = Path.of(configured);
            assumeTrue(Files.isRegularFile(path), "Paimon extension file is missing: " + path);
            statement.execute("LOAD '" + path + "'");
            return;
        }
        try {
            statement.execute("INSTALL paimon FROM community");
            statement.execute("LOAD paimon");
        } catch (SQLException e) {
            assumeTrue(false, "Paimon extension is unavailable: " + e.getMessage());
        }
    }
}
