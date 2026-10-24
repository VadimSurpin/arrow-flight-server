package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.services.TableStatisticsService.ColumnStatistics;
import pro.surpin.data.arrowflight.server.services.TableStatisticsService.TableStatistics;

class TableStatisticsServiceTest {
    @TempDir
    Path dataDir;

    private Connection duck;
    private ExecutorService background;
    private TableStatisticsService service;

    @BeforeEach
    void setUp() throws Exception {
        duck = DriverManager.getConnection("jdbc:duckdb:");
        Path table = Files.createDirectories(dataDir.resolve("sch").resolve("tbl"));
        try (Statement s = duck.createStatement()) {
            s.execute("COPY (SELECT i::BIGINT AS id, (i * 1.25)::DECIMAL(15,2) AS price, "
                    + "DATE '2020-01-01' + (i % 365)::INTEGER AS d, "
                    + "CASE WHEN i % 10 = 0 THEN NULL ELSE 'name' || (i % 7) END AS name, "
                    + "(i % 3)::INTEGER AS bucket, i::DOUBLE / 4 AS ratio "
                    + "FROM range(1, 1001) t(i)) TO '" + table.resolve("part-0.parquet") + "' (FORMAT PARQUET)");
            s.execute("COPY (SELECT i::BIGINT AS id, (i * 1.25)::DECIMAL(15,2) AS price, "
                    + "DATE '2020-01-01' + (i % 365)::INTEGER AS d, "
                    + "CASE WHEN i % 10 = 0 THEN NULL ELSE 'name' || (i % 7) END AS name, "
                    + "(i % 3)::INTEGER AS bucket, i::DOUBLE / 4 AS ratio "
                    + "FROM range(1001, 3001) t(i)) TO '" + table.resolve("part-1.parquet") + "' (FORMAT PARQUET)");
        }
        background = Executors.newSingleThreadExecutor();
        FileSystem fs = FileSystem.getLocal(new Configuration());
        service = new TableStatisticsService(fs, new org.apache.hadoop.fs.Path(dataDir.toString()),
                sql -> {
                    try (Statement s = duck.createStatement(); java.sql.ResultSet rs = s.executeQuery(sql)) {
                        rs.next();
                        return rs.getLong(1);
                    }
                }, background);
    }

    @AfterEach
    void tearDown() throws Exception {
        background.shutdownNow();
        duck.close();
    }

    @Test
    void footersGiveRowCountBoundsNullsAndSizes() throws Exception {
        TableStatistics stats = service.statistics("sch", "tbl", List.of("id", "price", "d", "name"));
        assertEquals(3000, stats.rows());
        ColumnStatistics id = stats.columns().get("id");
        assertEquals(1.0, id.min());
        assertEquals(3000.0, id.max());
        assertEquals(0, id.nulls());
        assertTrue(id.bytes() > 0);
        ColumnStatistics price = stats.columns().get("price");
        assertEquals(1.25, price.min(), 1e-9);
        assertEquals(3750.0, price.max(), 1e-9);
        ColumnStatistics d = stats.columns().get("d");
        long epochDay2020 = java.time.LocalDate.of(2020, 1, 1).toEpochDay();
        assertEquals(epochDay2020, d.min(), 1e-9);
        assertEquals(epochDay2020 + 364, d.max(), 1e-9);
        ColumnStatistics name = stats.columns().get("name");
        assertNull(name.min(), "string bounds are not reported");
        assertNull(name.max());
        assertEquals(300, name.nulls());
    }

    @Test
    void distinctCountsAppearOnceTheBackgroundEstimateFinished() throws Exception {
        TableStatistics first = service.statistics("sch", "tbl", List.of("id", "bucket"));
        assertNull(first.columns().get("id").ndv(), "the first answer must not wait for the estimate");
        service.awaitIdle();
        TableStatistics second = service.statistics("sch", "tbl", List.of("id", "bucket"));
        assertNotNull(second.columns().get("id").ndv());
        assertEquals(3000, second.columns().get("id").ndv(), 3000 * 0.05);
        assertEquals(3, second.columns().get("bucket").ndv(), 1);
    }

    @Test
    void unknownColumnsAreSkippedAndUnknownTablesRejected() throws Exception {
        TableStatistics stats = service.statistics("sch", "tbl", List.of("id", "missing"));
        assertTrue(stats.columns().containsKey("id"));
        assertTrue(!stats.columns().containsKey("missing"));
        assertThrows(java.io.FileNotFoundException.class, () -> service.statistics("sch", "nope", List.of("id")));
    }

    @Test
    void namesThatCouldEscapeTheDataDirectoryAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> service.statistics("..", "tbl", List.of("id")));
        assertThrows(IllegalArgumentException.class, () -> service.statistics("sch", "tbl", List.of("id\"; DROP")));
    }

    @Test
    void jsonRoundTripKeepsEverything() {
        TableStatistics original = new TableStatistics(42, java.util.Map.of(
                "a", new ColumnStatistics(1.5, 9.5, 3, 100, 7L),
                "b", new ColumnStatistics(null, null, -1, 0, null)));
        TableStatistics copy = TableStatisticsService.fromJson(TableStatisticsService.toJson(original));
        assertEquals(original, copy);
    }

    @Test
    void secondRequestIsServedFromTheFooterCache() throws Exception {
        TableStatistics a = service.statistics("sch", "tbl", List.of("id"));
        TableStatistics b = service.statistics("sch", "tbl", List.of("id"));
        assertEquals(a.rows(), b.rows());
        assertEquals(1, service.footerScans(), "footers must be read once per table version");
    }
}
