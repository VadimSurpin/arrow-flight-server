import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/** Checks Parquet and Iceberg HDFS reads through the server's DuckDB adapter. */
public final class IcebergHdfsProbe {

    /** Creates no instances of the probe. */
    private IcebergHdfsProbe() {
    }

    /**
     * Verifies the control file and all Spark-generated snapshots.
     *
     * @param args path to the fixture description
     * @throws Exception if fixture loading or any read fails
     */
    public static void main(String[] args) throws Exception {
        List<String> fixture = Files.readAllLines(Path.of(args[0]));
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT version()")) {
            result.next();
            System.out.printf("DuckDB %s, JDBC %s%n", result.getString(1),
                    connection.getMetaData().getDriverVersion());
        }

        var config = ConfigAdapter.getConfig();
        System.out.println("Checking HDFS Parquet with " + config.duckDbHdfsExtension());
        try (var pool = Executors.newSingleThreadExecutor();
                var adapter = new DuckDbAdapter(config, pool);
                var statement = adapter.connection().createStatement()) {
            checkRows(statement, "plain Parquet", DuckDbAdapter.readParquetFromClause(
                    List.of(fixture.get(0))), List.of(1L, 2L));

            System.out.println("Loading DuckDB Iceberg extension");
            statement.execute("INSTALL iceberg");
            statement.execute("LOAD iceberg");
            try (var result = statement.executeQuery(
                    "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'")) {
                result.next();
                System.out.println("Iceberg extension " + result.getString(1));
            }

            checkSnapshot(statement, fixture.get(1), List.of(1L, 2L));
            checkSnapshot(statement, fixture.get(2), List.of(1L, 2L, 3L));
            checkSnapshot(statement, fixture.get(3), List.of(1L, 3L));
        }
    }

    /**
     * Reads a pinned snapshot using its metadata file and ID.
     *
     * @param statement DuckDB statement
     * @param snapshot fixture row containing a label, metadata URI, and snapshot ID
     * @param expected expected visible row IDs
     * @throws SQLException if the scan fails or returns unexpected rows
     */
    private static void checkSnapshot(Statement statement, String snapshot, List<Long> expected)
            throws SQLException {
        String[] fields = snapshot.split("\t");
        String source = "iceberg_scan(" + DuckDbAdapter.sqlStringLiteral(fields[1])
                + ", snapshot_from_id = " + Long.parseLong(fields[2]) + ")";
        checkRows(statement, fields[0], source, expected);
    }

    /**
     * Checks the selected rows and count from a DuckDB source.
     *
     * @param statement DuckDB statement
     * @param label name of the tested state
     * @param source DuckDB FROM expression
     * @param expected expected visible row IDs
     * @throws SQLException if the scan fails or returns unexpected rows
     */
    private static void checkRows(Statement statement, String label, String source, List<Long> expected)
            throws SQLException {
        String select = "SELECT id FROM " + source + " ORDER BY id";
        System.out.println(label + ": " + select);
        List<Long> ids = new ArrayList<>();
        try (var rows = statement.executeQuery(select)) {
            while (rows.next()) {
                ids.add(rows.getLong(1));
            }
        }
        if (!ids.equals(expected)) {
            throw new SQLException(label + ": expected " + expected + ", got " + ids);
        }

        String count = "SELECT COUNT(*) FROM " + source;
        System.out.println(label + ": " + count);
        try (var rows = statement.executeQuery(count)) {
            rows.next();
            long actual = rows.getLong(1);
            if (actual != expected.size()) {
                throw new SQLException(label + ": expected count " + expected.size() + ", got " + actual);
            }
        }
        System.out.printf("PASS %s: rows=%s count=%d%n", label, ids, ids.size());
    }
}
