package pro.surpin.benchbase;

import java.lang.reflect.Method;

public final class HiveExecuteDriverSelfTest {
    private static final Method REWRITE_SQL;

    static {
        try {
            Class<?> rewriteClass = Class.forName(
                    "pro.surpin.benchbase.HiveExecuteDriver$SqlRewrite");
            REWRITE_SQL = rewriteClass.getDeclaredMethod("rewriteSql", String.class);
            REWRITE_SQL.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private HiveExecuteDriverSelfTest() {
    }

    public static void main(String[] args) throws Exception {
        assertRewrite(
                "SELECT 'engine' AS name, 'Spark SQL' AS setting, 'BenchBase JDBC target' AS description",
                "SHOW ALL");
        assertRewrite(
                "SELECT CAST(NULL AS STRING) AS unsupported_metric WHERE FALSE",
                "SELECT * FROM pg_stat_archiver");
        assertRewrite(
                "SELECT CAST(NULL AS STRING) AS unsupported_metric WHERE FALSE",
                "select * from pg_catalog.pg_statio_user_indexes;");
        assertRewrite(
                "SELECT DATE '1998-12-01' - INTERVAL ? day",
                "SELECT DATE '1998-12-01' - concat(?,' day')::interval");
        assertRewrite(
                "SELECT DATE '1998-12-01' - INTERVAL '62' day",
                "SELECT DATE '1998-12-01' - concat('62', ' days')::interval");
        assertRewrite(
                "SELECT o_orderkey FROM orders WHERE EXISTS (SELECT 1 FROM lineitem "
                        + "WHERE l_orderkey = o_orderkey)",
                "SELECT o_orderkey FROM orders WHERE EXISTS (SELECT * FROM lineitem "
                        + "WHERE l_orderkey = o_orderkey)");
    }

    private static void assertRewrite(String expected, String input) throws Exception {
        String actual = (String) REWRITE_SQL.invoke(null, input);
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected [" + expected + "] but got [" + actual + "]");
        }
    }
}
