package pro.surpin.benchbase;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

public final class HiveExecuteDriver implements Driver {
    private static final String URL_PREFIX = "jdbc:hiveexec:";
    private static final int QUERY_TIMEOUT_SECONDS = queryTimeoutSeconds();
    private final Driver delegate = new org.apache.hive.jdbc.HiveDriver();

    static {
        try {
            DriverManager.registerDriver(new HiveExecuteDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Connection connection = delegate.connect(toHiveUrl(url), info);
        return wrapConnection(connection);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(toHiveUrl(url), info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    private static String toHiveUrl(String url) {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            return url;
        }
        return "jdbc:" + url.substring(URL_PREFIX.length());
    }

    private static Connection wrapConnection(Connection connection) {
        InvocationHandler handler = new ConnectionHandler(connection);
        return (Connection) Proxy.newProxyInstance(
                HiveExecuteDriver.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                handler);
    }

    private static Statement wrapStatement(Statement statement) {
        InvocationHandler handler = new StatementHandler(statement);
        return (Statement) Proxy.newProxyInstance(
                HiveExecuteDriver.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                handler);
    }

    private static final class ConnectionHandler implements InvocationHandler {
        private final Connection delegate;

        private ConnectionHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();

            switch (name) {
                case "rollback":
                case "commit":
                case "setAutoCommit":
                case "setTransactionIsolation":
                    return null;
                case "getAutoCommit":
                    return Boolean.TRUE;
                case "getTransactionIsolation":
                    return Connection.TRANSACTION_NONE;
                case "unwrap":
                    return unwrap(args);
                case "isWrapperFor":
                    return isWrapperFor(args);
                case "createStatement":
                    Statement statement = (Statement) invokeDelegate(method, args);
                    configureStatement(statement);
                    return wrapStatement(statement);
                default:
                    break;
            }

            Object result = invokeDelegate(method, SqlRewrite.rewriteSqlArgumentIfNeeded(name, args));
            if (result instanceof Statement) {
                configureStatement((Statement) result);
            }
            return result;
        }

        private Object invokeDelegate(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        private Object unwrap(Object[] args) throws SQLException {
            Class<?> target = (Class<?>) args[0];
            if (target.isInstance(delegate)) {
                return delegate;
            }
            return delegate.unwrap(target);
        }

        private Object isWrapperFor(Object[] args) throws SQLException {
            Class<?> target = (Class<?>) args[0];
            return target.isInstance(delegate) || delegate.isWrapperFor(target);
        }
    }

    private static void configureStatement(Statement statement) throws SQLException {
        if (QUERY_TIMEOUT_SECONDS > 0) {
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
        }
    }

    private static int queryTimeoutSeconds() {
        String configured = System.getenv("BENCHBASE_QUERY_TIMEOUT_SECONDS");
        if (configured == null || configured.trim().isEmpty()) {
            return 0;
        }
        try {
            int timeout = Integer.parseInt(configured.trim());
            if (timeout < 0) {
                throw new IllegalArgumentException(
                        "BENCHBASE_QUERY_TIMEOUT_SECONDS must be >= 0: " + configured);
            }
            return timeout;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "BENCHBASE_QUERY_TIMEOUT_SECONDS must be an integer: " + configured, e);
        }
    }

    private static final class StatementHandler implements InvocationHandler {
        private final Statement delegate;

        private StatementHandler(Statement delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, SqlRewrite.rewriteSqlArgumentIfNeeded(method.getName(), args));
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    private static final class SqlRewrite {
        private static Object[] rewriteSqlArgumentIfNeeded(String methodName, Object[] args) {
            if (!methodUsesSql(methodName) || args == null || args.length == 0 || !(args[0] instanceof String)) {
                return args;
            }

            Object[] rewritten = args.clone();
            rewritten[0] = rewriteSql((String) args[0]);
            return rewritten;
        }

        private static boolean methodUsesSql(String methodName) {
            return methodName.startsWith("prepare")
                    || methodName.equals("execute")
                    || methodName.equals("executeQuery")
                    || methodName.equals("executeUpdate")
                    || methodName.equals("executeLargeUpdate");
        }

        private static String rewriteSql(String sql) {
            String trimmed = sql.trim();
            String withoutSemicolon = trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1).trim() : trimmed;
            if (withoutSemicolon.equalsIgnoreCase("SHOW ALL")) {
                return "SELECT 'engine' AS name, 'Spark SQL' AS setting, "
                        + "'BenchBase JDBC target' AS description";
            }
            if (withoutSemicolon.matches(
                    "(?i)SELECT\\s+\\*\\s+FROM\\s+(?:pg_catalog\\.)?pg_(?:stat|statio)_[a-z0-9_]+")) {
                return "SELECT CAST(NULL AS STRING) AS unsupported_metric WHERE FALSE";
            }

            String result = sql;
            result = result.replaceAll("(?i)\\?\\s*::\\s*date", "CAST(? AS DATE)");
            result = result.replaceAll("(?i)'(\\d{4}-\\d{2}-\\d{2})'\\s*::\\s*date", "DATE '$1'");
            result = result.replaceAll("(?i)\\?\\s*::\\s*decimal(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?", "CAST(? AS DECIMAL)");
            result = result.replaceAll("(?i)'(\\d+\\.?\\d*)'\\s*::\\s*decimal(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?", "$1");
            result = result.replaceAll(
                    "(?i)concat\\(\\s*(\\?|'-?\\d+')\\s*,\\s*'\\s*(year|month|day|hour|minute|second)s?\\s*'\\s*\\)\\s*::\\s*interval",
                    "INTERVAL $1 $2");
            result = result.replaceAll("(?i)interval\\s+'([^']+)'\\s+(year|month|day|hour|minute|second)s?", "INTERVAL '$1' $2");
            result = result.replaceAll(
                    "(?is)\\bexists\\s*\\(\\s*select\\s+\\*\\s+from\\b",
                    "EXISTS (SELECT 1 FROM");

            String normalizedSql = result.toLowerCase(Locale.ROOT);
            result = result.replaceAll("(?i)\\bcreate\\s+view\\b", "CREATE TEMPORARY VIEW");
            boolean isTpchQ17 = normalizedSql.contains("p_brand")
                && normalizedSql.contains("p_container")
                && normalizedSql.contains("l_quantity")
                && normalizedSql.contains("avg");

            if (isTpchQ17) {
                result = result.replaceAll(
                    "(?i)AVG\\s*\\(\\s*l_quantity\\s*\\)",
                    "AVG(CAST(l_quantity AS DECIMAL(28,2)))");
            }
            return result;
        }
    }
}
