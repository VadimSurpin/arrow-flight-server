package pro.surpin.data.arrowflight.starrocks.jdbc.postgresql;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Arrow Flight SQL JDBC driver entry point for StarRocks JDBC catalogs.
 *
 * <p>StarRocks selects its JDBC metadata resolver from this class name. Its BE
 * JDBC scanner also requires non-null Java class names from result metadata;
 * Arrow Flight JDBC leaves those unset, so this adapter derives them from the
 * standard JDBC type code.</p>
 */
public final class FlightSqlPostgresqlDriver implements Driver {
    private static final ArrowFlightJdbcDriver DELEGATE = new ArrowFlightJdbcDriver();

    static {
        try {
            DriverManager.registerDriver(new FlightSqlPostgresqlDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Connection connection = DELEGATE.connect(url, properties);
        return wrap(connection, Connection.class);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:arrow-flight-sql://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        return DELEGATE.getPropertyInfo(url, properties);
    }

    @Override
    public int getMajorVersion() {
        return DELEGATE.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return DELEGATE.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return DELEGATE.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return DELEGATE.getParentLogger();
    }

    private static <T> T wrap(Object target, Class<T> api) {
        InvocationHandler handler = (proxy, method, args) -> invoke(target, method, args);
        return api.cast(Proxy.newProxyInstance(
                FlightSqlPostgresqlDriver.class.getClassLoader(), new Class<?>[] {api}, handler));
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            Object result = method.invoke(target, args);
            if (target instanceof Connection && result instanceof PreparedStatement statement) {
                return wrap(statement, PreparedStatement.class);
            }
            if (target instanceof Statement && result instanceof ResultSet resultSet) {
                return wrap(resultSet, ResultSet.class);
            }
            if (target instanceof ResultSet && result instanceof ResultSetMetaData metadata) {
                return wrap(metadata, ResultSetMetaData.class);
            }
            if (target instanceof ResultSetMetaData && method.getName().equals("getColumnClassName")) {
                return classNameFor(((ResultSetMetaData) target).getColumnType((Integer) args[0]));
            }
            return result;
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static String classNameFor(int jdbcType) {
        return switch (jdbcType) {
            case Types.BOOLEAN, Types.BIT -> Boolean.class.getName();
            case Types.TINYINT -> Byte.class.getName();
            case Types.SMALLINT -> Short.class.getName();
            case Types.INTEGER -> Integer.class.getName();
            case Types.BIGINT -> Long.class.getName();
            case Types.REAL, Types.FLOAT -> Float.class.getName();
            case Types.DOUBLE -> Double.class.getName();
            case Types.NUMERIC, Types.DECIMAL -> java.math.BigDecimal.class.getName();
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR,
                    Types.LONGNVARCHAR -> String.class.getName();
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class.getName();
            case Types.DATE -> java.sql.Date.class.getName();
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> java.sql.Time.class.getName();
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> java.sql.Timestamp.class.getName();
            default -> Object.class.getName();
        };
    }
}
