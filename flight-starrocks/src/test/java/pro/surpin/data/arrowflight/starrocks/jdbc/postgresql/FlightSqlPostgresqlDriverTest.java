package pro.surpin.data.arrowflight.starrocks.jdbc.postgresql;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies the StarRocks JDBC metadata compatibility wrapper on each JDBC access path. */
@Tag("unit")
class FlightSqlPostgresqlDriverTest {
    /** Plain statements must expose the column class fallback on their result sets. */
    @Test
    void statementResultMetadataHasColumnClass() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet rows = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(rows);
        when(rows.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);

        Connection wrapped = FlightSqlPostgresqlDriver.wrap(connection, Connection.class);
        assertEquals(Integer.class.getName(), wrapped.createStatement()
                .executeQuery("SELECT 1").getMetaData().getColumnClassName(1));
    }

    /** Prepared-statement metadata must be wrapped even before the query executes. */
    @Test
    void preparedStatementMetadataHasColumnClass() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnType(1)).thenReturn(Types.DECIMAL);

        Connection wrapped = FlightSqlPostgresqlDriver.wrap(connection, Connection.class);
        assertEquals(java.math.BigDecimal.class.getName(), wrapped.prepareStatement("SELECT 1")
                .getMetaData().getColumnClassName(1));
    }

    /** Database metadata result sets must also expose the column class fallback. */
    @Test
    void databaseMetadataResultHasColumnClass() throws Exception {
        Connection connection = mock(Connection.class);
        DatabaseMetaData database = mock(DatabaseMetaData.class);
        ResultSet columns = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.getMetaData()).thenReturn(database);
        when(database.getColumns(isNull(), isNull(), isNull(), isNull())).thenReturn(columns);
        when(columns.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnType(1)).thenReturn(Types.VARCHAR);

        Connection wrapped = FlightSqlPostgresqlDriver.wrap(connection, Connection.class);
        assertEquals(String.class.getName(), wrapped.getMetaData()
                .getColumns(null, null, null, null).getMetaData().getColumnClassName(1));
    }

    /** Callable statements must retain their JDBC interface after wrapping. */
    @Test
    void callableStatementRetainsItsInterface() throws Exception {
        Connection connection = mock(Connection.class);
        CallableStatement statement = mock(CallableStatement.class);
        when(connection.prepareCall(anyString())).thenReturn(statement);

        Connection wrapped = FlightSqlPostgresqlDriver.wrap(connection, Connection.class);
        assertNotNull(wrapped.prepareCall("CALL p()"));
    }
}
