package pro.surpin.data.arrowflight.starrocks.jdbc.postgresql;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

/**
 * Arrow Flight SQL JDBC driver entry point for StarRocks JDBC catalogs.
 *
 * <p>StarRocks 4.0 selects its JDBC metadata resolver from the configured driver
 * class name. Including {@code postgresql} selects its JDBC metadata contract,
 * which Arrow Flight SQL's JDBC driver implements through DatabaseMetaData.
 * This class is packaged in the driver JAR that StarRocks downloads for the FE
 * and BE.</p>
 */
public final class FlightSqlPostgresqlDriver extends ArrowFlightJdbcDriver {
}
