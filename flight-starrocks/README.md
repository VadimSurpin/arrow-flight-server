# StarRocks Flight SQL connector

This module contains a StarRocks-loadable JDBC driver adapter and the Java/C++
components for a future native Flight SQL catalog.

## StarRocks JDBC catalog adapter

`pro.surpin.data.arrowflight.starrocks.jdbc.postgresql.FlightSqlPostgresqlDriver`
is packaged as a shaded JAR with the Apache Arrow Flight SQL JDBC driver. Give
that JAR to StarRocks' built-in JDBC catalog with `driver_url`, and use a
`jdbc:arrow-flight-sql://` `jdbc_uri`. StarRocks loads the driver JAR on the FE
and BE and can discover schemas/tables and scan rows from the Flight SQL
server. The class name includes `postgresql` because StarRocks 4.0 selects its
JDBC metadata resolver by matching the configured driver class name. The
adapter uses that resolver's JDBC `DatabaseMetaData` contract; it does not
pretend the remote Flight server is a PostgreSQL server.

Run the live StarRocks FE/BE plus Flight SQL query test on Linux with Docker:

```sh
./scripts/run-starrocks-flight-e2e.sh
```

The test creates a StarRocks JDBC external catalog, lists the Flight schema and
table, then runs `SELECT COUNT(*)` through the StarRocks backend scanner. This
path uses StarRocks' JDBC scanner, which converts the Flight JDBC result into
StarRocks rows. It does not use the native C++ `FlightScanReader` directly.

## Native catalog in a StarRocks fork

`ScanSql`, `ScanPredicate` (filter pushdown) and `FlightCatalog.planScan(..., ScanPredicate)` are used by the
fork's frontend; `cpp/` holds the pull-style reader vendored into the fork's backend. See
[docs/integrations/starrocks-native.md](../docs/integrations/starrocks-native.md).

## Native connector components

`FlightCatalog` uses Flight SQL metadata calls and returns planned endpoints
and tickets. `FlightScanReader` streams Arrow batches for a backend scanner.
They remain the components for a future native StarRocks catalog and BE scan
operator, which would avoid the JDBC row conversion path. StarRocks does not
currently expose a dynamic catalog plugin API for those components; a native
catalog and scanner must be integrated into a StarRocks source build.

### Java frontend component

`FlightCatalog` uses Flight SQL `GetCatalogs`, `GetDbSchemas`, `GetTables`,
and `GetExecuteSchema`. `planScan` executes a query once on the frontend and
returns the Flight endpoints and opaque tickets for distribution to backend
scanners. Its callers supply Flight authentication as Arrow
`CallOption` values. The server applies the caller's access rules to each
metadata request. `ScanSql` constructs a basic full scan or projected scan
from validated identifiers. This first implementation supports unquoted ASCII
identifiers only, and it deliberately leaves predicate and limit pushdown to
future work.

StarRocks frontend integration must register a catalog type, translate Arrow
types to StarRocks types, add optimizer and scan planning support, and pass a
scan request to the backend. `FlightCatalog` is an implementation component
for that work. The chosen user identity must be propagated through each
metadata and scan request; a shared privileged token would bypass per-user
Ranger rules on this Flight server.

### C++ backend component

`FlightScanReader::ScanTicket` redeems one frontend-planned ticket on its
assigned backend and forwards each Arrow `RecordBatch` to a callback without
materializing the whole result. `Scan` is also available for a single-backend
scan: it executes a Flight SQL query, honors endpoint locations, and reads all
endpoint tickets. The callback is the boundary
where a StarRocks backend scan node must convert Arrow arrays to StarRocks
columns and chunks. Its caller supplies a bearer token for the scan; obtain
that token for the same identity used during frontend authorization. TLS is
selected by the Flight `Location` URI (`grpc+tls://` for a secure server).

The upstream StarRocks changes must add a scan node and a plan wire format for
each Flight endpoint location, opaque ticket and per-user credentials, then call
`FlightScanReader::ScanTicket` from each backend scanner. The wire format must protect
credentials in transit and keep them out of logs. The current C++ component
does not perform StarRocks chunk conversion or register itself with StarRocks.

## Build and test

```sh
./mvnw test -pl flight-starrocks -am
cmake -S flight-starrocks/cpp -B flight-starrocks/cpp/build
cmake --build flight-starrocks/cpp/build
ctest --test-dir flight-starrocks/cpp/build --output-on-failure
```

The C++ build requires a C++17 compiler, CMake, and Arrow C++ with Flight SQL
enabled. The Java module uses this repository's Arrow 19 dependency. The C++
component should be built against the Arrow C++ version used in the target
StarRocks backend.
