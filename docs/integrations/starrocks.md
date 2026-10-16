# StarRocks access to this Flight SQL server

The Spark connector in `flight-spark` implements Spark DataSource V2. Its Java
classes cannot be loaded into StarRocks to make an external catalog. StarRocks'
[external catalog types](https://docs.starrocks.io/docs/data_source/catalog/catalog_overview/)
currently do not include Arrow Flight SQL. StarRocks'
[Arrow Flight SQL support](https://docs.starrocks.io/docs/unloading/arrow_flight/)
serves StarRocks query results to clients; it does not read another Flight SQL
server. A [native ADBC external catalog request](https://github.com/StarRocks/starrocks/issues/69868)
is open.

## StarRocks JDBC catalog

The `flight-starrocks` module packages an Arrow Flight SQL JDBC driver adapter
that StarRocks can load through its supported JDBC catalog. Create a catalog
with `type=jdbc`, set `driver_url` to the module's shaded
`starrocks-driver` JAR, set `driver_class` to
`pro.surpin.data.arrowflight.starrocks.jdbc.postgresql.FlightSqlPostgresqlDriver`,
and set `jdbc_uri` to the Flight SQL endpoint. The integration test runs a live
StarRocks FE/BE, creates this catalog, discovers a Flight schema and table,
and reads rows through a StarRocks scan.

This JDBC path uses StarRocks' generic JDBC scanner. The Flight JDBC driver
receives Arrow batches from the server and exposes them through JDBC, after
which StarRocks converts the values into its own row/column representation.
It does not call the `FlightScanReader` C++ helper and does not propagate the
individual StarRocks query user's identity; the catalog's configured Flight
credentials are used for its connections.

## Native connector components

Initial Java and C++ connector components now live in
[`flight-starrocks`](../../flight-starrocks/README.md). They implement metadata
access and streaming batch reads; the StarRocks source integration described
below remains necessary.

An inbound Flight SQL catalog needs changes in StarRocks itself. The existing
[JDBC connector](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCConnector.java)
is a useful model for catalog registration and metadata discovery, while the
[connector manager](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/connector/ConnectorMgr.java)
registers supported types. The implementation would need:

1. A new catalog type with connection, TLS, and authentication properties. The
   FE would use Flight SQL metadata APIs to enumerate schemas and tables and
   map Arrow types to StarRocks types.
2. A scan path that plans remote SQL and Flight endpoints in the FE, streams
   Arrow batches from this server in the BE/CN, and converts them to StarRocks
   chunks. Start with full table scans; add projection, filter, and limit
   pushdown after correctness tests.
3. Authentication propagation from each StarRocks scan to this server. Ranger
   rules on this server remain authoritative for each Flight request. Avoid a
   shared privileged identity that would bypass per-user policies.
4. StarRocks integration tests for schema discovery, data types, distributed
   reads, failure/retry behavior, TLS, and denied access.

The Java metadata client and C++ stream reader are not dynamically registered
as a native StarRocks catalog or BE scanner. That requires StarRocks source
changes and deployment. A simpler route
for Parquet already registered in Hive or Iceberg is to use a corresponding
StarRocks external catalog. Another route is to read through Spark and load
results into StarRocks when a direct catalog is not required.
