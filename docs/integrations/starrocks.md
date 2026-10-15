# StarRocks access to this Flight SQL server

The Spark connector in `flight-spark` implements Spark DataSource V2. Its Java
classes cannot be loaded into StarRocks to make an external catalog. StarRocks'
[external catalog types](https://docs.starrocks.io/docs/data_source/catalog/catalog_overview/)
currently do not include Arrow Flight SQL. StarRocks'
[Arrow Flight SQL support](https://docs.starrocks.io/docs/unloading/arrow_flight/)
serves StarRocks query results to clients; it does not read another Flight SQL
server. A [native ADBC external catalog request](https://github.com/StarRocks/starrocks/issues/69868)
is open.

## Native connector proposal

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

The helper JAR in this repository does not register a catalog in StarRocks;
that requires StarRocks source changes and deployment. A prototype could also try the
[Apache Arrow Flight SQL JDBC driver](https://arrow.apache.org/docs/14.0/java/flight_sql_jdbc_driver.html)
with a [StarRocks JDBC catalog](https://docs.starrocks.io/docs/data_source/catalog/jdbc_catalog/).
That route is unverified: StarRocks documents specific JDBC backends and
schema resolvers, so driver loading alone does not establish metadata or scan
compatibility. It would also use row-oriented JDBC conversion. A simpler route
for Parquet already registered in Hive or Iceberg is to use a corresponding
StarRocks external catalog. Another route is to read through Spark and load
results into StarRocks when a direct catalog is not required.
