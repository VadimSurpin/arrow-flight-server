# Native `flight` catalog for StarRocks

A fork of StarRocks `branch-4.0` (`/home/vadim/arrowflight-server/starrocks`, branch `feat/flight-connector`) adds
an external catalog of type `flight`. It reads from this repository's Flight SQL server without the JDBC row
conversion of the [JDBC plugin path](starrocks.md): the frontend plans the scan, every Flight endpoint becomes a
scan range, and a backend redeems the endpoint's ticket and converts Arrow batches straight into StarRocks chunks.

Design: [spec](../superpowers/specs/2026-10-17-starrocks-native-flight-catalog-design.md),
[plan](../superpowers/plans/2026-10-17-starrocks-native-flight-catalog.md). Build notes and the toolchain fixes the
authoring host needed: [starrocks-fork-notes.md](starrocks-fork-notes.md).

## Status

| Part | State |
|---|---|
| FE: catalog, metadata, type mapping, projection and filter pushdown, scan node, Thrift | Implemented. 27 unit tests pass (`mvn test -pl fe-core -am -Dtest='Flight*Test,CatalogFlightServiceTest'`); the existing JDBC tests still pass. |
| FE against a live Flight server | Verified with `scripts/run-starrocks-native-e2e.sh` (FE only): `CREATE EXTERNAL CATALOG`, `SHOW DATABASES/TABLES`, `DESC` with mapped types, credential hiding, clear errors. |
| BE: data source, Arrow-to-chunk conversion, connector registration | **Written but not compiled or run.** The authoring host has no AVX2 and no BE thirdparty, so the BE could not be built. Only `flight_scan_reader.cpp` was compiled and run. Build the BE on a capable host and run `scripts/run-starrocks-native-e2e.sh --with-be` before relying on queries. |

Not implemented: per-user identity (every query uses the catalog's credentials), limit and aggregate pushdown,
list/struct/map types, basic auth, custom TLS trust.

## Using it

```sql
CREATE EXTERNAL CATALOG flight PROPERTIES (
  "type" = "flight",
  "flight.uri" = "grpc://host:8815",          -- grpc://, grpc+tcp:// or grpc+tls://
  "flight.auth.type" = "token",              -- none (default) or token
  "flight.auth.token" = "...",               -- never shown by SHOW CREATE CATALOG
  "flight.cache.ttl.seconds" = "300"         -- table schema cache, default 300
);
SHOW DATABASES FROM flight;
SELECT id, name FROM flight.sales.orders WHERE id > 100;
```

TLS is chosen by the `grpc+tls://` scheme and uses the JVM trust store. The Flight server accepts a bearer token,
Kerberos or no authentication; this catalog supports the first and the last.

## Types

| Arrow | StarRocks |
|---|---|
| Int8/16/32/64 (signed) | TINYINT / SMALLINT / INT / BIGINT |
| Float / Double | FLOAT / DOUBLE |
| Decimal (precision up to 38) | DECIMAL(p, s) |
| Bool | BOOLEAN |
| Utf8 / LargeUtf8 | VARCHAR |
| Binary / LargeBinary | VARBINARY |
| Date (day) | DATE |
| Timestamp without time zone | DATETIME |

Anything else (unsigned integers, time-zoned timestamps, decimals above 38 digits, lists, structs) fails when the
table is described, with an error naming the column. Column names must be unquoted ASCII identifiers
(`[A-Za-z_][A-Za-z0-9_]*`) because that is what the Flight server's SQL accepts; other names are rejected the same way.

## Pushdown

The frontend sends the Flight server `SELECT <needed columns> FROM schema.table WHERE <pushed filter>`. A filter
is pushed only when it can be rendered with the same meaning: comparisons of a column with a non-null constant,
`IN`, `IS [NOT] NULL`, `BETWEEN`, and `AND`/`OR`/`NOT` of those. Everything else stays in StarRocks as a filter
above the scan. A comparison is pushed only between values of the same family (numeric with numeric, text with text, date with
date, timestamp with timestamp, boolean with boolean): StarRocks strips implicit casts before pushdown, so mixed
comparisons such as a text column against a number stay in StarRocks. Pushed filters are applied by the Flight
server only; they are not re-checked in the backend. String literals containing `'`, `\`, newlines and non-ASCII
text were checked to parse and compare as plain literals through the server's SQL pipeline; rows that actually
contain such values are not covered by the test data.

## Operating notes

* **FE JVM flag.** Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED` (and `java.base/sun.nio.ch`).
  The fork's `bin/start_fe.sh` adds both. Without them every Flight catalog statement fails with a misleading
  "Memory was leaked" error.
* Scan planning runs the query once on the frontend, so `EXPLAIN` contacts the Flight server and needs an alive
  backend.
* Endpoint tickets and the token are sent to the backends inside the plan in plain Thrift, and with a `grpc://`
  URI the token also crosses the network unencrypted; use `grpc+tls://` and a trusted network. They are not shown in
  `EXPLAIN`, `SHOW CREATE CATALOG` or the error messages produced by this connector; FE debug logging of plans was
  not audited.
* The backend half (`be/src/connector/flight_*`, `flight_chunk_converter_test.cpp`) is unverified: it has not been
  compiled or run, and an independent review found and fixed two bugs in it by reading alone. Treat query results
  as unproven until it is built and its gtests pass.

## Build and test

```sh
scripts/install-flight-starrocks-jar.sh                 # jar the fork's FE depends on
scripts/build-starrocks-fork.sh --fe                    # FE (about 7 minutes)
scripts/build-starrocks-fork.sh --be                    # BE: needs an AVX2 host or the prebuilt thirdparty
scripts/run-starrocks-native-e2e.sh                     # FE only
scripts/run-starrocks-native-e2e.sh --with-be           # also SELECT and EXPLAIN
```
