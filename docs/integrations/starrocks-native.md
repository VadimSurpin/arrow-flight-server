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
| BE: data source, Arrow-to-chunk conversion, connector registration | Built and exercised on GitHub Actions (`.github/workflows/starrocks-native-e2e.yml`): a real FE and BE read the test table through the native catalog, including string columns and pushed filters. It cannot be built on a host without AVX2, which is why CI is the verification path. |

Not implemented: per-user identity (every query uses the catalog's credentials), limit pushdown,
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

## Aggregation pushdown

`COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX` and `AVG` with `GROUP BY` plain columns are pushed into the scan by
`PushDownAggToFlightScanRule` (property `flight.aggregation_pushdown`, default `true`; set it to `false` to keep
every aggregate in StarRocks). A table plans to one Flight endpoint per server, and the server can only return a
partial aggregate per endpoint, so StarRocks keeps a merge aggregate above the scan: COUNT merges as SUM, SUM as
SUM, MIN as MIN, MAX as MAX, and AVG is pushed as a sum and a count and divided after the merge. The query sent
is, for example, `SELECT k, COUNT(*) AS __agg0, SUM(d) AS __agg1 FROM s.t WHERE ... GROUP BY k`; the `__aggN`
aliases are the scan's column names, which is how the backend matches Arrow columns to slots.

What is deliberately not pushed, because the server's results would not be exactly StarRocks' (checked against a
live server by `AggregateContractTest`):

* integral `SUM`: StarRocks returns an exact BIGINT, while a server-side sum is a DOUBLE (inexact above 2^53). The
  server also streams a plain `SUM` of an integer column as DECIMAL(38,0) although it announces DOUBLE when
  planning, which the backend's strict converter rejects; pushed sums are therefore always written
  `SUM(CAST(x AS DOUBLE))` (the server rejects a CAST around the aggregate but accepts one inside). `SUM` of
  FLOAT/DOUBLE and `AVG` of any numeric column are pushed;
* `MIN`/`MAX` of strings (collation may differ), dates and timestamps (the test data has no such columns, so the
  server's types were not verified), `DISTINCT`, expressions, DECIMAL, `HAVING`-style expressions on keys;
* any aggregate over a filter that cannot be rendered for the server, or over a scan with a `LIMIT`.

The pushed filter is carried on the aggregated scan, since its columns are no longer part of the scan's output.
Floating-point sums may differ in the last digits from StarRocks' own because the order of addition differs.
The multi-endpoint merge is covered by the rule's unit tests only: the standalone test server always plans a single
endpoint.

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
