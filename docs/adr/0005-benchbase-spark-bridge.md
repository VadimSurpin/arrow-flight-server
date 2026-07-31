# ADR 0005: BenchBase–Spark Bridge via HiveExecuteDriver

Status: Accepted
Date: 2026-07-14

## Decision Record

- Timestamp: 2026-07-14T17:00:00+07:00
- User: s.samokhin
- Question: BenchBase uses JDBC (Postgres dialect) to execute benchmark queries.
  Spark SQL does not expose a standard JDBC endpoint. How should BenchBase talk
  to Spark without rewriting the benchmark harness?
- Decision: Insert a thin JDBC shim (`HiveExecuteDriver`) that translates
  BenchBase's JDBC calls into Hive JDBC calls against Spark Thrift Server
  (HiveServer2). The shim rewrites Postgres-specific SQL syntax into
  Hive-compatible form and stubs out JDBC operations that Hive does not support.
- Rationale: Minimal code (< 250 lines), zero changes to BenchBase itself, and
  full reuse of Spark Thrift Server which is already part of the Spark ecosystem.
  BenchBase sees a standard JDBC datasource; Spark sees a standard Hive JDBC
  client.

## Context

BenchBase expects a JDBC connection to run TPC-H and TPC-DS workloads. It
generates SQL in Postgres syntax: `'1994-01-01'::date`, `?::decimal`,
`concat(?::varchar, ' days')::interval`. It also calls `Connection.rollback()`,
`Connection.setAutoCommit(false)`, and `Statement.setQueryTimeout()`.

Spark SQL does not have a native JDBC driver. It speaks the HiveServer2
protocol via the Hive JDBC driver (`jdbc:hive2://`). The Hive JDBC driver:

- does not support `?::date` or `::` cast syntax;
- does not support `rollback()` or `setTransactionIsolation()`;
- does not understand `SHOW ALL` (used by BenchBase as a warm-up query);
- uses `INTERVAL '1' year` not `INTERVAL '1 years'`.

The project runs a Spark Thrift Server container (`spark-thrift-server`) that
exposes HiveServer2 on port 10000. BenchBase runs in a separate container
(`benchbase-spark`). The two must communicate.

## Architecture

```
┌──────────────┐    JDBC (Postgres dialect)     ┌──────────────────┐
│  BenchBase   │ ──────────────────────────────▶ │ HiveExecuteDriver│
│  (Java)      │    url=jdbc:hiveexec:hive2://   │  (shim, ~230 loc)│
└──────────────┘    spark-thrift-server:10000/db  └────────┬─────────┘
                                                            │
                                               Hive JDBC (jdbc:hive2://)
                                                            │
                                                            ▼
┌───────────────────────────────────────────────────────────────┐
│  Spark Thrift Server (HiveServer2)         port 10000         │
│  Spark SQL engine                                              │
│    ├─ FlightSource catalog  (tpch_flight) — via Flight SQL     │
│    └─ Direct catalog        (tpch_direct)  — via Parquet      │
└───────────────────────────────────────────────────────────────┘
```

### Components

**1. `HiveExecuteDriver`** (`shim/net/surpin/benchbase/HiveExecuteDriver.java`)

A `java.sql.Driver` implementation registered via `DriverManager`. It:

- Intercepts URLs with prefix `jdbc:hiveexec:`.
- Strips the prefix and delegates to `org.apache.hive.jdbc.HiveDriver`:
  `jdbc:hiveexec:hive2://host:port/db` → `jdbc:hive2://host:port/db`.
- Wraps the returned `Connection` and `Statement` objects in JDK dynamic
  proxies (`InvocationHandler`) to intercept calls.

**2. Connection proxy** (`ConnectionHandler`)

Stubs out operations that HiveServer2 does not support:

| BenchBase call           | Proxy behaviour             |
|--------------------------|-----------------------------|
| `rollback()`             | no-op (returns null)        |
| `commit()`               | no-op                       |
| `setAutoCommit(false)`   | no-op                       |
| `setTransactionIsolation(...)` | no-op                 |
| `getAutoCommit()`        | returns `true`              |
| `getTransactionIsolation()` | returns `TRANSACTION_NONE` |
| `createStatement()`      | wraps in `StatementHandler`  |
| all other                | delegate to Hive connection  |

**3. Statement proxy** (`StatementHandler`)

Rewrites SQL before sending it to Hive (see `SqlRewrite` below). Also
configures `queryTimeout` from env `BENCHBASE_QUERY_TIMEOUT_SECONDS`
(default 120 s).

**4. `SqlRewrite`** (static methods in `HiveExecuteDriver.SqlRewrite`)

Transforms Postgres syntax to Hive-compatible syntax:

| Postgres (BenchBase)                | Hive (Spark SQL)                   |
|-------------------------------------|------------------------------------|
| `?::date`                           | `CAST(? AS DATE)`                  |
| `'1994-01-01'::date`                | `DATE '1994-01-01'`                |
| `?::decimal` / `?::decimal(10,2)`   | `CAST(? AS DECIMAL)` / `CAST(? AS DECIMAL(10,2))` |
| `'0.05'::decimal`                   | `0.05`                             |
| `concat(?::varchar, ' days')::interval` | `INTERVAL ? day`              |
| `INTERVAL '1' year`                 | `INTERVAL '1' year`                |
| `SHOW ALL`                          | BenchBase-compatible engine row    |

### Integration

The shim is compiled and placed on the classpath inside the BenchBase
container (`benchbase-entrypoint.sh` scans `/benchbase/classes`). BenchBase's
XML config specifies:

```xml
<url>jdbc:hiveexec:hive2://spark-thrift-server:10000/tpch_flight</url>
<driver>pro.surpin.benchbase.HiveExecuteDriver</driver>
```

Spark Thrift Server is configured with:

```
spark.sql.catalog.spark_catalog=pro.surpin.data.arrowflight.client.spark.FlightSessionCatalog
spark.sql.hive.metastore.sharedPrefixes=pro.surpin.data.arrowflight,flight,org.apache.arrow,io.grpc,io.netty,com.google.protobuf
```

The `sharedPrefixes` setting tells the Hive metastore to load the listed
classes from the application classloader (not the Hive classloader), avoiding
`ClassNotFoundException` for Arrow / Flight / gRPC classes during query
planning.

## Alternatives Considered

### Option A: Custom BenchBase engine adapter

Write a `BenchmarkDriver` implementation that talks directly to the Flight SQL
server via Arrow Flight client (gRPC), skipping Spark entirely.

- **Pros**: Full control, no Spark dependency, no SQL rewrite needed.
- **Cons**: Requires deep integration with BenchBase's internal API (not
  designed for non-JDBC backends). Must implement every JDBC method that
  BenchBase calls, plus metadata queries (`getTables`, `getColumns`, etc.).
  BenchBase's worker threads expect JDBC `ResultSet` semantics. Cannot reuse
  Spark's query planning or DataSource V2 optimisations.

### Option B: Spark JDBC connector (DataStax / Simba)

Use a commercial or open-source JDBC driver for Spark SQL.

- **Pros**: Wire-compatible out of the box.
- **Cons**: DataStax Spark JDBC driver is proprietary and requires licensing.
  Simba ODBC/JDBC drivers are also proprietary. Open-source alternatives
  (Apache Calcite Avatica, Presto JDBC) add an extra service and do not map
  cleanly to Spark Thrift Server.

### Option C: Direct Parquet read via DuckDB JDBC

Skip Spark entirely: BenchBase connects to DuckDB JDBC which reads Parquet
files directly.

- **Pros**: Used in the non-Spark benchmark path (`benchbase-entrypoint.sh`
  config has `url=jdbc:duckdb:...`).
- **Cons**: Does not test the Spark + Flight SQL integration — the very thing
  the project is built for. Cannot measure end-to-end latency through the
  Flight SQL server.

### Option D: HiveExecuteDriver shim (selected)

- **Pros**: ~230 lines of Java. Zero changes to BenchBase. Reuses Spark Thrift
  Server which is already deployed. SQL rewrite is straightforward regex. No
  new services or dependencies.
- **Cons**: SQL rewrite is fragile — TPC-DS queries with complex casts or
  intervals may need additional patterns. Connection/statement proxy stubs out
  JDBC semantics that BenchBase occasionally relies on (e.g., batch operations
  not yet needed).

## Consequences

- **Positive**: BenchBase runs any query against any Spark SQL catalog
  (FlightSource or direct Parquet) by changing the schema name in the JDBC
  URL.
- **Positive**: The `compare` mode runs the same query set against both
  `tpch_flight` (via Flight SQL) and `tpch_direct` (direct Parquet), producing
  an apples-to-apples latency comparison.
- **Positive**: `userClassPathFirst=true` in the Spark Thrift Server config
  prevents Hive JARs from shadowing Arrow/Flight classes during query
  planning. Without this, `ClassNotFoundException` for Arrow types occurs.
- **Negative**: `SqlRewrite` must be kept in sync if BenchBase adds new
  Postgres syntax or if Spark changes its SQL parser.
- **Negative**: Hive JDBC does not support batch INSERT or `Connection.prepareStatement`
  with generated keys — BenchBase's loader phase cannot run through this shim.
  Data loading is done via a separate DuckDB-based path.
- **Negative**: `setQueryTimeout` relies on `BENCHBASE_QUERY_TIMEOUT_SECONDS`
  env var. If BenchBase passes per-statement timeouts differently, the shim
  may need updating.
- **Future**: If BenchBase adds native Arrow Flight support, the shim can be
  retired. Until then, it is the least-invasive bridge between JDBC and Spark.
