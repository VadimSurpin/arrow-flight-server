# StarRocks native Flight SQL catalog — design

Date: 2026-10-17
Branch: `feat/starrocks-native-integration`
Status: awaiting review

## Goal

Build a fork of StarRocks (`branch-4.0`) with a native `flight` external catalog type that reads from this
repository's Flight SQL server. The main motivation is **performance**: stream Arrow batches from the Flight
server straight into StarRocks chunks, skipping the JDBC row conversion used by the existing
`FlightSqlPostgresqlDriver` plugin (see `flight-starrocks/README.md`).

## Scope

In scope (milestone 1):

- `CREATE EXTERNAL CATALOG ... PROPERTIES("type"="flight", ...)`.
- Schema and table discovery through Flight SQL metadata calls.
- Full-table scans, **projection pushdown** and **filter pushdown**.
- Arrow to StarRocks type mapping for primitive types.
- Catalog-level credentials (token or basic auth), TLS.

Out of scope: per-user identity propagation (Ranger per StarRocks user), limit and aggregate pushdown,
writes/sinks, complex types (list/struct/map), upstreaming to StarRocks.

## Environment constraints

- Machine: 4 cores, 11 GB RAM, ~30 GB free disk, no Docker, no sudo.
- Toolchain is installed under `tools/.local/env` (micromamba): OpenJDK 17, Maven 3.9, CMake 4.4, Ninja, Make,
  g++ 14. The directory is excluded locally via `.git/info/exclude`.
- Verification is by a local FE+BE built from the fork (no containers). A full BE build may need swap and the
  prebuilt thirdparty archive; if the BE cannot be built here, stop and report before spending hours.

## Base and layout

- Fork: `~/arrowflight-server/starrocks`, cloned from `branch-4.0` (matches the `4.0.14` image used by the
  existing JDBC E2E), working branch `feat/flight-connector`.
- `flight-starrocks` (this repo) remains the home of `FlightCatalog` and `ScanSql`; the FE consumes it as a jar.
- The C++ `FlightScanReader` is vendored into `be/src/connector/` (BE cannot link an external CMake project).
- `scripts/build-starrocks-fork.sh` in this repo wraps the toolchain environment for reproducible builds.

## FE design (`fe-core`)

- Register `FLIGHT("flight", FlightConnector.class, null)` in `ConnectorType`. Properties: `flight.uri`,
  `flight.auth.*`, `flight.tls.*`. Secrets are masked in `SHOW CREATE CATALOG` and logs.
- `FlightMetadata` implements `ConnectorMetadata` over `FlightCatalog`: `listDbNames`, `listTableNames`, `getTable`
  via `GetDbSchemas`, `GetTables`, `GetExecuteSchema`. Table schemas cached with a TTL (like `JDBCMetaCache`).
- Arrow to StarRocks type mapping: ints, floats, decimal, bool, string/binary, date, timestamp. Unsupported
  types fail at `getTable` with a clear error.
- `FlightScanNode`: takes the pruned column list (projection) and converts expressible conjuncts to SQL (reusing
  the JDBC scan node's expression-to-SQL approach, adapted to the dialect this server accepts); inexpressible
  conjuncts stay residual. `ScanSql` is extended with projection and a `WHERE` clause. It calls
  `FlightCatalog.planScan` once at the FE; each returned endpoint becomes one scan range (locations + ticket).
- Correctness guard: the BE re-applies **all** conjuncts to returned chunks, pushed down or not.
- Thrift: new `TFlightScanNode` and `TFlightScanRange` (ticket bytes, locations, credentials). Credentials and
  tickets must not appear in `EXPLAIN` output or logs.

## BE design (`be/src/connector`)

- `FlightConnector` is registered in `connector.cpp`; it builds a `FlightDataSourceProvider` from
  `TFlightScanNode`.
- `FlightDataSource` (one per scan range / endpoint): `open` builds the Flight client from the range locations
  (TLS by `grpc+tls://`), applies credentials, redeems the ticket; `get_next` pulls the next `RecordBatch` and
  converts it to a `Chunk`; `close` cancels an open stream. It reports raw rows, bytes and IO time.
- `FlightScanReader` gains a pull-style `Next(RecordBatch*)` next to the existing callback API (which stays for
  current tests).
- `flight_chunk_converter` (separate, unit-testable): maps Arrow arrays to StarRocks columns by slot type,
  including nullability; type mismatch with the FE-declared type is a hard error; splits large batches into
  chunk-sized pieces.
- Predicates arrive via `set_predicates` and are applied by the standard scan machinery (the residual guard).
- gRPC errors map to `Status` including the endpoint URI but never the token or ticket. Retries cover
  connection establishment only, not mid-stream failures.
- Open risk: confirm the BE thirdparty build includes the Arrow Flight client libraries. This is the first task
  after cloning; if absent, adjust the thirdparty build.

## Testing

1. FE unit tests: type mapping, `FlightMetadata` with a mocked Flight client, SQL generation for projection and
   filters (unsupported expressions remain residual).
2. BE gtests: `flight_chunk_converter` for every supported type and nulls; `FlightDataSource` against an
   in-process Flight server.
3. Integration on a local FE+BE: `CREATE EXTERNAL CATALOG ... type=flight`, `SHOW DATABASES`, `SELECT COUNT(*)`,
   a projection, a filter, and `EXPLAIN` showing pushdown, against `flight-server` with the repo's test data.
4. Negative cases: bad token, unreachable endpoint, unsupported Arrow type, no credentials in logs/`EXPLAIN`.

Done means all four layers pass and the integration run is recorded under `docs/integrations/` with exact
commands.
