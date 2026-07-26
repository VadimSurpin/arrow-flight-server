# SQL Query Execution Flow

This document describes how an SQL query flows through the Arrow Flight server: from receiving the query to reading Parquet files, executing operations, and sending the result back to the client.

## Main Components

`HadoopArrowFlightServer` starts the server and configures Hadoop FileSystem, Hazelcast, and Arrow Flight.

`FlightSqlProducer` implements the Flight SQL layer. It receives queries, creates `FlightInfo`, builds `Ticket` objects, creates endpoints, and restores query state during `DoGet`.

`ExecutionService`, `MetadataService`, and `ParquetAdapter` handle Parquet-related work. `ParquetAdapter` owns file discovery and locality detection. `MetadataService` handles schema construction and Java footer fast paths. `ExecutionService` orchestrates DuckDB execution.

`ParquetQueryParser` parses SQL and extracts schema, table, selected columns, filters, aggregations, and group by columns.

`AppConfig` / `ConfigAdapter` loads runtime tuning values from `arrowflight.properties`, with JVM system properties and selected environment variables as overrides.

## High-Level Flow

A query goes through two main phases.

The first phase is planning. The client calls `GetFlightInfo`; the server determines the result schema, distributes files across Flight nodes, and returns endpoints with tickets.

The second phase is reading. The client calls `DoGet` using the tickets, and the corresponding Flight nodes read their assigned files and stream the result in Arrow format.

Data is not read during `GetFlightInfo`. Actual reading starts only during `DoGet`.

## Receiving the SQL Query

The SQL query enters the server through `FlightSqlProducer.getFlightInfoStatement`.

At this stage, the server:

1. Builds or reuses the cached Arrow result schema.
2. Builds endpoint assignments from the cached distributed file inventory.
3. Computes Parquet byte and footer row-count estimates.
4. Returns those estimates in `FlightInfo`.

## Hazelcast Cache and Ticket

Each endpoint ticket contains the SQL query, assigned relative file paths, target
server, byte estimate, and load-accounting flag. The payload is authenticated
with HMAC-SHA256 using a cluster-shared secret created through Hazelcast. Any
Flight node can therefore verify a ticket without a distributed lookup.

When every shard has one owner, endpoint planning and `DoGet` do not store or
load per-query state in Hazelcast. For replicated shards, Hazelcast retains only
a 10-minute load lease so an abandoned ticket cannot leak its reservation.
Legacy UUID handles still use the local/distributed statement-cache fallback.

## Building the Result Schema

Before returning `FlightInfo`, the server must know the result schema. For that, `FlightSqlProducer` calls `MetadataService.getQuerySchema`.

The result schema is built before reading data. For regular queries, it is derived from the Parquet table schema and the selected columns. For aggregations, it is derived from aggregation expressions and group by columns.

This schema is returned to the client as part of `FlightInfo`.

## File Distribution

File distribution is performed in `QueryPlanner.plan`.

The planner parses the SQL and filters the distributed file inventory to the
referenced tables. Table-to-file assignments are cached for 30 seconds and are
refreshed immediately if current cluster membership invalidates a cached plan.

Next, the server retrieves all registered Flight servers from `serverRegistry`. Each Flight node registers itself in `serverRegistry` during startup.

For each file, the server calls `pickServer`. This method chooses the Flight server that will read the file.

If a Flight server runs on a host that stores the file blocks, that node is preferred. If no useful locality is available, the file is assigned using round-robin distribution across all Flight servers.

The result of this phase is a grouping of files by server. A separate endpoint is created for each group.

## Endpoint and Location

`Location` is the network address of a Flight server. It tells the client which node should be contacted to read a specific part of the result.

`FlightEndpoint` contains a `Location` and a `Ticket`. Conceptually, an endpoint tells the client to use this ticket with this node.

For every endpoint, the server creates a signed self-contained ticket containing
the SQL query and the file list assigned to that endpoint.

## Reading by Ticket

When the client calls `DoGet`, the server enters `FlightSqlProducer.getStreamStatement`.

The server verifies the ticket signature and decodes the SQL query and assigned
file list locally.

The server then calls `ExecutionService.readParquet`, passing the allocator, SQL query, file list, listener, and stream-start flag.

Actual query execution starts at this point.

## Query Engine Selection

The project uses two execution paths for Parquet reads. The route is selected in `ExecutionService.readParquet` after `ParquetQueryParser` has extracted projection, filter, aggregation, and group-by information.

Routing rules:

1. Metadata-only aggregates use Java and Parquet footers.
2. Full scans, projections, filters, general aggregations, and joins use DuckDB.

Java handles cases that can be answered without reading data pages; every remaining query is executed by DuckDB.

## Java Footer Path

The Java footer path is used for simple aggregate queries without `WHERE` and without `GROUP BY`.

Supported fast-path expressions are:

- `COUNT(*)`
- `COUNT(col)`, when Parquet null-count statistics are complete
- `MIN(col)`, when Parquet min statistics are complete
- `MAX(col)`, when Parquet max statistics are complete

For `COUNT(*)`, Java sums row counts from Parquet row-group metadata. For `COUNT(col)`, Java subtracts null counts from row counts. For `MIN` and `MAX`, Java merges per-row-group statistics.

This path does not read column data pages and does not start DuckDB for the query. If required statistics are missing or incomplete, execution falls back to DuckDB.

## DuckDB Path

DuckDB is used for every query that cannot be answered from Parquet footer metadata:

- full scans and projections
- filtered scans (`WHERE ...`)
- filtered projections
- `GROUP BY`
- `SUM`
- aggregates that cannot use footer statistics
- joins

`ExecutionService` builds SQL over DuckDB's `read_parquet([...])` table function. Join table aliases are temporary DuckDB views over the corresponding Parquet inputs.

DuckDB returns results through `DuckDBResultSet.arrowExportStream`. The server streams each DuckDB Arrow batch directly through Flight after the client is ready, without intermediate buffering or ownership transfers.

DuckDB reads local files without an extension. HDFS URIs require the configured DuckDB HDFS extension.

## Runtime Tuning

The default runtime configuration lives in `src/main/resources/arrowflight.properties`.

The most important streaming values are `batchSize` and `flightBackpressureThresholdBytes`.
They control DuckDB Arrow export granularity and how much serialized output Flight may pipeline before waiting for the client.

I/O parallelism is also configurable. If `ioParallelism` is set, that exact thread count is used. Otherwise the value is derived from available CPU cores:

`max(ioParallelismMinThreads, min(availableProcessors, ioParallelismMaxCores) * ioParallelismMultiplier)`

Use `ioParallelismMaxCores=0` to mean "no core cap". JVM system properties such as `-Darrowflight.io.parallelism=64` can override the file configuration.

## Sending the Result to the Client

A regular `SELECT` usually sends multiple Arrow batches. Aggregations often send a single final batch, although the result size depends on the query and the number of groups.

Sending is performed through `ServerStreamListener`. `VectorSchemaRoot` contains the current Arrow batch, and `putNext` sends it to the client.

After reading is complete, the server completes the stream.

## Current Execution Limitations

The current implementation distributes work at the whole-file level. A single large file is not split between multiple Flight nodes.

Round-robin distribution balances the number of files but does not account for data volume. For more accurate benchmark/production scenarios, file sizes or row groups should be considered.

Signed endpoint tickets are self-contained. Only replicated-shard load leases
depend on Hazelcast, and lease expiry affects accounting rather than ticket
decoding.

Actual query execution starts only during `DoGet`. `GetFlightInfo` is responsible for planning, schema construction, endpoints, and tickets.
