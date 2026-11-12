# Arrow Flight Project Questions: Architecture, Decisions and Next Steps

This document records the discussion and code inspected on October 30, 2026,
Asia/Novosibirsk. It is a study guide and discussion record, not a specification
of an architecture that has already been implemented.

The snapshot predates the partial aggregation extension. For the subsequent
COUNT/MIN/MAX/SUM/AVG implementation, see
[Trino partial aggregation](../integrations/trino-partial-aggregation.md).
References to the working branch and outstanding proposals below describe the
October snapshot.

## 0. Versions Discussed

- Working branch: `feat/trino-flight-connector`, commit `1718b3bf`,
  `fix(trino): enforce safe scan pushdown`.
- Previous schema fix: `41025d50`, `fix(trino): decode Arrow IPC table schemas`.
- Inspected version of Vadim's branch: `upstream/feat/starrocks-native-integration`,
  commit `ebf8f579`.
- The older `feat/starrocks-flight-connector` and newer
  `feat/starrocks-native-integration` branches have different capabilities.

Source links for the working tree are relative. Capabilities found only in
Vadim's branch link to a pinned commit. Local files can differ after switching
branches.

Status labels used below:

- **Implemented**: found in the specified code.
- **Proposed**: discussed as a change, but not implemented at the snapshot.
- **Open question**: requires separate verification or measurement.

## 1. Project Purpose

### What business problem does the project solve?

Provide Spark, Trino and StarRocks with shared data access through Arrow Flight
SQL. Physical storage and part of query execution sit behind a server layer.

The goal is to support multiple storage systems and formats without rewriting
every engine integration whenever a source is added. The discussion prioritized
multiple formats, S3 and completing the Trino/StarRocks integrations.

A common protocol does not make the engines equivalent: connector APIs, types,
optimizers and aggregation pushdown capabilities differ.

### How are responsibilities divided?

- The analytical engine plans SQL and executes the operations retained in its plan.
- The connector negotiates pushdown with the engine and accesses Flight.
- The Flight planner assigns file reads to servers.
- Flight executors read data and execute supported SQL, primarily through DuckDB.
- Arrow Flight transports columnar results over gRPC.

## 2. General Query Path

### Does the client send the entire user query to a Flight node?

Not necessarily. In normal connector execution, the engine first plans the
query and determines which operations can be delegated. The connector builds
SQL for that portion of the work.

In the inspected Trino scan path, a user could write:

```sql
SELECT category, SUM(amount)
FROM flight.sales.orders
WHERE amount > 100
GROUP BY category;
```

Flight received a scan similar to:

```sql
SELECT category, amount
FROM sales.orders
WHERE amount > 100;
```

Trino received rows and performed aggregation itself. Sending a complete SQL
statement through a separate client mode is a different execution path and
must be distinguished from ordinary connector pushdown.

### What happens after the planning request?

1. The receiving Flight node identifies tables and required files.
2. `QueryPlanner` distributes files across available servers.
3. The client receives `FlightInfo`: the result schema and endpoints.
4. The engine creates read tasks.
5. Each task opens `DoGet(ticket)` on the corresponding Flight server.
6. The server reconstructs the task, reads assigned files and sends Arrow batches.
7. The engine executes the remaining operations.

Main server components:
[FlightSqlProducer](../../flight-server/src/main/java/pro/surpin/data/arrowflight/server/adapters/FlightSqlProducer.java),
[QueryPlanner](../../flight-server/src/main/java/pro/surpin/data/arrowflight/server/services/QueryPlanner.java),
[ExecutionService](../../flight-server/src/main/java/pro/surpin/data/arrowflight/server/services/ExecutionService.java).

## 3. HDFS: Data Placement and Metadata

### What is the main HDFS node? Does it store the data?

The discussion used “main” to mean the **NameNode**. It stores filesystem
metadata: the namespace, file information and block locations.

**DataNodes** store file blocks. One file can span blocks on multiple machines
and have replicas. Local reading depends on proximity to DataNodes; locating
Flight beside the NameNode does not place it beside the data blocks.

### Does ParquetAdapter access the NameNode or Hadoop FileSystem?

These are different layers of the same interaction. **Hadoop FileSystem is a
software API**, not another server replacing the NameNode.

With HDFS, directory listings and block-location requests use the HDFS client
and involve the NameNode. File contents are read from DataNodes. The same API
can access local disks through a local filesystem implementation or object
storage through S3A.

### How are files discovered?

In the inspected Trino branch:

1. [ParquetAdapter.localFileInventory](../../flight-server/src/main/java/pro/surpin/data/arrowflight/server/adapters/ParquetAdapter.java)
   scans the configured directory, file sizes and block locations.
2. A node selects files with at least one block on its configured storage host.
3. `ClusterService.registerLocalFiles` publishes that inventory to Hazelcast.
4. `ClusterService.fileLocations` combines the inventories.
5. `QueryPlanner` uses the combined registry when assigning files.

One local block does not mean the entire file can be read locally. Other blocks
may require network reads.

### Which DataNodes should run a Flight process?

With the inventory-based design, verify that all required files appear in the
registry. Running Flight on only an arbitrary subset of DataNodes can leave
files outside the published inventories.

**Proposed:** allow executors to read any file in shared storage, preferring
locality where available. Flight could then run on selected machines with spare
CPU and memory rather than on every DataNode. For S3, executors could run in a
separate compute cluster with storage access.

## 4. Hazelcast and a Planning Redesign Proposal

### Is publishing files to Hazelcast redundant?

For shared HDFS, some information duplicates filesystem metadata. The project
currently combines two maps:

- Physical file and block placement.
- Flight server availability, addresses, load and published files.

Hazelcast provides server registration and coordination. Per-server file
inventories are an implementation choice, not an Arrow Flight requirement.

Inventories have independent value for separate local disks. Shared HDFS/S3
could instead use centralized file discovery. Any additional registry needs
refresh rules when files or cluster membership change.

### What alternative was proposed?

**Proposed, not implemented:** separate file discovery from executor registration.

1. A coordinator discovers the complete table file list from storage or a catalog.
2. It caches file lists and metadata with defined refresh rules.
3. It obtains available executors, addresses, storage hosts and status from Hazelcast.
4. It creates tasks and assigns them using size, load and locality.
5. If no local executor exists, it assigns remote reading to another executor.
6. It returns executor endpoints to the client.

Results flow directly from executors to the client; the coordinator need not
relay the full result. Choosing which machines run Flight should not determine
which table files the planner can see.

### Is a separate metadata component needed?

A schema and table-file/task discovery contract would support ordinary HDFS/S3
directories and, later, table-format catalogs.

This is a design direction, not an existing generic API. Storage access,
physical file formats and analytical engine adapters should remain separate
contracts.

## 5. Which Flight Node Plans a Query?

### Is the planning node selected randomly?

The client connects to a configured node. Trino, for example, uses `flight.host`
and `flight.port`. That node handles `FlightInfo` requests.

`QueryPlanner` exists on Flight nodes; the design does not require a separate
coordinator process. The inspected client does not randomly choose its first
node. An external load balancer could make that choice.

### Can one node be designated as the main Flight node?

Yes. A dedicated **Flight coordinator** with a client-visible address was
discussed. Proposed roles were:

- `coordinator`: metadata, planning and endpoint creation.
- `worker`: task execution and result streaming.
- `combined`: both roles, useful for local development.

**These roles were not implemented.** Planning and execution components were
assembled together. A role split would also require registry and availability
checks to distinguish coordinators from executors.

### Can the coordinator run beside the NameNode to avoid network access?

It can, and this is reasonable for a small test setup. Flight still uses the
HDFS API: communication becomes local, but request handling, listings and
metadata processing remain.

Measure the benefit. Placement alone does not eliminate planning overhead;
caching and reducing repeated calls may matter more. The production deployment
recommendation discussed was a lightweight coordinator and separate DuckDB
executors, keeping their load away from the NameNode.

## 6. FlightInfo, Endpoints, Tickets and DoGet

### Are endpoints inside tickets?

`FlightInfo` contains a schema and `FlightEndpoint` entries. Each endpoint
contains a ticket and locations:

```text
FlightInfo
  schema
  endpoints
    endpoint
      locations: addresses serving one result
      ticket: information used to request that result
```

Multiple locations on one endpoint are alternative addresses for the same
result, not independent data partitions.

### What does this project's ticket contain?

The server packs `HandleState`: SQL, assigned file paths, server identity and
load-accounting information. The payload is signed. Clients treat tickets as
opaque bytes and do not need to decode their structure.

Code:
[HandleState](../../flight-server/src/main/java/pro/surpin/data/arrowflight/server/model/HandleState.java),
[EndpointTicketCodec](../../flight-server/src/main/java/pro/surpin/data/arrowflight/server/services/EndpointTicketCodec.java).

### Does DoGet receive a list of operations?

`DoGet` receives a ticket. The server restores SQL and assigned files, then
executes the task. The ticket does not contain a serialized DuckDB physical
operator plan. One SQL statement may require scanning, filtering, projection
and aggregation; one `DoGet` can send many Arrow batches.

### Who builds SQL, tasks and physical plans?

- The analytical engine optimizer selects supported pushdown.
- The connector builds SQL for Flight.
- `QueryPlanner` assigns files and creates endpoints.
- `EndpointTicketCodec` encodes the task.
- `ExecutionService` builds DuckDB SQL for the assigned files.
- DuckDB chooses its local physical execution plan.

### Is this plan guaranteed to be optimal?

No general guarantee exists. The client engine and DuckDB optimize separately.
`QueryPlanner` primarily assigns files; it is not a unified optimizer for
Spark/Trino and all DuckDB instances. More pushdown or more endpoints does not
necessarily reduce elapsed time. Compare execution plans and measurements over
the same data.

## 7. Spark Tasks and Flight Parallelism

### Does Spark create a worker for every endpoint?

The connector normally creates an input partition per endpoint. Spark schedules
read tasks on executors. Executor count comes from application resources and
the cluster manager and need not equal endpoint count. For example, three
executors can process 100 read tasks in several waves.

### Does Spark have something like a thread pool?

As a simplified model, the driver assigns tasks and executor processes run
them in multiple threads across machines. Three executors with four cores each
can run roughly 12 tasks concurrently when each task uses one core.

A standalone Spark worker, an executor and a thread are different entities.
Spark executors and Flight servers are also separate system participants.

### Can a Flight node read and process data in parallel?

Parallelism exists at several levels:

1. Different Flight nodes execute different tasks.
2. One node serves several `DoGet` requests.
3. DuckDB uses multiple threads within a SQL statement.
4. Parquet row groups can be scanned in parallel, including within one file.

`duckDbThreads` configures `SET threads`. One endpoint does not imply one
DuckDB thread. Coordinate concurrent request count and threads per query with
the node's total resources.

### Did Vadim create more tasks than servers?

**Yes, at whole-file granularity.** The native branch has `endpointsPerServer`,
defaulting to `1`. `QueryPlanner.splitByBytes` divides a server's assigned files
into several groups of similar size.

Commit: [1d3f331a](https://github.com/VadimSurpin/arrow-flight-server/commit/1d3f331a1a9492585034615f72e47a5992a4d9a1).
The inspected benchmark configurations used `3`.

### Is one file distributed by row groups across Flight nodes?

No such implementation was found in the inspected server planner. Tickets carry
file paths rather than row-group ranges. Reading row-group statistics, pruning
and DuckDB's internal parallelism do not distribute row groups across Flight
nodes.

### Should row-group scheduling be added now?

First compare existing file assignment with multiple endpoints per server, using
both ordinary datasets and skewed layouts with a few large files.

If large files leave other nodes idle, a prototype could assign row-group
ranges. It would need a task representation, ticket changes and a reader that
reads only assigned groups without rescanning the whole file. Concurrency
limits and rules for combining small tasks would also be needed.

A dynamic queue from which idle nodes take work is another proposed stage,
not an implemented performance guarantee.

## 8. Streaming, Batches and Processing Models

### How does streaming differ from materializing a complete result?

A streaming consumer receives and processes portions incrementally. Full
materialization first collects the entire result. Streaming suits large results
and bounded reader memory; materialization can suit a small result reused in
memory.

### Are streaming and batched reads opposites?

Arrow Flight streams **RecordBatches**. Transfer is both streaming and batched:
each batch contains multiple rows represented as columns.

Processing terminology describes something else:

- Batch processing handles a finite dataset, such as yesterday's report.
- Stream processing continuously handles arriving events, such as Kafka data.

A finite SQL query can use batch processing and stream its result.

### Do Arrow batches prove DuckDB has not materialized the result?

No. Inspect JDBC mode and query operators separately. In the native branch,
Vadim explicitly enabled `jdbc_stream_results=true`; that setting was absent
from the Trino branch at this snapshot. Earlier references to streaming
therefore described transport and client reading, not a guarantee that DuckDB
avoids materialization.

Operators such as sorting and aggregation can also retain state or process the
whole input before producing output, even if their source streams.

### Does Arrow always contain partial aggregates?

Arrow defines data representation, not SQL execution stage. It can carry
filtered rows, partial aggregate states or final results.

## 9. Trino Implementation at the Snapshot

### What runs on Flight and what runs in Trino?

**Implemented at the snapshot:** Flight reads files, selects required columns
and applies supported exact WHERE conditions. Trino executes remaining filters,
computed expressions, JOIN, GROUP BY, aggregates, sorting and LIMIT.

Independent partial aggregation across endpoints was not enabled in this scan
mode, which did not require a Trino engine patch.

### What changed in the latest scan fix?

- `FlightFilters.split` separates exactly translatable domains from residual filters.
- `applyFilter` retains accepted conditions and returns unsupported domains and expressions to Trino.
- `applyProjection` collects columns used by expressions rather than every supplied assignment.
- A column used only by a server-side filter need not be transferred.
- A technical column preserves row counting when no user columns are requested.
- Reader cleanup failure no longer replaces the original read failure.

See [Trino scan pushdown](../integrations/trino.md).

### Which filters are supported?

Integer, boolean, decimal and date comparisons, string equality and IN with
representable UTF-8 literals, and NULL checks are supported.

REAL/DOUBLE comparisons, string ranges and unsupported expressions remain in
Trino. Invalid UTF-8 or NUL literals remain there too. If part of a domain
disjunction is unsupported, pushing only its supported branch could discard
rows; the complete domain must remain in Trino.

### Which tests were added, and what happened to them?

Tests initially covered filter precision and NULL, projections, independent
stream reading and residual execution in Trino.

At the user's explicit request, **12 added unit tests and two integration
tests** and their fixtures were removed. Existing tests remained. One existing
expectation changed so an exactly executed server filter was not retained for
duplicate evaluation in Trino.

Compile, Checkstyle, SpotBugs and 740 existing unit tests passed afterward.
This result describes the pinned version, not later branches. At that point
in the discussion, the user did not want additional tests.

## 10. Partial Aggregation and a Trino Patch

### Does Spark support aggregation pushdown?

Spark DataSource V2 provides interfaces, but the connector must implement them,
validate expressions and agree on the result schema. The project's
`FlightScanBuilder` implements `SupportsPushDownAggregates`. When partial
pushdown is accepted, Spark performs the required final merging.

### Why retain aggregation inside Trino?

The inspected Trino 446 connector API did not provide the general contract
needed here: independent partial aggregates from several Flight endpoints
with automatic final merging in Trino.

Trino can aggregate scanned rows and can delegate aggregation to a source that
guarantees complete results. Those paths differ from returning independent
partial states.

### Has Trino already tried and failed to solve this?

[PR #21550](https://github.com/trinodb/trino/pull/21550), about declared
decomposition of aggregate functions, was discussed. It was open and unmerged
when inspected. It concerns a broader aggregation change, with discussion of
spilling and optimizer interactions, including partial aggregation through JOIN.
An open PR does not prove a limited solution for this project is impossible.

Its upstream status can change and should be checked before further work.

### Can a custom Trino patch be written?

Yes, as a research project. It must explicitly describe connector partial
results and preserve correct final aggregation in the plan.

The initial prototype discussed COUNT(*), ordinary grouping columns and at
least two endpoints. MIN/MAX and more involved SUM/AVG could follow with type,
overflow and precision checks. Simply removing a restriction from full
aggregation pushdown could make the engine interpret partial results as final.

At the snapshot, the immediate priority was the ordinary filter/projection
connector. A fork would require maintained builds and compatibility with engine
updates. Subsequent partial aggregation work is documented separately above.

## 11. StarRocks: Java Modules and Engine Patches

### Why are there about seven Java files in StarRocks and more in Trino?

Comparing Java directories alone is incomplete. The native branch contains
substantial C++ code and a patch to StarRocks itself. Trino uses a separate Java
SPI plugin; native StarRocks integration changes FE, BE, planning and Thrift.

### Did Vadim modify StarRocks itself?

The native integration includes engine source changes:

- FE: a `type=flight` catalog, metadata, type mapping and scan planning.
- BE: Flight reading and conversion from Arrow batches to StarRocks chunks.
- Read parameters transferred through Thrift.
- Optimizer rules, including supported aggregate pushdown and merging above scans.

See the [patch](https://github.com/VadimSurpin/arrow-flight-server/blob/ebf8f579/starrocks-fork/flight-connector.patch)
and [native integration guide](https://github.com/VadimSurpin/arrow-flight-server/blob/ebf8f579/docs/integrations/starrocks-native.md).

### What is a patch, and how is it used?

A patch is a textual description of source changes. The build checks out an
official StarRocks commit, applies the patch and builds modified FE/BE binaries.
Installing official StarRocks alone does not enable the integration. Native
integration needs prepared binaries or custom images; patching happens at
build time, not for every query.

The project has an [image build workflow](https://github.com/VadimSurpin/arrow-flight-server/blob/ebf8f579/.github/workflows/starrocks-images.yml).
The workflow's existence does not establish that a particular published image
tag is available; that requires separate verification.

### Were these changes accepted upstream?

The inspection did not find a `VadimSurpin` pull request in `StarRocks/starrocks`
for this integration. It confirmed a maintained patch and custom build in this
project. Not finding that PR does not rule out related work by other authors.

### How does the StarRocks aggregation patch work?

`PushDownAggToFlightScanRule` pushes supported aggregates into a Flight scan
while preserving the necessary merge in StarRocks. Partial COUNT values merge
through SUM; AVG can use partial SUM and COUNT followed by division. Supported
types and expressions constrain the transformation.

The separate JDBC integration path is not the same implementation as the
native catalog with custom FE/BE builds.

## 12. S3, S3A, MinIO and File Formats

### Is the project limited to HDFS? Where is S3 support?

S3 support was found in the inspected `upstream/feat/starrocks-native-integration`.
That does not establish support in every StarRocks-named branch or in the
Trino branch without comparison.

The main implementation is
[S3Settings](https://github.com/VadimSurpin/arrow-flight-server/blob/ebf8f579/flight-server/src/main/java/pro/surpin/data/arrowflight/server/adapters/S3Settings.java),
which coordinates Hadoop S3A and DuckDB httpfs settings. Dependencies, startup
configuration and URI conversion for DuckDB also matter.

This is primarily server-side support. The analytical engine receives Arrow
and need not read S3 itself for that query path.

### Is S3A a storage system or a filesystem?

S3 is object storage. S3A is a Hadoop connector exposing it through the Hadoop
FileSystem API, usually with `s3a://...` URIs. A shared API does not make S3 behave
like HDFS: metadata, network access and object operations differ, and S3 has
no HDFS block locality.

### What is MinIO?

Self-hostable S3-compatible object storage. It allows testing the S3 path
without requiring AWS S3.

### What data format does S3 store? Can DuckDB read it?

S3 stores objects: bytes addressed by keys. An object can contain Parquet,
ORC, CSV or another format. S3 itself is not a table format.

DuckDB can read Parquet from S3 with appropriate access configuration. The
project must configure both Hadoop-based discovery and DuckDB execution.

### Was adding S3 equivalent to adding HDFS?

Hadoop FileSystem permits some reuse, but S3 also requires endpoints,
credentials, implementation compatibility, dependencies and DuckDB settings.
HDFS locality does not apply to S3. Supporting one configured data root does
not automatically provide a universal catalog of independent storage systems.

### Where is the Parquet dependency? How would ORC be added?

The server discovers `.parquet` files, reads footers, converts schemas and
builds `read_parquet(...)`. ORC needs coordinated discovery, schema reading,
execution and statistics. The selected backend's support, including remote
access, must be verified separately.

A common server-side format/reader contract with Parquet and ORC implementations
was discussed. Engine connectors would continue receiving Arrow. Adding `.orc`
to an extension list alone would not provide support.

## 13. Improving Flight Node Performance

### Can node execution be changed to improve performance?

The discussion prioritized concrete execution changes before larger scheduling
redesigns. Vadim's inspected native branch contained changes absent from the
Trino snapshot:

1. **JDBC result streaming:** `jdbc_stream_results=true`,
   [d0cdfc7d](https://github.com/VadimSurpin/arrow-flight-server/commit/d0cdfc7de9873a9c56029ca1ce7bb8c2bfafaf2b).
2. **One DuckDB instance per server, with duplicate() connections:** shared
   thread and memory budgets,
   [f5f11b38](https://github.com/VadimSurpin/arrow-flight-server/commit/f5f11b382b27fb6dc1bb813a594d54fd24b33f27).
3. **Configurable prepared-statement caching, disabled by default:** avoids
   accumulating plans for changing queries,
   [d4632895](https://github.com/VadimSurpin/arrow-flight-server/commit/d46328952ce52e6e029971596f31c2fd195862c7).
4. **Multiple endpoints per server:** whole files divided into size-balanced groups.

Porting selected server fixes while preserving Trino client changes was the
proposed next step; it had not been completed at this snapshot.

### What should be measured before further redesign?

- Planning time and time to first batch.
- Total query duration.
- Bytes read and transferred.
- CPU, memory and spill.
- Individual task durations and inter-node skew.
- Behavior under concurrent queries.

Use the same data and configuration, distinguishing cold and warm runs.
Increasing threads or endpoints alone does not guarantee a speedup.

### Which benchmark findings were discussed?

The native branch includes
[Stage A findings](https://github.com/VadimSurpin/arrow-flight-server/blob/ebf8f579/benchmarks/suites/runs/findings-stageA.md)
for TPC-DS SF100: Flight versus direct Spark reads across layouts, discovered
bugs and remaining issues. These were single runs without warmup; they do not
establish a universal speedup or isolate the effect of one change.

Numerical **S3 benchmark** results tied to a reproducible configuration were
not recorded in this discussion. This remains an open question. HDFS/local
results must not be presented as S3 measurements.

## 14. ACID, RPC Atomicity and Consistent Reads

### Is one DoGet atomic?

It is one read task or RPC. It can send several batches and then fail; the
client engine must handle failure of the overall query. One ticket does not
create a transaction across Flight nodes, and already transferred data cannot
be undone through a network rollback.

### What does ACID mean?

- **Atomicity:** transaction changes commit together or do not commit.
- **Consistency:** declared data constraints are preserved.
- **Isolation:** concurrent operations interact under the chosen isolation level.
- **Durability:** acknowledged changes persist.

ACID is not a property of Arrow representation or an individual Flight call.
Transaction guarantees must be defined for tables, operations and catalogs.

### Which consistency problem matters for reading?

All tasks need a consistent data version. If one executor reads an old file A
and another reads an updated file B, the result may mix table states. A fixed
path list is insufficient if files can be overwritten or deleted. Metadata
cache TTL does not create a snapshot.

### Should the project implement its own ACID mechanism?

The recommendation discussed was to focus on a consistent-read contract before
building a complete transaction manager:

1. A query selects a specific table snapshot.
2. Its plan and tickets reference that snapshot.
3. Every executor reads the snapshot's files.
4. Those files remain immutable and available until the query finishes.

This was separate work, not an implemented part of the proposed changes.

### What can provide snapshots and transactions?

- [HDFS snapshots](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html)
  provide read-only directory snapshots where configured and available.
- [Apache Iceberg](https://iceberg.apache.org/spec/) provides table snapshots
  and atomic metadata changes. Correct metadata and delete handling are needed;
  listing Parquet files alone is insufficient.
- Plain Parquet directories can use an operational contract of immutable,
  published dataset versions, provided writers honor it too.

A snapshot of one table does not automatically provide a transaction spanning
multiple tables. Define consistency boundaries separately.

## 15. Arrow IPC and Client-Core Abstractions

### What did “a schema in StarRocks format” mean?

That wording was inaccurate. The discussion concerned a **standard Arrow IPC
schema message** used by the StarRocks server branch, not a proprietary
StarRocks format.

### What does the schema decoder do?

[TableSchemaDecoder](../../flight-client-core/src/main/java/pro/surpin/data/arrowflight/client/metadata/TableSchemaDecoder.java)
restores an Arrow `Schema` from metadata bytes, supporting the IPC representation
in use and the older unframed encoding. It contains neither Trino SPI nor Trino
type conversion; the Trino adapter performs conversion separately.

### What was requested for client-core?

Common contracts across connectors, with engine-specific implementations for
Spark/Trino/StarRocks. Protocol, Arrow schemas, endpoint descriptions and batch
reading can be shared. Engine types, expressions and residual-filter semantics
belong to engine adapters.

A complete abstraction redesign of `flight-client-core` **was not performed**.
Implementation paused for design discussion. The module contains both common
contracts/models and concrete client code.

## 16. Reading the Code and Understanding Builds

### Where should a data engineer start?

The Trino connector was selected as the practical starting point: scans,
filters, projections, splits and pages, followed by real Trino plans and
measurements of read costs. An optimizer patch is a separate, deeper topic
after understanding the ordinary connector.

### Which Trino files should be read first?

Paths are relative to `flight-trino/src/main/java/pro/surpin/data/arrowflight/trino/`:

1. `FlightConnector.java`: connects the connector components.
2. `metadata/FlightMetadata.java`: metadata and filter/projection negotiation.
3. `metadata/FlightTableHandle.java`: scan description, without the data itself.
4. `pushdown/FlightFilters.java`: domain translation and supported-filter splitting.
5. `pushdown/FlightQueries.java`: SQL sent to Flight.
6. `read/FlightSplitManager.java`: endpoints as Trino read tasks.
7. `client/FlightService.java` and `CoreFlightService.java`: access contract and shared-client bridge.
8. `read/FlightPageSource.java`: Arrow batches converted into Trino pages.
9. `type/ArrowTypes.java` and `ArrowToTrinoBlockConverter.java`: types and value conversion.

Read `FlightPlugin`, `FlightConnectorFactory`, configuration and packaging on
a second pass.

### How should the project be built and checked in WSL?

The discussed branch requires Java 21. Start with the [README](../../README.md)
and [build guide](../user_guides/build-test-and-scripts.md).

```bash
java -version
./mvnw -pl flight-trino -am package -DskipTests
```

This command **builds** the plugin and required modules; it does not start
Trino. A regular Trino server needs a separate process/container, installation
of the plugin and catalog configuration. The branch also contains an integration
test profile using `DistributedQueryRunner`; this is a test fixture, not a
production deployment guide.

### What do ./mvnw, -pl, -am and other options mean?

- `./mvnw`: Maven Wrapper selects the configured Maven version.
- `compile`: compile production code.
- `test`: execute tests selected by the active profiles.
- `package`: package artifacts according to the module.
- `checkstyle:check`: check code style.
- `spotbugs:check`: analyze bytecode statically.
- `-pl flight-trino`: select a module.
- `-am`: also build its reactor dependencies.
- `-DskipTests`: skip test execution; tests are usually still compiled.
- `-o`: offline mode, using only dependencies already available locally.

A successful build does not mean a service has been started or is reachable.

### Why can a repeated build be fast?

Dependencies may already be in Maven's cache and current classes in `target`.
Incremental compilation need not rebuild unchanged code. Check Maven's final
status and selected goals, not only elapsed time.

The session also found an IDE-generated `.class` with
`Unresolved compilation problem`, rather than normally compiled source. A clean Maven build resolved
those failures. Stale artifact failures are not automatically source defects.

## 17. Recorded Priorities and Open Decisions

### What had been completed in the working branch?

- Arrow IPC metadata schema compatibility.
- Safe separation of Trino and Flight filters.
- Projection based on inputs actually used.
- Preservation of the original failure when reader cleanup fails.
- Removal of newly added tests at the user's request, retaining existing tests.

### Which directions were proposed but not implemented?

1. Port selected DuckDB server fixes from the native branch.
2. Separate Flight coordinator and executor roles explicitly.
3. Discover all shared-storage files independently of local inventories.
4. Assign work with remote-reading fallback and load awareness.
5. Add row-group tasks and dynamic assignment if skew justifies them.
6. Define consistent snapshot reading.
7. Add a server format abstraction for ORC.
8. Develop shared client-core contracts without engine API dependencies.

These are possible work items, not an approved combined implementation plan.
Select a concrete stage before implementing them.

### What still needed investigation?

- Actual NameNode/DataNode/Flight placement and machine resources.
- Primary storage model: local shards, shared HDFS, S3 or a combination.
- Writers that change data and whether immutable versions are feasible.
- Actual bottlenecks in target queries.
- Reproducible S3 benchmark results and execution conditions.
- Whether transactional table changes are required or consistent read-only snapshots suffice.
- Whether to pursue a research Trino patch after completing the ordinary connector.

## Sources and Further Reading

- [SQL query execution flow](sql-query-execution-flow.md).
- [Hadoop and Parquet storage](hadoop-parquet-storage.md).
- [ClusterService](cluster-service.md).
- [File distribution decision](../adr/0002-file-execution-distribution.md).
- [Trino scan pushdown](../integrations/trino.md).
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).
- [Spark: Cluster Mode Overview](https://spark.apache.org/docs/3.5.9/cluster-overview.html).
- [DuckDB: Parquet and parallelism](https://www.duckdb.org/docs/current/guides/performance/file_formats).
- [DuckDB JDBC Driver](https://github.com/duckdb/duckdb-java/blob/main/src/main/java/org/duckdb/DuckDBDriver.java).
- [Trino PR #21550](https://github.com/trinodb/trino/pull/21550).
- [Apache Iceberg specification](https://iceberg.apache.org/spec/).

External links and upstream statuses can change. This document records the
discussion and versions inspected on the stated date.
