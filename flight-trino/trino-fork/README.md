# Trino 446 partial aggregation pushdown

This patch lets the Flight connector produce native Trino intermediate count states.
Trino's existing final aggregation combines those states. DuckDB computes each
endpoint's partial count; the final aggregator runs in Trino, including on a
coordinator configured to execute tasks. No Trino worker is needed on a Flight node.

## Scope

The initial implementation supports `COUNT(*)`, `COUNT(column)` and one ordinary
grouping set. Count inputs and grouping keys support BIGINT, INTEGER, SMALLINT,
TINYINT, BOOLEAN and VARCHAR. Unsupported aggregates, types, DISTINCT, aggregate
FILTER/ORDER BY, masks, expressions and residual row filters retain Trino's original
execution path. A request containing an unsupported aggregate is declined as a whole.

The connector defaults to disabled partial pushdown. Enable it per query session:

```sql
SET SESSION flight.partial_aggregation_pushdown = true;
SELECT k, count(*), count(v) FROM flight.test_schema.grouped GROUP BY k;
```

Replace `flight` with your catalog name. Disable the property for the baseline.
The property has no effect on stock Trino: the patched planner must call the new SPI.

Each source partition must cover a disjoint part of the input. Explicit partition
predicates retain their existing requirement to cover all rows without overlap.
Partial count states are signed Arrow Int64 values, non-null and nonnegative.
The reader rejects malformed states. COUNT returns zero for an empty global input;
empty grouped input produces no groups. HAVING stays above final aggregation.
If planning returns no endpoints, the connector supplies one local zero-state split
for a global count. Grouped scans still produce no rows. The zero-state split opens
no Flight stream.

## Engine changes

`BASE_SHA` pins the upstream Trino 446 commit. `flight-connector.patch` is a unified
Git patch, applied with `git apply`, not a mailbox series for `git am`.

The patch adds `ConnectorMetadata.applyPartialAggregation`, with an empty default
implementation, and forwards it through the engine metadata and tracing wrappers.
It reuses `AggregationApplicationResult` to describe a scan of intermediate states:
projections have the requested intermediate types, assignments include grouping
columns, and grouping-column mappings retain the existing meaning. Intermediate
types and their representation belong to the pinned Trino version; a connector
must decline states it cannot reproduce exactly.

The returned connector handle must declare `producesPartialAggregation()`. This
default-false SPI method identifies a state producer to Trino's default-value plan
validator. The validator retains its checks for single-node/single-stream execution
or the required exchanges. The connector is responsible for empty global defaults.

`PushPartialAggregationIntoTableScan` runs after normal partial/final planning and
before hash generation. It replaces a supported PARTIAL directly above a scan with
a state scan, preserving the existing FINAL/INTERMEDIATE consumers. It also handles
a SINGLE directly above a scan by retaining a FINAL over connector states, covering
plans that do not already contain a partial stage. Only built-in, decomposable
aggregates are eligible. The implementation does not register Flight as a Trino
worker and does not add an aggregation algorithm or a Flight-to-Flight exchange.

`FlightMetadata` intentionally declares the additional SPI method without
`@Override`: all parameter/result types already exist in stock 446. Consequently the
connector builds against stock 446 and the same method is dispatched by the patched
SPI at runtime. Existing connectors keep the default empty response.

## Apply and build the engine

Run in Linux/WSL with Java 21, starting in this repository:

```bash
git clone https://github.com/trinodb/trino.git ../trino
git -C ../trino checkout -b flight-partial-aggregation \
  "$(cat flight-trino/trino-fork/BASE_SHA)"
bash flight-trino/trino-fork/apply.sh ../trino
cd ../trino
./mvnw clean install -DskipTests
```

The script checks the exact base and requires a clean checkout before applying.
The build command follows the upstream 446 build workflow; it skips the upstream
test suite. Run the targeted checks below before deploying. Package the resulting
server distribution in your own image and install the Flight connector there.
Mounting the connector into the unmodified `trinodb/trino:446` image does not install
the engine patch. Use the same engine build on every Trino node.

For a single Trino process:

```properties
coordinator=true
node-scheduler.include-coordinator=true
```

Build the connector with the existing project packaging command:

```bash
./mvnw -pl flight-trino -am package -DskipTests
```

## Verify the exact changed engine classes

Run from the repository root:

```bash
python3 flight-trino/trino-fork/prepare-tests.py
./mvnw -pl flight-trino -am -Pintegration,patched-trino test \
  -Dtest='TrinoPartialAggregation*Test' -Dgroups='unit | integration' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The preparation script downloads only changed upstream files at `BASE_SHA`, checks
the patch and applies it inside `flight-trino/target/trino-patch-sources`.
The two profiles compile those engine classes into the test classpath over the stock
446 libraries; they do not build or publish a complete Trino distribution. Re-run
preparation after editing the patch. Use `patched-trino` together with `integration`.

The test starts a real Flight server with Parquet/DuckDB and a real Trino coordinator
with zero separate workers. Two disjoint scan partitions produce real endpoints
with overlapping grouping keys. Assertions compare against disabled pushdown,
check emitted COUNT SQL and preserved FINAL, and cover null keys/inputs, empty
inputs, HAVING, residual filters, unsupported aggregates and task concurrency 1/4.
An additional catalog simulates a Flight plan with zero endpoints to check default
states through the real final aggregator.
This exercises multiple endpoints on one Flight server; it is not a multi-host
Flight cluster benchmark or a full upstream Trino regression run.

Connector unit tests cover contract refusal, output columns, aliases, SQL rendering
and malformed Arrow states. Engine rule unit tests check SINGLE and PARTIAL plans
and reject unmarked state scans. Run ordinary project checks as required by AGENTS.md.
All maintained changes for this experiment live inside `flight-trino`.
