# Trino 446 partial aggregation pushdown

This patch lets the Flight connector produce native Trino intermediate aggregate states.
Trino's existing final aggregation combines those states. DuckDB computes each
endpoint's partial aggregates; the final aggregator runs in Trino, including on a
coordinator configured to execute tasks. No Trino worker is needed on a Flight node.

## Scope

The connector supports the following partial aggregation contracts:

| Function | Input types | Remote computation | Native intermediate state | Empty state |
| --- | --- | --- | --- | --- |
| `COUNT(*)`, `COUNT(column)` | BIGINT, INTEGER, SMALLINT, TINYINT, BOOLEAN, VARCHAR, REAL, DOUBLE, DECIMAL(p, s), DATE, TIMESTAMP without time zone, VARBINARY | COUNT | Non-null, nonnegative BIGINT | 0 |
| `MIN`, `MAX` | BIGINT, INTEGER, SMALLINT, TINYINT, REAL, DOUBLE, DECIMAL(p, s) | MIN or MAX; floating MAX uses MIN(-v) followed by sign restoration | Nullable input value with original precision/scale | NULL |
| `SUM` | BIGINT, INTEGER, SMALLINT, TINYINT | SUM over an explicit DECIMAL(19, 0) input | Checked nullable BIGINT | NULL |
| `SUM` | DOUBLE | SUM | Nullable DOUBLE | NULL |
| `SUM` | REAL | SUM after DOUBLE conversion | Nullable DOUBLE, REAL final result | NULL |
| `SUM` | DECIMAL(p,s) | FLIGHT_EXACT_SUM over a wide unscaled integer accumulator | Native VARBINARY sum state | NULL |
| `AVG` | BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE | SUM plus COUNT of non-null input values | ROW(DOUBLE sum, BIGINT count) | ROW(0.0, 0) |
| `AVG` | REAL | DOUBLE SUM plus COUNT | ROW(BIGINT count, DOUBLE sum), REAL final result | ROW(0, 0.0) |
| `AVG` | DECIMAL(p,s) | FLIGHT_EXACT_SUM plus COUNT | Native VARBINARY average state | NULL |

One ordinary grouping set, including multiple keys, is supported. Grouping keys
support BIGINT, INTEGER, SMALLINT, TINYINT, BOOLEAN and VARCHAR. Trino-inserted
coercions or projections can prevent a direct aggregation-over-scan match, even
when the underlying scalar input type is supported. Unsupported aggregates, types, DISTINCT, aggregate
FILTER/ORDER BY, masks, expressions and residual row filters retain Trino's original
execution path. A request containing an unsupported aggregate is declined as a whole.

The connector defaults to disabled partial pushdown. Enable it per query session:

```sql
SET SESSION flight.partial_aggregation_pushdown = true;
SELECT k, count(*), min(v), max(v), sum(v), avg(v)
FROM flight.test_schema.grouped GROUP BY k;
```

Replace `flight` with your catalog name. Disable the property for the baseline.
The property has no effect on stock Trino: the patched planner must call the new SPI.

Each source partition must cover a disjoint part of the input. Explicit partition
predicates retain their existing requirement to cover all rows without overlap.
The reader checks Arrow state types, count validity and AVG sum/count consistency.
COUNT returns zero for an empty global input; MIN, MAX, SUM and AVG return NULL.
Empty grouped input produces no groups. HAVING stays above final aggregation.
If planning returns no endpoints, the connector supplies one local split with
each aggregate's empty native state. That split opens no Flight stream.

## Partial aggregation contract

`FlightAggregation` contains grouping columns and a list of `AggregateSpec`
contracts. Each specification records the logical function, the typed input and
the native output state column. It derives the remote aggregate expressions,
state assembly strategy and empty state. `FlightQueries` renders those expressions;
`FlightAggregationStates` validates Arrow results and builds native Trino blocks.
The expected intermediate type supplied by the engine must match exactly, or the
connector declines the entire request.

An integer/DOUBLE AVG request becomes two physical Arrow columns, SUM and COUNT. For integral
inputs, values are converted to DOUBLE before summation, matching Trino's AVG
accumulator arithmetic. Trino 446 serializes `LongAndDoubleState` in alphabetical
field order: Double, then Long. The reader therefore constructs ROW(sum, count),
not ROW(count, sum), and converts NULL sums with zero counts to 0.0. FINAL performs
the division. Endpoints returning (30, 2) and (30, 1) consequently produce 20,
not the average of the two endpoint averages. Counts exclude NULL inputs.

COUNT also accepts REAL, DECIMAL, DATE, TIMESTAMP without time zone and
VARBINARY columns. It counts NaN and empty binary values, preserves NULL
exclusion and always returns a BIGINT state. No value casts or new state
serializers are needed. TIME, TIMESTAMP WITH TIME ZONE, CHAR and complex COUNT
inputs retain ordinary execution; GROUP BY key support is unchanged.

Use the updated Flight server for binary columns as well: its catalog maps Arrow
binary types to BLOB, avoiding an unsupported Calcite conversion during startup.

Integral SUM never passes through DOUBLE. The server's existing decimal SUM path
widens DECIMAL(19, 0) inputs to DECIMAL(29, 0); the connector converts those exact
Arrow decimals to BIGINT with a range check. An out-of-range partial fails, and
Trino retains its own overflow checks while merging partial states. As with other
distributed sums, changing reduction order can change which intermediate
overflow is encountered. Floating-point SUM/AVG can differ in low-order bits
because DuckDB and Trino use different reduction orders; bitwise reproducibility
is not guaranteed.

Floating MIN/MAX ignore NaN when any non-NaN input exists, matching Trino 446;
all-NaN input returns NaN. DuckDB MAX does not have this behavior, so the connector
requests one `MIN(-(v))` state and restores its sign before native FINAL merging.
The server preserves the REAL/DOUBLE output type and bypasses footer statistics
for transformed inputs. NULL and infinities retain their native behavior. Opposite
zero signs compare equal in Trino; mixed-zero reduction can retain either sign.
Decimal MIN/MAX preserve precision and scale up to DECIMAL(38, s), including exact
decoding of physical Parquet footer statistics. They never pass through DOUBLE.
REAL/DECIMAL SUM and AVG use separate native contracts; temporal/string extrema
and interval aggregates retain ordinary Trino execution. See the
[type and state matrix](../../docs/integrations/trino-type-contracts.md) for exact
decimal transport, read limits and explicit unsupported-column errors.
JOIN, DISTINCT, grouping sets and aggregate FILTER/ORDER BY are not added here.
The existing engine rule already handles these intermediate types. The patch
also corrects a stock Trino 446 DECIMAL AVG bug: its overflow carry must have the
same decimal scale as the wrapped sum. Native FINAL and state formats are retained;
ordinary and pushed AVG both receive the correction. Rebuild the patched engine
on every Trino node when deploying these decimal contracts.

Handle JSON now uses `aggregates` instead of the former `counts` field. Deploy the
same connector build on every Trino node; mixed old/new handle serialization is
not supported. Deploy the updated Flight server as well: floating MAX requires
its negated-input schema handling, and decimal footer extrema require its exact
statistics decoding. Decimal SUM/AVG require the new internal FLIGHT_EXACT_SUM
operation and BIGNUM accumulation on that server. Further details are available in
[the integration guide](../../docs/integrations/trino-partial-aggregation.md).

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

## Run on the existing TPC-H Compose stack

Keep using the existing benchmark preparation script. It reads `numServers` from
`flight-server/src/main/resources/arrowflight.properties` (currently six), resets
the benchmark volumes and prepares the shared TPC-H dataset:

```bash
BENCHMARK_SCALE_FACTOR=0.1 bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch prepare all
docker compose --profile trino up -d --build trino
docker compose logs -f trino
```

The `trino` service builds the pinned engine patch and connector using the
Dockerfile in this directory. The first build includes the full Trino reactor
with tests skipped; subsequent builds can reuse Docker layers. This image build
and the full multi-node Compose workflow have not yet been validated end to end.
The coordinator also executes tasks and is published at `http://localhost:18080`.
No additional launcher script or Compose override is required.

After `SERVER STARTED`, leave the logs with Ctrl+C and open the interactive CLI:

```bash
docker compose exec trino trino --catalog flight --schema tpch \
  --session flight.partial_aggregation_pushdown=true
```

```sql
SELECT l_returnflag, count(*), min(l_orderkey), max(l_orderkey), avg(l_orderkey)
FROM lineitem
GROUP BY l_returnflag
ORDER BY l_returnflag;
```

This example uses an integer input. Plain-column TPC-H DECIMAL SUM/AVG/MIN/MAX
are also eligible. Use EXPLAIN to check for native Flight state columns
and a final aggregation. An existing running benchmark stack can be reused by
skipping the preparation command.

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
check emitted aggregate SQL and preserved FINAL, and cover null keys/inputs, empty
inputs, HAVING, residual filters, unsupported aggregates and task concurrency 1/4.
Numeric cases include unequal endpoint AVG counts, integer sums beyond 2^53,
partial/final BIGINT overflow and non-finite DOUBLE SUM/AVG inputs. MIN/MAX cases
include REAL/DOUBLE NaN, infinities, signed zero, empty/all-NULL input and exact
DECIMAL values across short/long representations through precision 38.
An additional catalog simulates a Flight plan with zero endpoints to check default
states through the real final aggregator.
This exercises multiple endpoints on one Flight server; it is not a multi-host
Flight cluster benchmark or a full upstream Trino regression run.

Connector unit tests cover contract refusal, output columns, aliases, SQL rendering
and malformed Arrow states. Engine rule unit tests check SINGLE and PARTIAL plans
and reject unmarked state scans. Run ordinary project checks as required by AGENTS.md.
Connector and engine sources live inside `flight-trino`; server-side expression
handling and Parquet decimal statistics decoding live inside `flight-server`.

Verified on 2026-11-13: compile, Checkstyle and SpotBugs passed; 762 unit tests
passed. In WSL, eight integration tests and four planner rule tests passed with
0 failures/errors and BUILD SUCCESS. The existing fixture still reports a
Surefire fork shutdown timeout after the successful tests. This does not verify
the full Docker image or a multi-host deployment.

Verified on 2026-11-14 after extending COUNT: compile, Checkstyle and SpotBugs
passed; 765 unit tests passed. WSL passed nine integration tests and four planner
rule tests with 0 failures/errors and BUILD SUCCESS. Coverage includes the new
scalar inputs, NaN/NULL, empty binary values, timestamp precisions, grouped/global
and empty states, and an unpartitioned Parquet footer-count path. The existing
Surefire fork shutdown timeout remains; the full Docker image and multi-host
deployment have not been validated.

Verified on 2026-11-14 for aggregate completion stages 1–4: compile, Checkstyle
and SpotBugs passed; 777 unit tests passed. WSL passed 12 integration tests, four
planner rule tests and two native decimal state tests (18 total, zero
failures/errors, BUILD SUCCESS). Cases include REAL SUM/AVG, exact decimal sums
and averages, overflowing partial cancellation, rounding, every valid decimal
type contract, serializer byte compatibility and AVG overflow-scale correction
for both signs at every scale 0..38. Temporal/nested reads and mixed-query
fallback are checked. Logs use the `target/numeric-completion-` prefix. The
final run includes REAL non-finite/zero values and exact UINT64-to-DECIMAL(20,0)
aggregates at the unsigned maximum. The separate REAL edge-case rerun also passed.
The existing Surefire shutdown timeout remains. Full-distribution/Docker builds,
multi-node runs and benchmarks were explicitly excluded from this scope.
