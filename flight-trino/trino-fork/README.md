# Trino 446 partial aggregation pushdown

This patch lets the Flight connector produce native Trino intermediate aggregate states.
Trino's existing final aggregation combines those states. DuckDB computes each
endpoint's partial aggregates; the final aggregator runs in Trino, including on a
coordinator configured to execute tasks. No Trino worker is needed on a Flight node.

## Scope

The connector supports the following partial aggregation contracts:

| Function | Input types | Remote computation | Native intermediate state | Empty state |
| --- | --- | --- | --- | --- |
| `COUNT(*)`, `COUNT(column)` | BIGINT, INTEGER, SMALLINT, TINYINT, BOOLEAN, VARCHAR, DOUBLE | COUNT | Non-null, nonnegative BIGINT | 0 |
| `MIN`, `MAX` | BIGINT, INTEGER, SMALLINT, TINYINT | MIN or MAX | Nullable input value | NULL |
| `SUM` | BIGINT, INTEGER, SMALLINT, TINYINT | SUM over an explicit DECIMAL(19, 0) input | Checked nullable BIGINT | NULL |
| `SUM` | DOUBLE | SUM | Nullable DOUBLE | NULL |
| `AVG` | BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE | SUM plus COUNT of non-null input values | ROW(DOUBLE sum, BIGINT count) | ROW(0.0, 0) |

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

An AVG request becomes two physical Arrow columns, SUM and COUNT. For integral
inputs, values are converted to DOUBLE before summation, matching Trino's AVG
accumulator arithmetic. Trino 446 serializes `LongAndDoubleState` in alphabetical
field order: Double, then Long. The reader therefore constructs ROW(sum, count),
not ROW(count, sum), and converts NULL sums with zero counts to 0.0. FINAL performs
the division. Endpoints returning (30, 2) and (30, 1) consequently produce 20,
not the average of the two endpoint averages. Counts exclude NULL inputs.

Integral SUM never passes through DOUBLE. The server's existing decimal SUM path
widens DECIMAL(19, 0) inputs to DECIMAL(29, 0); the connector converts those exact
Arrow decimals to BIGINT with a range check. An out-of-range partial fails, and
Trino retains its own overflow checks while merging partial states. As with other
distributed sums, changing reduction order can change which intermediate
overflow is encountered. Floating-point SUM/AVG can differ in low-order bits
because DuckDB and Trino use different reduction orders; bitwise reproducibility
is not guaranteed.

DOUBLE MIN/MAX remain in Trino because its extrema operators treat unordered
NaN values differently from DuckDB MAX. REAL, DECIMAL inputs, temporal/string
extrema and additional state formats are not enabled by this extension. Decimal
used internally for integral SUM does not enable general DECIMAL aggregation.
JOIN, DISTINCT, grouping sets and aggregate FILTER/ORDER BY are not added here.
The existing engine rule already handles these intermediate types; this extension
changes the connector, not Trino's aggregation algorithms or the engine patch.

Handle JSON now uses `aggregates` instead of the former `counts` field. Deploy the
same connector build on every Trino node; mixed old/new handle serialization is
not supported. Further details are available in
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
partial/final BIGINT overflow and non-finite DOUBLE SUM/AVG inputs.
An additional catalog simulates a Flight plan with zero endpoints to check default
states through the real final aggregator.
This exercises multiple endpoints on one Flight server; it is not a multi-host
Flight cluster benchmark or a full upstream Trino regression run.

Connector unit tests cover contract refusal, output columns, aliases, SQL rendering
and malformed Arrow states. Engine rule unit tests check SINGLE and PARTIAL plans
and reject unmarked state scans. Run ordinary project checks as required by AGENTS.md.
Connector and engine sources for this experiment live inside `flight-trino`.
