# Partial Aggregation from Flight to Trino

Branch: `feat/trino-partial-aggregation-pushdown`. Engine base: Trino 446,
pinned in `flight-trino/trino-fork/BASE_SHA`.

## Query Execution

Users write ordinary SQL. The planner patch replaces a supported PARTIAL with
a scan of intermediate states from Flight. For an original SINGLE aggregation,
it retains a FINAL above the state scan. DuckDB on each Flight node computes
its portion, while Trino's existing Exchange and final aggregation combine the
results. A Trino worker does not need to run beside each Flight node.

```sql
SET SESSION flight.partial_aggregation_pushdown = true;

SELECT k, count(*), min(v), max(v), sum(v), avg(v)
FROM flight.test_schema.grouped
GROUP BY k;
```

The property is disabled by default. It does not enable partial pushdown on
stock Trino without the engine patch. Build and verification commands are in
[the patch README](../../flight-trino/trino-fork/README.md).

## Supported Operations

| Operation | Input | Flight output | FINAL input |
| --- | --- | --- | --- |
| COUNT | Integer types, BOOLEAN, VARCHAR, REAL, DOUBLE, DECIMAL(p, s), DATE, TIMESTAMP without time zone, VARBINARY; also COUNT(*) | Non-null value count or row count | BIGINT |
| MIN/MAX | BIGINT, INTEGER, SMALLINT, TINYINT, REAL, DOUBLE, DECIMAL(p, s) | Minimum/maximum with Trino-compatible NaN handling | Nullable value of the input type |
| SUM | Integer types | Exact sum through DECIMAL | Range-checked BIGINT |
| SUM | DOUBLE | Sum | Nullable DOUBLE |
| AVG | Integer types and DOUBLE | SUM and COUNT(v) as separate Arrow columns | ROW(DOUBLE, BIGINT) |

Ordinary GROUP BY supports multiple keys of integer types, BOOLEAN and VARCHAR.
Some implicit Trino coercions introduce a Project between aggregation and scan;
these plans may retain the original execution path even for the listed types.

## Partial Aggregation Contract

`FlightAggregation` stores a list of `AggregateSpec` contracts. Each describes
the logical function, typed input, intermediate output type, remote expressions,
state assembly strategy and empty state.

- `FlightMetadata` checks the request and the intermediate type expected by Trino.
- `FlightQueries` builds SQL for Flight, preserving filters before aggregation.
- `FlightAggregationStates` validates Arrow results and builds native Trino Blocks.
- Trino's existing FINAL combines the states. Its aggregation algorithms are unchanged.

AVG cannot be computed by averaging endpoint averages. Values 10 and 20 on one
endpoint and 30 on another produce states (sum=30, count=2) and (sum=30, count=1).
The final result is 60 / 3 = 20.

Trino 446 serializes AVG as `(sum DOUBLE, count BIGINT)`. Integer inputs are
converted to DOUBLE before summation, matching Trino's native AVG accumulator.
COUNT(v) excludes NULL inputs.

COUNT accepts these scalar input types without casting their values. NaN and
empty binary values count as non-NULL values. Decimal precision/scale and
timestamp precision do not change the BIGINT state contract. TIME, TIMESTAMP
WITH TIME ZONE, CHAR and complex COUNT inputs remain unsupported by partial
pushdown; the supported GROUP BY key types are unchanged.

Binary table columns also require the updated Flight server: its catalog renders
Arrow Binary/LargeBinary/FixedSizeBinary as BLOB instead of passing them through
Calcite's unsupported binary conversion.

MIN/MAX preserve their input type, including decimal precision and scale up to
DECIMAL(38, s). Decimal extrema never pass through DOUBLE. Parquet footer extrema
decode physical decimal statistics to exact scaled values before comparison.

Trino's floating-point MIN and MAX ignore NaN when a non-NaN value exists and
return NaN for all-NaN input. DuckDB MIN has that behavior, but DuckDB MAX chooses
NaN over ordinary numbers. Floating-point MAX therefore sends `MIN(-(v))` to
Flight; the connector negates that one scalar state before native Trino FINAL
merges it. This retains a single remote aggregate, supports infinities and NULL,
and applies to both REAL and DOUBLE. Transformed inputs bypass unmodified Parquet
footer statistics. Trino treats opposite zero signs as equal; with mixed signs,
which sign survives can depend on the reduction order.

Integral SUM generates `SUM(CAST(v AS DECIMAL(19, 0)))`. The server returns
DECIMAL(29, 0) through its existing decimal SUM path. The connector converts
that exact value to BIGINT with a range check. This preserves integers above
2^53 that cannot be represented exactly as DOUBLE. An out-of-range partial
state fails; final merging retains Trino's overflow checks. Changing reduction
order can change when an intermediate overflow occurs.

## Empty Input and Limitations

COUNT returns 0; SUM/MIN/MAX/AVG return NULL. For AVG, the connector supplies
`(0.0, 0)`, from which FINAL produces NULL. If there are no endpoints, a local
synthetic split supplies each required state without opening a Flight stream.
Empty grouped input produces no groups. HAVING executes after FINAL.

REAL and DECIMAL SUM/AVG, string/temporal MIN/MAX, DISTINCT, GROUPING SETS and
aggregate FILTER/ORDER BY retain ordinary Trino execution.
If any requested aggregate is unsupported, the connector declines the entire
aggregation request. DOUBLE reduction order can change low-order result bits;
bitwise reproducibility is not guaranteed.

This extension adds neither JOIN pushdown nor Flight-to-Flight exchange.
All Trino nodes must use the same engine patch and connector build: handle JSON
changed from `counts` to `aggregates`.

## Verification

Unit tests cover types, unsupported contracts, SQL generation, alias collisions,
handle serialization, Arrow validation, empty states and overflow. The
`integration,patched-trino` profile compares pushdown enabled/disabled using
real Flight, Parquet/DuckDB and Trino. Two endpoints have overlapping keys and
unequal AVG input counts. Cases cover NULL, HAVING, empty input, sums above
2^53, partial/final BIGINT SUM overflow and NaN/positive/negative infinity for
DOUBLE SUM/AVG. Extrema cases cover REAL/DOUBLE NaN mixed with finite values,
all-NaN and all-NULL partitions, infinities, signed zero and exact short/long
decimals, including DECIMAL(38, 18). Task concurrency is tested at 1 and 4.

The fixture uses one Flight process and a Trino coordinator that executes
tasks. It does not replace multi-host testing, full-distribution verification
or benchmarks.

Verification on 2026-11-11: compile, Checkstyle and SpotBugs passed; all 755 unit
tests passed across the modules. In WSL, six integration tests and three planner
rule tests passed, and Maven reported BUILD SUCCESS. At shutdown, Surefire
reported a 30-second fork JVM exit timeout; test results remained 0 failures
and 0 errors. This test-process shutdown warning remains a fixture limitation.

Verification on 2026-11-13 after extending numeric MIN/MAX: compile, Checkstyle
and SpotBugs passed; all 762 unit tests passed. In WSL, eight integration tests
and four planner rule tests passed (0 failures/errors, BUILD SUCCESS). Queries
retained native FINAL and matched disabled pushdown, including decimal values
above 2^53 and precision-38 boundaries. The previously observed Surefire fork
shutdown timeout also occurred after this successful run. Logs are in
`target/minmax-checks.log`, `target/minmax-tests.log` and
`target/minmax-integration-wsl.log`.

Verification on 2026-11-14 after extending COUNT inputs: compile, Checkstyle and
SpotBugs passed, as did all 765 unit tests. WSL passed nine integration tests and
four planner rule tests (0 failures/errors, BUILD SUCCESS). New cases cover REAL
NaN, DECIMAL precisions 9/18/19/38, DATE, millisecond/microsecond/nanosecond
TIMESTAMP columns, and VARBINARY including empty values. Global/grouped results,
HAVING, NULL/empty/no-endpoint states and types match disabled pushdown at task
concurrency 1/4. An unpartitioned catalog also exercises Parquet footer counts.
The pre-existing Surefire fork shutdown timeout remains. Logs are in
`target/count-types-checks.log`, `target/count-types-tests.log` and
`target/count-types-integration-wsl.log`.
