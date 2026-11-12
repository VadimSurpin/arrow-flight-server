# Trino scan pushdown

For experimental partial COUNT/MIN/MAX/SUM/AVG pushdown with a patched Trino 446
engine, see [the engine patch](../../flight-trino/trino-fork/README.md) and
[the partial aggregation contract guide](trino-partial-aggregation.md). The scan behavior
below describes stock Trino with partial aggregation pushdown disabled.

The connector targets Trino 446 on Java 21 and does not require changes to the engine.

Flight servers read Parquet, select the requested columns and apply supported `WHERE`
predicates. Trino performs joins, aggregation, sorting, limits and remaining filters.

## Predicate contract

`FlightMetadata.applyFilter` separates each requested column domain into:

- an exact server predicate, stored in `FlightTableHandle.constraint`;
- an unsupported predicate, returned to Trino as the remaining filter.

Additional connector expressions remain in Trino. Only complete column domains are
pushed: an unsupported branch of an `OR` prevents pushing that entire column domain.
Repeated calls intersect the accepted domains; a contradiction produces no splits.

Supported value predicates include integer, boolean, decimal and date comparisons,
and valid UTF-8 string equality and `IN` lists. String ranges and `REAL`/`DOUBLE`
comparisons remain in Trino. Floating-point pushdown is deliberately conservative
until literal coercion and non-finite value semantics are covered by a server contract.
Null checks are supported independently of value comparisons. String literals with
invalid UTF-8 or embedded NUL remain in Trino.

## Projection and streaming

`applyProjection` selects the input columns referenced by the requested expressions.
Computed expressions remain in Trino. A scan without input columns, such as `COUNT(*)`,
reads one available column to preserve row counts without transferring every column.

Each planned Flight endpoint becomes one Trino split. Trino schedules splits on its
workers; each page source reads one endpoint lazily, one Arrow batch at a time.
Pages own their converted values, so subsequent batches and stream closure do not
invalidate previously returned pages. End of stream, cancellation and read failures
close the reader. Cleanup failures do not replace the original read failure.

## Validation

Run the module checks and unit tests from the repository root:

```bash
./mvnw -pl flight-trino -am compile checkstyle:check spotbugs:check
./mvnw -pl flight-trino -am test
```

The existing integration suite exercises metadata, Parquet scans, filters,
projections, aggregation and partitioned reads on a real Trino worker:

```bash
./mvnw -P integration -pl flight-trino -am test \
  -Dtest=TrinoFlightIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```
