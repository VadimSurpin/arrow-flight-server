# Trino 446 engine patch

`BASE_SHA` pins the upstream source used by the connector and runtime image.
The current build, launch, compatibility matrix and checks are in the
[connector README](../flight-trino/README.md).

The patch adds a default-empty `ConnectorMetadata.applyPartialAggregation` hook,
forwards it through engine metadata and tracing, and inserts a rule that replaces
supported PARTIAL/SINGLE scans with native intermediate states. FINAL and
INTERMEDIATE aggregation remain native Trino operations. Empty global inputs retain
their native defaults. The table handle marks state-producing scans for plan validation.

The restored optional CASE/COALESCE bridge has a default-false table capability and
can round-trip conditional expressions when a connector declines them. Flight leaves
this capability disabled against the unchanged main server. Decimal AVG's native
scale/overflow carry correction remains in the engine patch, including for native
fallback. The patch does not introduce Flight-to-Flight exchange or require a Trino
worker on a Flight node.

## Prepare and test

```sh
bash flight-trino/scripts/build.sh integration
```

`prepare-tests.py` fetches only the changed pinned source files, checks the patch,
and applies it under `flight-trino/target/trino-patch-sources`. The integration and
patched-trino profiles compile those actual classes over the stock 446 test libraries.
Tests use the unchanged Flight server with Parquet/DuckDB and compare enabled and
disabled pushdown, including masks, conditionals, exact decimal fallback, six endpoints,
stream failure, nulls, empty input, grouping, NaN and overflow. Unit tests also cover
conditional round-trips and the runtime jar overlay.

## Full source checkout

For engine development, the unified patch can also be applied to a clean source checkout:

```sh
git clone https://github.com/trinodb/trino.git ../trino
git -C ../trino checkout -b flight-partial-aggregation "$(cat trino-fork/BASE_SHA)"
bash trino-fork/apply.sh ../trino
```

`apply.sh` requires the exact pinned commit and a clean checkout. The normal Docker
build instead compiles those same changed classes and overlays only their class entries
into the official 446 runtime jars; it preserves the rest of the distribution.
