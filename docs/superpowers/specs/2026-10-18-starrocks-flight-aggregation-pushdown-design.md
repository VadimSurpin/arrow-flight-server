# StarRocks Flight catalog: aggregation pushdown (design)

Extends [the native flight catalog](2026-10-17-starrocks-native-flight-catalog-design.md).

## Intent

Motivation is performance: let the Flight server compute `GROUP BY` partials so only groups, not rows, cross the
network. A table plans to several Flight endpoints, so the server can only return a **partial aggregate per
endpoint**; StarRocks keeps a final merge aggregate above the scan.

Decided with the user: partial-per-endpoint plus final merge; scope COUNT(*), COUNT(col), SUM, MIN, MAX, AVG with
`GROUP BY` plain columns.

## Planning (FE)

* New transformation rule on `Aggregate(FlightScan)` (a `LogicalJDBCScanOperator` over a `FlightTable`). It fires
  only if every group key is a plain column and every aggregate is COUNT(*), COUNT(col), SUM, MIN, MAX or AVG
  without DISTINCT, over a plain column of a supported type. Otherwise the plan is untouched.
* Rewrite into two phases. The scan carries an aggregation descriptor (keys + partial specs); the original
  aggregate stays above as the merge: COUNT merges as SUM, SUM as SUM, MIN as MIN, MAX as MAX, AVG as
  SUM(sum)/SUM(count).
* Filters and projection push first; the aggregate runs over the filtered scan.
* The scan output schema becomes `(keys..., partials...)`. Partial types: COUNT -> BIGINT; SUM of integral types
  -> BIGINT; SUM of float types -> DOUBLE; MIN/MAX -> the input type, restricted to numeric, date and datetime.
  String MIN/MAX is not pushed (collation may differ).
* Catalog property `flight.aggregation_pushdown` (default `true`) switches the rule off.

## SQL (flight-starrocks)

* `ScanAggregate` model (group keys, list of `(func, column)`), validated like `ScanPredicate` (identifiers only).
* `ScanSql.select(..., ScanAggregate)` emits
  `SELECT k1, CAST(SUM(x) AS BIGINT) AS _a0, COUNT(x) AS _a1 FROM s.t [WHERE ...] GROUP BY k1`.
  Aliases are positional, so output column order is fixed.
* First plan task: verify against the live server that GROUP BY, CAST and aliases pass its SQL path and what Arrow
  types come back. A function that misbehaves is removed from the pushable set.

## Thrift and BE

* `TFlightScanNode` gets an optional `aggregation` description; the FE enables it only for the native catalog.
* The BE reader already converts the Arrow schema it receives against the slot descriptors in strict mode, so it
  needs no logic change; one gtest covers a partial-schema batch.

## Testing

* FE unit tests: rule fires / declines (DISTINCT, expression key, string MIN, unmapped type, flag off), merge
  mapping, AVG split.
* E2E (`starrocks_native_e2e.py`): per shape, compare pushed vs un-pushable variant (or flag off); EXPLAIN shows
  the partials; empty input ungrouped, NULL keys, NULL-only SUM, multi-endpoint table.
* Docs updated; fork patch re-exported; CI rerun.

## Out of scope

COUNT(DISTINCT), HAVING, expressions in keys or aggregates, string MIN/MAX, DECIMAL SUM, grouping sets.
