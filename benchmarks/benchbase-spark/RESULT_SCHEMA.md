# Benchmark result schema

New paired benchmark runs use schema version `1.0.0`, defined by
[`schema/benchmark-result-v1.schema.json`](schema/benchmark-result-v1.schema.json).
The contract covers four artifact types:

- `run.json` records source SHA and dirty state, workload policy, runtime pins,
  topology, the deterministic engine-order schedule, timestamps, and terminal
  status.
- `observations/observation-NNN/<engine>/engine-result.json` retains every
  ordered engine outcome, raw measured transaction, serial repetition index,
  correctness result, plan, failure reason, validity state, and raw artifact
  link.
- `aggregate-summary.json` stores per-observation values, equal-weight
  observation aggregates, and links back to every raw repetition CSV.
- `benchmark-result.json` is the complete paired comparison, with both engine
  outcomes for every observation, dataset manifest, logical query digests,
  per-observation engine order, aggregate summary, and schema validation state.

## Paired measurement policy

A comparison defaults to one paired observation. Set
`BENCHBASE_PAIRED_OBSERVATIONS=3` or higher for a publishable comparison, which
requires at least three complete pairs. The first pair follows
`BENCHBASE_COMPARE_ORDER`, and later pairs alternate automatically, so both
`flight → direct` and `direct → flight` occur in one publishable invocation.

The supported cache policy is `warm-cache`, recorded through
`BENCHBASE_CACHE_POLICY`. The stack is prepared once and is not reset or
evicted between engine runs or observations. A timed workload still applies
`BENCHBASE_WARMUP_SECONDS` independently to every engine run. This combination
uses explicit warm-cache measurements while alternating order to distribute
JVM, Spark code generation, HDFS, and OS cache effects.

Each observation records its start and finish timestamps, warmup, cache policy,
engine order, engine exit codes, and failures in `observation-context.json`.
A failed engine does not stop the remaining engine or later pairs. Failed and
partial observations remain in the final artifact, while publication requires
three valid complete pairs and both engine orders.

Latency aggregates use each successful observation's median as one equally
weighted input. Reports include every observation and the median, minimum,
maximum, spread, p25, p75, IQR, and p95 across observation values. Paired
Flight/Direct ratios and differences are calculated only for complete pairs.
Per-query aggregates follow the same observation-level method.

Runtime execution paths use only these stable labels: `footer-count`,
`footer-stats`, `duckdb-scan`, `duckdb-aggregation`, `duckdb-join`,
`distributed`, `mixed`, `fallback`, and `unknown`. A uniform path spanning
multiple nodes is classified as `distributed` and keeps its concrete
`uniform_path`; differing node paths are `mixed`. `fallback` and `unknown`
never set `pushdown_evidence` to true.

The harness captures `run-context.json` before execution, preserves every raw
observation, and validates all four artifact types before updating the
publishable dashboard. Validate an artifact manually with:

```bash
python benchmarks/benchbase-spark/benchmark-result-schema.py validate \
  benchmarks/benchbase-spark/results/<run>/benchmark-result.json
```

Breaking contract changes require a new schema file and a new
`schema_version`; existing version `1.0.0` artifacts remain immutable.
