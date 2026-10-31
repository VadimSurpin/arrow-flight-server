# Benchmark suites

One framework for TPC-H, TPC-DS, ClickBench, NYC taxi and the synthetic family: the same bootstrap tool, the same query layout, the same harness and the same result format. Written for the learning set in `docs/roadmap/optimizer-classifier-data-plan.md`.

```
benchmarks/suites/
  suites.py            registry: default schema, Spark settings per suite, query lookup
  bootstrap.py         list suites, regenerate queries, build/run the data generation (local or as a cluster Job)
  harness.py           run / check / summary / validate, one JSONL record per run
  configs/             configuration sets (default; phase1 = one knob at a time)
  <suite>/queries/     qNN.sql (tpch 22, tpcds 99, clickbench 43, taxi 6, synthetic 8 named queries)
  tests/               harness tests against a stub driver (python3 -m unittest discover -s tests)
../datasets/           the generators (gen.py, sweep.py, fetch.py) that bootstrap.py calls
```

## Data layout

Everything sits in the `learn` bucket, one directory per schema and one per table: `s3://learn/<schema>/<table>/part-*.parquet`. The Flight server resolves `schema.table` to `<data dir>/<schema>/<table>`, so the same names serve the direct Spark read and the Flight read.

| Suite | Schemas |
|---|---|
| tpch | `tpch_sf100_asis` |
| tpcds | `tpcds_sf100_{asis,sorted,shuffled}` |
| ssb (data only, no queries yet) | `ssb_sf100_{asis,shuffled}` |
| clickbench | `clickbench` (table `hits`) |
| taxi | `taxi` (table `yellow`, 2015-2024) |
| synthetic | `syn_<axes>` (tables `fact`, `dim`) |

## Workflow

```
python3 bootstrap.py list
python3 bootstrap.py data --suite tpcds --sf 100 --layout sorted --kubectl KUBECTL    # data generation Job
python3 harness.py validate --suite tpcds --schema tpcds_sf100_asis                    # EXPLAIN every query first
python3 harness.py run --suite tpcds --schema tpcds_sf100_sorted --configs configs/phase1.json --reps 2 \
    --kubectl KUBECTL --out runs/tpcds-sorted.jsonl
python3 harness.py check runs/tpcds-sorted.jsonl      # result mismatches, failures, invalid runs
python3 harness.py summary runs/tpcds-sorted.jsonl
```

`run` needs the Spark driver reachable at `--url` (default `http://127.0.0.1:18000`, a port-forward to `svc/spark-driver:8000`). `--kubectl` adds the cluster state to each record and the validity check.

## Record format (JSONL)

`type: "features"` once per (suite, schema, query): operator counts from `EXPLAIN FORMATTED` (scans, joins by type, aggregates, sorts, exchanges...) and a plan hash.

`type: "run"` per execution: `suite, schema, query, config, mode (direct|flight), rep, warmup, wall, error, row_count, result_hash, rows (only when 50 or fewer), valid, invalid_reason, conf (the Spark settings applied), ts, cluster{nodes_ready, nodes, images, restarts_total}`.

- Order within a query is shuffled (seeded). One discarded warm-up per query and mode runs with the default configuration.
- A configuration is only run in a mode where its settings differ from the default's, so Direct is not repeated for knobs that only touch Flight.
- `check` compares every run to the default Direct run of the same query: row count, then result hash, then the stored rows with a 1e-6 relative tolerance. A difference is reported as a bug, not as a label.
- `valid` is false when a pod restarted or a node was NotReady during the run, or the error mentions memory or a lost connection. Drop those rows before training.

## Deployment prerequisites (not done on the cluster yet)

- The Spark driver image has to contain the new `spark/spark_server.py` (`POST /run`, tables registered per schema from the S3 listing). Rebuild with the `bench-images.yml` workflow and restart the driver. The old `GET /run?q=N` still works.
- The Flight cluster must read the `learn` bucket: `FLIGHT_DATA_DIR=s3a://learn`. That restarts the Flight pods, and it replaces the old `tpch` bucket layout (`sf100.lineitem`) with `tpch_sf100_asis.lineitem`.
- TPC-H lives under `tpch_sf100_asis` in `learn` (copied from the `tpch` bucket).

## Known gaps

- TPC-DS queries use the default parameters of DuckDB's `tpcds_queries()`, not several dsqgen seeds. `validate` finds the ones Spark's parser rejects; none have been run on Spark yet.
- ClickBench needs `spark.sql.parquet.binaryAsString`; whether the Flight server returns those columns as text is untested.
- Taxi files differ in column types between years; the direct read uses `mergeSchema`, the Flight server's behaviour is untested.
- Synthetic: the two filtered queries are fixed at 1% and 50% selectivity; the rest of the selectivity axis is not covered. The `runtimeBloomMinKeys` "off" value in `phase1.json` is an assumption (a key count nothing reaches).
- SSB has data but no queries: the tables are derived from TPC-H and lack the SSB columns `p_category` and `p_brand1`.
- StarRocks is not driven by this harness yet.
