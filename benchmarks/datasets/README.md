# Learning-set datasets

Generators for the datasets in `docs/roadmap/optimizer-classifier-data-plan.md`. Everything is Parquet, zstd, written to a local directory or `s3://` (MinIO credentials from `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT`).

```
python benchmarks/datasets/gen.py --suite tpcds --sf 100 --layout sorted --out s3://learn
python benchmarks/datasets/sweep.py --phase 2 --out s3://learn          # prints the synthetic grid
```

| Suite | Source | Notes |
|---|---|---|
| `tpch`, `tpcds` | DuckDB `tpch` / `tpcds` extensions | the extensions are downloaded at first use, so the generating host needs network access |
| `ssb` | derived from TPC-H data | same table shapes and row counts as SSB, but not its exact value domains |
| `synthetic` | SQL in `gen.py` | axes: key cardinality, skew, clustering across files, file count, null fraction, key type |

Layouts (tables over 1M rows): `asis` generator order, `sorted` by the table's main join key (`SORT_KEYS`), `shuffled` random. TPC-H data comes out sorted by `orderkey`, so for `lineitem` and `orders` `asis` and `sorted` are the same; the layouts differ for TPC-DS and SSB.

The output tree is `<out>/<suite>_sf<N>_<layout>/<table>/part-NNN.parquet` (synthetic: `<out>/syn_<axis values>/{fact,dim}/`; one directory level per schema, as the Flight server resolves `schema.table` to `<data dir>/<schema>/<table>`), plus a `manifest.json` (a one-row `manifest.parquet` on S3) with row and file counts and the axis values. The generator builds the whole dataset in a local DuckDB file first, so the scratch directory (`--spill`) needs a few times the dataset size; set `--memory` to the pod's limit.

Public datasets are copied unchanged by `fetch.py` (ClickBench `hits`, 100 files, 14.7 GB; NYC yellow taxi 2015-2024, 120 files, 11 GB; job `deploy/k8s-tpch/22-fetch-datasets.yaml`). Not covered yet: JOB/IMDB, and the TPC-DS and synthetic queries still need Spark-dialect SQL files under `benchmarks/tpch-k8s/queries/`.

## Layout features

`layout.py` computes layout features from Parquet footers only (no data is read): per column the share of file pairs whose min/max ranges intersect (`file_overlap`), the median file range width (`range_frac`) and the same overlap over row groups inside a file (`rg_overlap`), and per table the best sort key and `sortedness` (1 - lowest `file_overlap`). Output is JSONL, one record per table (`type: "layout"`), keyed by schema and table so the classifier can join it with harness runs. Run `python -m unittest test_layout` here (needs pyarrow); on the cluster use `deploy/k8s-tpch/23-layout-features.yaml`. Results for the TPC-DS and TPC-H schemas: `benchmarks/suites/runs/layout.jsonl`.
