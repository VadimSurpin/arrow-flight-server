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

The output tree is `<out>/<suite>/sf<N>/<layout>/<table>/part-NNN.parquet` (synthetic: `<out>/synthetic/<axis values>/{fact,dim}/`), plus a `manifest.json` (a one-row `manifest.parquet` on S3) with row and file counts and the axis values. The generator builds the whole dataset in a local DuckDB file first, so the scratch directory (`--spill`) needs a few times the dataset size; set `--memory` to the pod's limit.

Not covered yet: ClickBench, JOB/IMDB and NYC taxi are downloads rather than generated data, and the TPC-DS and synthetic queries still need Spark-dialect SQL files under `benchmarks/tpch-k8s/queries/`.
