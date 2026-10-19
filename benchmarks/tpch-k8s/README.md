# TPC-H SF100 on Kubernetes: StarRocks reading Parquet directly vs through the Flight catalog

Setup (namespace `tpch-bench`, manifests in `deploy/k8s-tpch/`):

* MinIO holds the SF100 Parquet files (23 GB, 20 files per table, generated with DuckDB `dbgen`).
* **Direct**: StarRocks (1 FE, 3 BE) reads the Parquet from MinIO through `FILES()` views (database `direct`).
* **Flight**: the same StarRocks reads the same tables through the `flight` catalog from a 3-node Flight SQL cluster.
  Each Flight node has a full local copy of the files on its own disk (no S3 inside the Flight server).
* Both modes run the 22 standard TPC-H queries (`queries/`), 3 runs each. Cold = first run, warm = best of the rest.
  Results are compared across modes (`run.py`); the table is produced by `report.py results`.

## Results

| Query | Direct cold / warm (s) | Flight cold / warm (s) | Flight vs direct (warm) | Results match | Config (direct/flight) |
|---|---|---|---|---|---|
| Q1 | 31.5 / 30.7 | 69.7 / 42.4 | 1.38x | yes | default/default |
| Q2 | 8.6 / 3.1 | 12.5 / 4.8 | 1.52x | yes | default/default |
| Q3 | 32.2 / 28.8 | 38.9 / 18.2 | 0.63x | yes | default/default |
| Q4 | 28.7 / 27.2 | 54.4 / 14.4 | 0.53x | yes | default/default |
| Q5 | 57.8 / 27.7 | 98.3 / 56.4 | 2.04x | yes | spill/spill |
| Q6 | 18.3 / 15.7 | 18.6 / 3.7 | 0.24x | yes | default/default |
| Q7 | 30.1 / 28.2 | 29.7 / 19.4 | 0.69x | yes | default/default |
| Q8 | OUT OF MEMORY | 58.7 / 34.6 | - | yes | -/default |
| Q9 | OUT OF MEMORY | 68.7 / 56.9 | - | n/a (no direct result) | -/spill |
| Q10 | 36.3 / 32.8 | 42.5 / 12.5 | 0.38x | yes | default/default |
| Q11 | 8.6 / 2.3 | 13.1 / 3.1 | 1.32x | yes | default/default |
| Q12 | 32.4 / 30.7 | 35.4 / 6.0 | 0.20x | yes | default/default |
| Q13 | 12.9 / 11.4 | 23.9 / 15.4 | 1.34x | yes | default/default |
| Q14 | 20.2 / 19.7 | 39.5 / 4.9 | 0.25x | yes | default/default |
| Q15 | 18.7 / 18.4 | 35.3 / 5.3 | 0.29x | yes | default/default |
| Q16 | 6.7 / 2.6 | 4.9 / 3.1 | 1.22x | yes | default/default |
| Q17 | 33.6 / 31.4 | 19.2 / 17.1 | 0.54x | yes | default/default |
| Q18 | 58.8 / 49.1 | 78.5 / 60.0 | 1.22x | yes | spill/spill |
| Q19 | 31.6 / 32.1 | 35.1 / 7.2 | 0.22x | yes | default/default |
| Q20 | 21.4 / 19.9 | 15.3 / 8.2 | 0.41x | yes | default/default |
| Q21 | 92.4 / 88.1 | 104.1 / 95.9 | 1.09x | yes | spill/spill |
| Q22 | 9.8 / 5.1 | 8.4 / 5.8 | 1.15x | yes | spill/spill |

## How to read these numbers

* **Warm runs favour Flight on scan-heavy queries.** The Flight nodes read from local disk and the OS page cache;
  direct mode re-reads every byte from MinIO over the network on every run (no StarRocks data cache is configured).
  The cold column is the fairer comparison, and there Flight is equal or slower on most queries.
* **Join-heavy queries are not faster through Flight** (Q5 about 2x slower, Q18 and Q21 slightly slower): StarRocks does
  the joins in both modes, and Flight only replaces the scan.
* **Q8 and Q9 do not run in direct mode** on this cluster (out of memory even at an 18 GB BE limit with spilling).
  Direct Q8 succeeded once in the first run (856 s) before running out of memory on the next run. Flight runs both.
  Q9 therefore has no cross-check against direct.
* **Configurations differ between rows.** The base run used a 12 GB BE memory limit and Flight with a 6 GB heap and no
  DuckDB cap. Q5, Q18, Q21 and Q22 were re-run with `enable_spill=true`; Q8 and Q9 with an 18 GB BE limit and Flight
  capped at 4 GB heap, 3 GB direct memory and 6 GB DuckDB. Q21 killed a Flight pod (OOM) under the uncapped settings.
* One sample per cell (3 runs, a single cluster, no repeated trials); direct Q19 had a 148 s outlier in one run.

## Findings that need follow-up

* **Planning bug fixed along the way**: a Flight scan was planned as a single stream, so aggregates over a multi-endpoint
  table returned one row per endpoint. Fixed in the fork (`LogicalProperty` and `OutputPropertyDeriver`); the E2E suite
  still only has a single-endpoint server, so a multi-endpoint E2E check is missing.
* **Stale pod IPs**: after the Flight pods were restarted, the FE kept connecting to the old pod IP until the FE itself
  was restarted.
* **Aggregation pushdown** does not cover DECIMAL sums, expressions or joins, so most TPC-H aggregates still run in
  StarRocks.
