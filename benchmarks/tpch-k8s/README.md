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

## Update: aggregation pushdown over expressions and DECIMAL, and where the time goes

The Flight settings used for the table above (1 DuckDB thread, 1 endpoint per node) were the server defaults and are far
from optimal. Current settings (`deploy/k8s-tpch/30-flight-cluster.yaml`): DuckDB threads 8, `endpointsPerServer=3`, one
shared DuckDB instance per server (4 GB), Java heap 2 GB, direct memory 6 GB. The table above has **not** been re-run.

Pushed vs. unpushed Flight (same cluster, same data, warm, results identical in every case):

| Query | Pushed (s) | Unpushed (s) | Note |
|---|---|---|---|
| Q1 | 4.7 | 25-26 | DECIMAL sums over expressions and averages pushed; direct mode took 30.7 s |
| Q6 | 1.0 | 1.2 | already selective |
| Q11 | 3.2-3.9 | 3.3 | |
| Q15 | 3.3-4.1 | 3.4 | |
| Q17 | 12.5 | 11-12 | |
| Q22 | 1.9 | 1.9-2.1 | |
| Q2 | 2.4-3.7 | 2.1 | pushed is slower: `MIN(ps_supplycost) GROUP BY ps_partkey` has ~20M groups, so every endpoint returns nearly all of them |

Only Q1 gains, because it is the only TPC-H query that aggregates a single table with few groups. The high-cardinality
case (Q2) shows that pushdown should be cost-based; the FE has no row-count or NDV statistics for Flight tables yet.

Stage breakdown of Q1 (client inside the cluster reading all 9 endpoints in parallel and discarding the data):

| Stage | Time | Volume |
|---|---|---|
| `COUNT(*)` (footer statistics) | 0.15 s | |
| DuckDB scan + filter + aggregate (the pushed Q1) | 3.9 s | 36 rows |
| StarRocks end to end, pushed | 4.7 s | |
| Scan of one BIGINT column, no filter | 3.8-6.3 s | 600M rows, 4.8 GB |
| Raw Q1 scan, 7 columns, to a Python client | 57 s | 591M rows, **46 GB** |
| StarRocks end to end, unpushed (scan + convert + aggregate) | 26 s | |

Reading: the server scans lineitem in about 4 s, so what a raw scan pays for is moving 46 GB of Arrow (78 bytes per row:
DECIMAL(15,2) is sent as 128-bit decimals and the one-character flags as full strings). StarRocks consumes it faster than
the Python client (26 s against 57 s, which is client-bound), so a lower bound for the wire rate is about 1.8 GB/s. Next
levers: narrower types on the wire (64-bit decimals, dictionary-encoded strings), IPC compression across nodes, and
cost-based pushdown. Failures found on the way: the BE crashed on a decimal merge aggregate without precision and scale
(fixed, E2E now covers decimals), and the server ran out of direct memory (and of the pod limit) under 8-12 concurrent
streams per node, which is why the memory profile above is smaller on heap and larger on direct memory.

### Compressing the Flight batches: negative result

Between two pods the network carries about 6.5-6.9 Gbit/s (0.85 GB/s) per node pair (iperf3), and an unpushed Q1 moves
46 GB in 26 s, so the wire looked like the limit. Compressing a 4M-row slice of that scan with native pyarrow gave
78 -> 25 bytes per row with LZ4 and 78 -> 13 with ZSTD (narrowing decimals to 64 bits adds nothing once compressed;
dictionary strings help by about 30% on top). Flight for Java has no codec option, so an experiment swapped the stream's
batch writer for one with a codec. Measured on the cluster (unpushed, warm):

| Codec | Q1 | Q3 | Q18 |
|---|---|---|---|
| none | 25.4 s | 9.7 s | 38.5 s |
| zstd | 32.2 s | 10.2 s | second run failed (BE memory) |
| lz4 | hung until the 1800 s timeout | failed (pod restarted) | failed |

A Flight pod was also OOM-killed within seconds of starting Q1 with zstd. The Arrow Java codecs copy every buffer through
Java arrays and native compression buffers that count against neither the heap nor the direct-memory limit, so the server
becomes the bottleneck and the memory hog. The experiment was removed again. Getting the benefit would need a native or lower-level implementation.

## Update: narrow decimals and join key reduction

Settings: `decimalWire=narrow` on the Flight servers (see `deploy/k8s-tpch/30-flight-cluster.yaml`), join key reduction at
its default of 100,000 keys unless noted. All pushed/unpushed pairs below returned identical results.

**Narrow decimals.** `DECIMAL(p, s)` with p up to 18 is sent as the unscaled int64 (p up to 9: int32) instead of a 128-bit
Arrow decimal; the stream schema carries `decimal.precision`/`decimal.scale` metadata and the BE copies the integers
straight into the column (`flight_chunk_converter.cpp`). Raw Q1 scan bytes drop from 78 to about 46 per row. Unpushed Q1
went from about 25-26 s to 22.9 s warm and matches direct mode exactly; the wire is therefore not the only bottleneck.
The option is off by default because other Flight clients do not know the form; the E2E runs a narrow and a plain server
over the same data and compares them through StarRocks.

**Join key reduction.** The planner asks the Flight server for the distinct join keys of a selectively filtered Flight
scan (`SELECT k ... GROUP BY k`, abandoned past `flight.join_key_reduction.max_keys`) and adds `k IN (...)` to the scan on
the other side of an inner or semi equi-join. The join stays, so results cannot change. It costs one extra server query
at planning time (+0.1 to 0.8 s measured with `EXPLAIN`).

| Query | Keys | Rows scanned of the big table | Reduced (warm) | Unreduced (warm) |
|---|---|---|---|---|
| Q17 `lineitem` x `part` | 20 K | 600 M -> 0.6 M | 3.3 s | 9.2 s |
| Q8 `lineitem` x `part` (limit 300 K) | 134 K | 600 M -> 4 M | 7.5 s | 19.3 s |
| Q8 with the default limit | 134 K (over) | unchanged | 20.3 s | 19.6 s |
| Q2, Q16, Q19, Q20, Q14 | no usable filtered side or filter not pushable | unchanged | within about 15% | |

Q8 shows that the default limit is conservative: parsing a 134 K-key IN list takes about 0.4 s on the server (once; queries
over 256 KB are no longer cached) and the plan carries it as text, so a larger `max_keys` is worth setting per catalog.
Queries whose filtered side is a join result (Q3, Q5, Q10) or has millions of keys, or whose filter the server cannot
evaluate entirely (for example `LIKE`, Q2/Q9/Q14/Q19 style), are not reduced.

Why not StarRocks runtime filters: the planner already attaches them to the Flight scan, but a partitioned join builds
one bloom filter per hash partition and a reader must reproduce StarRocks' partition-layout hashing exactly to test them;
a mismatch silently drops rows, so that route was not taken. The plan-time key fetch is exact by construction.

Not done: BE gtests (`run_be_unit_tests`) were never run, the BE change is covered by the CI E2E; reduction through
`LIKE`/`OR` filters, multi-level joins and keys beyond a few hundred thousand would need a different mechanism.

## Update: reading from MinIO, streaming and memory

The Flight pods now read straight from MinIO (`deploy/k8s-tpch/31-flight-cluster-s3.yaml`, `FLIGHT_DATA_DIR=s3a://tpch`):
the Hadoop `s3a` filesystem lists files and reads Parquet footers (no DuckDB), DuckDB `httpfs` scans the data. Settings come
from `FLIGHT_S3_*`. Full 22-query runs, best warm run in seconds:

| Q | Direct | Flight, results materialized | Flight, streamed | Streamed + read-ahead |
|---|---|---|---|---|
| 1 | 31.4 | 4.7 | 5.1 | 4.8 |
| 3 | 34.7 | 19.2 | 30.7 | 12.5 |
| 5 | 26.2 | 41.6 | 32.2 | 20.6 |
| 8 | fails | 48.9 | 38.0 | 29.6 |
| 11 | 2.2 | fails | 4.4 | 4.0 |
| 13 | 11.5 | fails | 10.8 | 7.5 |
| 18 | fails | fails | 23.1 | 22.1 |
| 21 | 41.6 | fails | 36.2 | 35.2 |
| 22 | 2.5 | fails | 3.7 | 7.2 |

**Memory.** The pods went over their 18 GiB limit and restarted. Cause: DuckDB JDBC materializes a whole result before the
first row is returned, so each scan stream held its entire output (a 24M-row scan produced its first batch after 25 s and the
rest 0.65 s later). `jdbc_stream_results=true` makes DuckDB produce batches as the consumer takes them: first batch after
0.7 s, pod peak memory 5.5-8 GiB, and Q11, Q13, Q18, Q21, Q22 complete. Plain streaming was slower on several queries because
the scan could no longer run ahead of the consumer; `streaming_buffer_size = 64MiB` per stream and
`preserve_insertion_order = false` bring most of that back (Q3 30.7 -> 12.5 s). Both are tunable
(`-DduckDbStreamingBuffer`, `-DduckDbPreserveInsertionOrder`, `-DduckDbStreamResults`).

**Compression, again (negative).** A parallel ZSTD compressor (`-DflightCompression=zstd`) is available and tested, off by
default. Unpushed scans, best warm run, seconds:

| | plain | plain + zstd 1 | plain + zstd 3 | narrow | narrow + zstd 1 |
|---|---|---|---|---|---|
| Q1 | 22.3 | 23.0 | 26.4 | **16.6** | 25.3 |
| Q3 | **10.8** | 13.2 | 16.7 | **10.8** | 14.5 |
| Q21 | 35.3 | 35.5 | 40.3 | 35.5 | 48.1 |

At about 200 MB/s per thread the codec costs more than the wire saves, so narrow decimals stay and compression stays off.
(Q18 survives only with compression, and only sometimes: the BE runs out of memory first.)

**Join runtime filters pushed to the server** (BE flags `flight_runtime_in_pushdown`, `flight_runtime_bloom_pushdown`).
Three runs each; IN filters help where the build side has at most 1,024,000 keys: Q8 38 -> 21-27 s, Q17 29 -> 15 s,
Q22 3.5 -> 2.6 s. The bloom filter added nothing measurable on top (Q4, Q7 unchanged over 5 runs).

**Who wins.** Earlier tables compared runs taken at different times, with different cache states, and showed direct ahead on
Q4, Q7 and Q22. Measured together, Q4 (flight 10-15 s, direct 17-21 s), Q7 (15 against 22 s) and Q22 are flight wins. Direct
still wins Q11 (2.4 against 3.8 s) and Q15 (17 against 19-22 s, one revenue branch was not aggregated on the server).
