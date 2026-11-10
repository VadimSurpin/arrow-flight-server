# Cache-off re-collection: Flight vs Direct and the knob sweep (TPC-H, SSB, TPC-DS at SF100)

The Flight pods ran with the DuckDB external file cache and the parquet metadata cache switched off (`-DduckDbExternalFileCache=false -DduckDbObjectCache=false`, server commit 325a26b), and the pods were restarted before every chunk of queries. Reason: comparing Flight on cached data with Direct access to one MinIO is not a fair comparison. Default runs use 3 repetitions in both modes after 1 warmup run; phase-1 (knob) runs use 2 repetitions, Flight only, 13 configurations, 4 warmups on TPC-H and 10 on SSB and TPC-DS. All numbers are medians of valid measured runs. Tables: `analysis-nocache-stageA-*.md`, `analysis-nocache-stageB-*.md`; raw data `fresh-nocache/*.jsonl`.

## 1. Flight vs Direct (default configuration)

Flight/Direct total time over the queries that succeeded in both modes (below 1: Flight faster):

| suite | asis | sorted | shuffled |
|---|---|---|---|
| TPC-H | 0.28 (19 queries) | 0.63 (21) | 0.66 (20) |
| SSB | 1.06 (12) | 1.05 (12) | 0.74 (11) |
| TPC-DS | 0.49 (29) | 0.43 (30) | 0.54 (29) |

- With the cache off Flight is still faster in total on TPC-H and TPC-DS and level on SSB. The first-pass cache-on figures were TPC-H 0.31 / 0.73 / 0.87, so the cache-on advantage was partly the cache (sorted and shuffled most).
- TPC-H asis: Direct times out (300 s) on q17, q18 and q21; the 0.28 excludes them and flatters Flight. Flight loses mostly on q13, q14 and q03.
- SSB asis: Flight loses on q12 (4.0x), q13 (1.5x) and q11 (1.2x); the first two are the broadcast-join threshold (q12) and a query no knob fixes (q13).

## 2. Do the knobs matter?

Queries where some configuration beats default by more than 10%:

| suite | asis | sorted | shuffled |
|---|---|---|---|
| TPC-H | 6 of 20 | 10 of 20 | 6 of 19 |
| SSB | 9 of 12 | 6 of 12 | 5 of 11 |
| TPC-DS | 17 of 28 | 16 of 29 | 13 of 28 |

Total wall time, seconds (default / best config per query / best single configuration):

| suite | layout | default | per-query best | best single (config) |
|---|---|---|---|---|
| TPC-H | asis | 422 | 383 | 404 (shuffle_48) |
| TPC-H | sorted | 308 | 264 | 287 (shuffle_48) |
| TPC-H | shuffled | 375 | 345 | 367 (complete_agg_off) |
| SSB | asis | 167 | 134 | 142 (bcast_256m) |
| SSB | sorted | 164 | 143 | 151 (bcast_256m) |
| SSB | shuffled | 164 | 140 | 153 (bcast_256m) |
| TPC-DS | asis | 1256 | 597 | 706 (bcast_10m) |
| TPC-DS | sorted | 949 | 557 | 755 (bcast_10m) |
| TPC-DS | shuffled | 807 | 691 | 804 (bcast_10m) |

- **SSB has one robust rule**: raise the broadcast limit (`bcast_256m`). It is the best single configuration in all three layouts, and its asis result was identical in two independent collections (162 s with 4 warmups, 165 s with 10; q12 92 s to about 22-24 s). q13 is not fixed by any knob.
- **TPC-H has little headroom**: 5-14% from per-query choice. Shuffle and aggregation knobs are the "best single" but by 3-5%, which is within noise (see section 3).
- **TPC-DS has large headroom**, but it is inflated by failures: the TPC-DS defaults of some queries fail or time out and are counted as 300 s in these totals, so the gain over default is partly "makes it run at all". Treat the TPC-DS totals as an upper bound and look at the per-query tables.
- Best configuration identical in all three layouts: TPC-H 5 of 18 (28%), SSB 5 of 11 (45%), TPC-DS 11 of 28 (39%). Applying another layout's best configuration is rarely harmful (TPC-H 0-1 queries more than 10% slower per pair, SSB 0-2, TPC-DS 0-2) and usually helps or is neutral.

## 3. Noise

- **Per-query "best config" is mostly noise outside the strong effects.** Re-collecting the same cells with more warmups changed the per-query winner in most queries: TPC-H asis, the best configuration agreed in only 5 of 20 queries between the 1-warmup and the 4-warmup collection; SSB asis agreed in 4 of 12 between the 4- and 10-warmup collections. The queries that kept their winner are the ones with real effects (SSB q04, q10, q11, q12 with `bcast_256m`). The count of queries with a knob gain above 10% moved from 7 to 6 (TPC-H asis) and from 6 to 9 (SSB asis), so counts of "tunable queries" have an uncertainty of about 2-3 queries. The winner is a minimum over 13 configurations with 2 repetitions, so a gain below about 10-15% should not be used as a label.
- **Warmup bias**: the first runs in a phase-1 chunk are slower. TPC-H asis phase-1 with 1 warmup was 3-9% slower in every configuration than with 4 warmups (default 607 s against 568 s). SSB with 4 warmups was still about 25% slow on the first two configurations, hence 10 warmups for SSB and TPC-DS. The old files are kept in `fresh-nocache/old-warmup1/` and `old-warmup4/`.
- **Residual order effect**: the configuration order within a chunk is fixed, so the position cannot be separated from the configuration (the first two positions are `cbo_off` and `partial_decimal_sum_on`, which are slower configurations anyway). The independent check is the `default` configuration, measured at position 4 of the phase-1 chunk with 4-10 warmups against its separate default run (1 warmup): ratio 0.97-1.03 on TPC-H (geometric mean 0.99-1.03), 0.94-1.11 on SSB, 0.86-0.92 on TPC-DS. So the phase-1 numbers agree with the default-run numbers within about 3% on TPC-H and about 10% on SSB; on TPC-DS the 1-warmup default runs are up to 20% slower than the 10-warmup runs, which suggests the default-unit TPC-DS Flight times are still slightly cold-biased.
- **Outliers**: TPC-H q12 and q06 had slow single repetitions (13 to 33 s for q12, 3.7 s against 26 s for q06 in the first default runs); I did not find the cause. Cache-off Flight timings after a pod restart are otherwise stable.
- **Cluster disturbance**: no invalid runs in any unit, and `redo.py` found no query to drop. A host reboot at 2026-11-04 18:37 interrupted one unit (TPC-DS sorted phase-1) at q67; the partial q67 records were removed and the unit resumed with the finished queries kept. Two (query, config, rep) cells in TPC-DS shuffled phase-1 have an errored attempt and a successful retry; the successful record is the one to use.

## 4. Configuration failures (TPC-DS)

- `partial_decimal_sum_on` on q09 (all layouts): known wrong-result bug.
- `cbo_reorder_off` on q36, `shuffle_48` on q17, `bcast_256m` on q25 (and several other configurations on q17 and q25): Spark fails with "Not enough memory to build and broadcast the table to all worker nodes", i.e. a driver memory limit, not a Flight problem. The cache-on first pass had the first two on q09 and q36 but not q17/q25, so these depend on timing or memory state and are worth treating as "configuration can fail" in the classifier labels rather than as a speed result.

## 5. Memory growth in the Flight pods

Cache on: aged pods (about 32 h) idled at 4-8 GB and were slower. Cache off: idle memory 0.6-2 GB, peaks 2.6-4.0 GB during queries, no creep over more than 24 h of continuous collection (sampler `fresh/podmem.csv`). Together with the DuckDB documentation this points at the DuckDB buffer pool and file cache keeping data in memory. Not yet demonstrated by reading `duckdb_memory()` on an aged cache-on pod.

## 6. What this means for the classifier data

- Use Flight-vs-Direct and "which configuration" as separate labels. Flight/Direct is a stable, large effect on TPC-H and TPC-DS; the configuration label is only reliable where the effect is large (SSB broadcast limit, a few TPC-DS queries).
- Label a configuration only when it beats default by more than about 15% in the median of at least 3 repetitions, and record failures and timeouts as their own outcome.
- Record the cache mode (on/off) as a feature; the Flight advantage depends on it.

## 7. ClickBench and NYC taxi (cache off, single layout)

Same protocol as above (`collect_cbtaxi.sh`): default run 3 repetitions in both modes, then the phase-1 sweep (Flight, 13 configurations, 2 repetitions, 10 warmups) on queries with a default Flight median under 120 s. Tables: `analysis-nocache-{taxi,clickbench}-{default,knobs}.md`; data `fresh-nocache/{taxi,clickbench}-*.jsonl`. No invalid runs in any unit.

| suite | queries compared | Direct total (s) | Flight total (s) | Flight/Direct |
|---|---|---|---|---|
| taxi | 4 of 6 | 51 | 31 | 0.61 |
| ClickBench | 35 of 43 | 223 | 239 | 1.07 |

- **Taxi**: Flight is faster on q04 (3.7 s against 8.8 s) and q05 (2.0 s against 15.6 s). **Direct fails on q03 and q06** with a Parquet "column cannot be converted" error on `yellow_tripdata_2024-11.parquet`: the column types drift between years and `mergeSchema` does not repair it. Flight (DuckDB reading the files) returns a result for both. This is a Flight advantage in robustness, not speed, and should be its own label ("Direct fails").
- **ClickBench**: Flight and Direct are level in total (17 queries faster, 18 slower, median ratio 1.01). Flight is more than 15% slower on q30 (2.2x), q22 (1.4x), q07, q01, q23, q29 and q09 (1.2-1.3x). Most ClickBench queries run in 0.3-6 s, so ratios on the sub-second ones are noisy.
- **Eight queries fail identically in Flight and Direct** (Spark side, not Flight): q19 and q43 (`minute` / `date_trunc` cannot resolve the `EventTime` column type) and q37-q42 (`CAST_INVALID_INPUT` on the string '2013-07-01', i.e. the date columns are strings in this copy of the data). They are excluded from the knob sweep and should be excluded from the classifier data. The earlier worry that Flight skips Binary tables did not materialise: Flight returned results with equal result hashes on all queries that also ran in Direct.
- **Knobs**: taxi 3 of 6 queries and ClickBench 10 of 35 have a knob more than 10% better than default, but the gains are mostly on queries of a few seconds or less. Gains that stand out: ClickBench q24 (55.7 s to 43.9 s with `bcast_256m`, 21%), q31 (5.1 s to 2.7 s with `cbo_reorder_off`, 47%), q35 (13.2 s to 10.8 s with `shuffle_48`, 18%), q21 (7.0 s to 5.5 s with `shuffle_192`, 22%); taxi q06 (3.9 s to 2.0 s with `bloom_off`, 47%) and q02 (12.2 s to 10.1 s with `retain_off`, 17%). No knob wins consistently (each is faster on 2-7 queries and slower on a similar number), and `cbo_off` is slower than default on 18 of 35 ClickBench queries. No configuration fixes or breaks a query here (0 failures in the sweep).
- Consequence for the classifier: on these scan-heavy single-table suites the useful label is mainly Flight vs Direct (and "Direct fails"); the configuration label has few reliable examples (q24, q31, q35, q21, taxi q06) and each needs confirmation with more than 2 repetitions before use.

## 8. Synthetic datasets (cache off, 22 schemas x 8 queries, 50M rows)

Same protocol; default run 3 repetitions in both modes, then the 13-configuration sweep (Flight, 2 repetitions, 10 warmups). No errors and no invalid runs (4,752 sweep records). Tables from `analyze_synthetic.py`: `analysis-nocache-synthetic.md`. The schemas come from `datasets/sweep.py --phase 2`: a base dataset (1M distinct keys, no skew, random clustering, 32 files, no nulls, int key) and one-axis variations (a few schemas change two axes at once and are left out of the axis table).

- **Flight vs Direct**: Flight is faster at the median on 7 of 8 queries (ratios 0.60-0.84, faster in 19-22 of 22 schemas), level on `topn` (1.01), and slower on `join_selective_dim` (1.25; faster in only 4 of 22 schemas). All queries run in 0.7-1.5 s.
- **Axes** (total over the 8 queries, Flight/Direct): key cardinality is the clearest axis: 0.72-0.83 from 1,000 to 1M keys, 0.90 at 10M and 1.02 / 1.18 at the extremes (10 keys: 1.02; 1G keys: 1.18, Flight 38.5 s against Direct 32.5 s). More files help Flight (256 files 0.56, 32 files 0.72, 4 files 0.83). Skew, null fraction and string keys do not change the ratio (0.71-0.80; the string key is slower in absolute terms for both modes). Sorted and overlapping clustering are faster in absolute terms for both engines, with a ratio of 0.82-0.90 against 0.72 for random. Each axis value is a single schema, so these are single measurements; differences of a few percent are not reliable.
- **Knob sweep is mostly noise here**: the queries are 1 s, so a 15% and 0.2 s gain is about the size of the run-to-run variation. 24 of 176 cells show such a gain, and the largest ones (default 3.1-3.6 s against 0.5-1.0 s, e.g. `count_distinct_sel50` on the 10-key schema, several 1,000-key schemas) are slow default runs in the sweep, not knob effects. Per configuration the faster and slower counts are about equal, except `cbo_off` (slower in 74 cells, median +21%), `bcast_10m` (slower in 39, +10%) and `partial_decimal_sum_on` (slower in 26): the same configurations that lose on the other suites. No configuration is a reliable win on these data.
- Consequence for the classifier: use the synthetic data for the Flight-vs-Direct label and its dependence on key cardinality and file count (where the ratio moves from 0.56 to 1.18); do not use it for configuration labels.

## 9. StarRocks (native `flight` catalog, cache off)

Same protocol on 1 FE + 3 BE: Flight pods restarted before each chunk, default run (3 repetitions, 1 warmup), then the phase-1 knob sweep (5 catalog properties settings, 2 repetitions). `direct` reads the same Parquet through `FILES()` views over S3 with the BE data cache off. Tables: `python3 runs/compare_sr.py`; data in `runs/fresh-sr/`. Result hashes are compared at 5 significant digits (StarRocks keeps 8 decimals on averages).

- **StarRocks Flight vs Spark Flight** (geomean of per-query median wall): SSB 0.90-0.94, TPC-H 0.62-0.71, TPC-DS 0.36-0.39 (TPC-DS totals 333-374 s against 852-1043 s).
- **StarRocks Flight vs Spark direct**: SSB 0.66-0.82, TPC-H 0.22-0.51, TPC-DS 0.22-0.23. StarRocks direct against Spark direct: SSB 0.93-1.06, TPC-H 0.25-0.50, TPC-DS 0.16-0.19, so most of the gap to Spark is the engine, not the Flight path.
- **StarRocks Flight vs StarRocks direct** (geomean direct/Flight, same engine, queries valid in both): SSB 1.26-1.38 (Flight faster), TPC-H 0.96-1.12 (level), TPC-DS 0.75-0.80 (direct faster by 20-25%), synthetic 1.26 (176 query cells, 140 s Flight against 154 s direct in total). The sign of the label depends on the suite, as it does for Spark.
- **Knobs** (geomean of per-query median against `default`, 404-408 query/layout cells per setting): `complete_aggregation=false` 1.15 (slower in 76% of cells), `statistics=false` 1.11 (slower in 68%), `aggregation_pushdown=false` 0.98, `join_key_reduction=false` 0.99, both off 1.01, i.e. neutral. Only complete aggregation and statistics are worth keeping on as defaults; the two pushdown knobs give no reliable label.
- **Genuine failures**: TPC-H q09 and q18 hit the BE memory limit (`Memory of default_wg/query_pool exceed limit`) in direct mode on all layouts and in the sweep with `statistics=false` (q09) and with aggregation pushdown off (q18); TPC-H q08 timed out at 300 s once per layout in direct mode; TPC-DS as-is q72 timed out at 300 s with `statistics=false`.
- **ClickBench and taxi**: no Flight data, because the catalog lists no ClickBench tables (an unsigned-int column type is unsupported; `sr_client` falls back to the single table `hits` / `yellow` for direct views). ClickBench direct: 25 queries return, the rest fail on `varbinary` columns (`Error statistic type : varbinary(1048576)`, 39 runs) or on analyzer errors. Taxi direct fails on every query (18 of 18 runs, `Access storage error: failed to get file schema`, the files differ in the case of column names). The Flight catalog needs an unsigned-int mapping (UInt8-64 to the next wider signed type); that is a change in the StarRocks fork and cannot be built here.
- **Incident (2026-11-08 23:44)**: the FE catalog `flight` got stuck (`SHOW CATALOGS` lists it, `DROP` says the connector does not exist, `CREATE` says it exists). Taxi default, one synthetic schema and TPC-DS shuffled phase 1 q83-q99 failed 100%; restarting the FE pod fixed it, root cause unknown. The shuffled phase-1 unit had its done marker written anyway and was found only on 2026-11-09 (201 records removed, q83-q99 rerun clean). TPC-DS as-is phase 1 q38 failed in all 13 runs with `Invalid Flight endpoint ticket` (cluster formation after a pod restart) and was rerun clean (`tpcds-asis-phase1-rerun.jsonl`).

## 10. Why StarRocks over Flight is slower than direct on TPC-DS (profile)

Setting: `tpcds_sf100_asis`, cache off, query profile (`ANALYZE PROFILE`) of 20 queries in both modes (`profile_scans.py`, `runs/features/tpcds-profile.jsonl`); join-key-reduction test in `runs/features/h2-join-key-reduction.jsonl`. One profiled run per query and mode, so single measurements.

- **Not a fixed per-query overhead.** Over the three layouts Flight/direct is 1.27 for the 37 queries where direct takes under 5 s and 1.31 for the 62 where it takes 5 s or more; the gap grows with the work done.
- **Per query (Flight/direct wall):** q88 3.8, q09 3.3, q29 2.5, q49 2.3, q02 2.3, q48 2.1, q03 1.8, q32 1.6, q76 1.5, q74 1.5, q11 1.3, q95 1.3, q07/q19/q78 1.15-1.2, q52 1.1, q80 1.05, q13 1.03, q42 0.75. q72 over Flight timed out at 300 s in this run although it took 26 s in the main collection, so run-to-run variance is large.
- **Scan time dominates, and the BE waits.** q88: the Flight scan of `store_sales` has ScanTime 42 s with CPU 0.5 s (direct: 4.4 s); summed over the 32 scans 176 s against 38 s.
- **Extra rows explain only some queries.** q88 scans return 16.6M rows over Flight against 6.9M direct, q03 44M against 2.8M (runtime filters are applied only in the BE after the rows arrive). Over 19 queries the correlation between the row ratio and the slowdown is -0.36. q02, q29 and q48 return equal or fewer rows over Flight and are still 2.1-2.5x slower; q09 has summed scan time 288 s against 77 s. The remaining cost is per-row work on the Flight server and the wire (DuckDB read, Arrow encoding, transfer) against the BE's native Parquet reader. This is inferred from the profile, not measured on the server.
- **The pushed key lists are not the cost.** Join-key reduction pushes `ss_hdemo_sk IN (7,200 keys)` (the whole key domain) to the Flight scan. Switching join-key reduction off (3 repetitions after a warmup, median): q88 19.9 s -> 32.0 s (slower), q29 13.5 -> 12.8, q02 3.9 -> 3.7, q09 15.9 -> 15.7, q48 10.6 -> 10.4, q03 7.3 -> 7.6. No query gets faster by a clear margin and q88 loses 60%, so the setting helps where it reduces rows and the long list costs nothing measurable.
- **Limits.** Profile metrics are coarse (ScanTime is summed over parallel instances; my parser misses some operators); one run per query; the profiled q02 took 8.6 s but 3.9 s in the test with profiling off, so profiling itself costs time.
- **What to measure next.** Time inside the Flight server per endpoint (DuckDB scan, Arrow encode, gRPC send) with rows and bytes sent; then try more endpoints per scan, pushing min/max or bloom filters from the join build side into the Flight scan, and Arrow IPC compression.

## 11. Server-side profile of StarRocks over Flight (TPC-DS as-is)

New Flight-server metrics (`arrowflight_stream_*` on port 9404 of each pod, commit 1d3e575): rows read and sent, batches, Arrow bytes on the wire, time to the first batch (includes the DuckDB `executeQuery`, i.e. the scan and execution), time waiting for batches inside the loop, time handing batches to Flight (copy, compression, serialisation, gRPC enqueue), back-pressure wait, and summed server execution time. `profile_server.py` scrapes the three pods before and after each repetition (one warmup, three measured; median shown; data in `runs/features/server-profile.jsonl`). Server seconds are sums over concurrent endpoint streams, not CPU seconds; compare their shares, not their sizes to the wall time. The direct and earlier Flight columns are medians from `runs/fresh-sr/tpcds-asis-*.jsonl`.

| query | Flight wall (s) | direct (s) | earlier Flight (s) | streams | rows sent | wire MB | first batch | loop wait | send | back-pressure | share of first batch |
|---|---|---|---|---|---|---|---|---|---|---|---|
| q02 | 1.6 | 4.1 | 1.7 | 21 | 92K | 2.5 | 12.5 | 0.0 | 0.0 | 0.0 | 100% |
| q29 | 11.2 | 5.9 | 11.5 | 32 | 446M | 19,221 | 22.8 | 92.3 | 11.2 | 4.7 | 17% |
| q48 | 4.7 | 7.2 | 4.5 | 13 | 44M | 2,128 | 7.8 | 23.6 | 1.3 | 0.0 | 24% |
| q09 | 16.8 | 6.9 | 16.8 | 136 | 136 | 0.0 | 1,245 | 0.4 | 0.0 | 0.0 | 100% |
| q88 | 16.9 | 5.3 | 17.1 | 104 | 89M | 2,161 | 752 | 120 | 3.8 | 0.0 | 86% |
| q03 | 6.4 | 10.8 | 10.6 | 12 | 44M | 899 | 26.7 | 8.5 | 0.5 | 0.0 | 75% |
| q42 | 5.3 | 4.2 | 2.3 | 12 | 5.0M | 101 | 33.6 | 0.1 | 0.1 | 0.0 | 99% |
| q72 | 30.8 | 43.3 | 46.4 | 28 | 558M | 21,003 | 6.1 | 52.0 | 11.0 | 12.5 | 7% |

- **Sending and back-pressure are small everywhere.** Even q29 and q72, which put 19-21 GB on the wire, spend 11 s and 5-12 s (summed over streams) there. The BE keeps up with the stream.
- **The slow queries are DuckDB-bound on the Flight pods.** q09 (136 endpoints, 136 result rows, 0 MB) and q42 (5 MB) spend 99-100% of the server time before the first batch, q88 86%. In these the Flight servers scan and aggregate `store_sales` and the answer is tiny, so wire and BE are not the cost. Switching pushdown off does not help (sweep: `agg_pushdown_off` / `complete_agg_off` on q09 1.0-1.19, on q88 0.93-1.08 of default), so moving the aggregation to the BE does not remove the scan cost.
- **q29 is different.** It sends 446M rows (19 GB) although the BE keeps about 113M after runtime filters (profile of section 10): the join filters reach the BE only after the rows arrive.
- **The comparison is not like for like in CPU.** Each Flight pod is limited to 4 CPUs with `duckDbThreads=4` and a 5 GB DuckDB memory limit (3 pods, 12 cores in total), while each StarRocks BE has 8 CPUs (24 cores). In direct mode the BEs scan with 24 cores; in Flight mode the scan and aggregation run on 12 cores. This is a plausible main reason for the TPC-DS gap and is untested; the Flight pods and BEs also share the same three nodes.
- **Earlier ratios were partly profiling overhead.** In section 10 the profiled runs were slower than unprofiled ones (q02 8.6 s against 3.9 s). In this run, without profiling, Flight is faster than direct on q02, q48, q03 and q72 for the as-is layout, and slower on q09, q88, q42 and q29. Treat section 10's per-query ratios as directional.
- **Operational finding.** After the Flight pods are restarted, the StarRocks FE keeps the old pod IP in its Flight connector (`No route to host: flight-0.flight/10.42.1.174`) until the catalog is dropped and created again. `restart_flight.sh` does not do this. It is a candidate cause for the `Invalid Flight endpoint ticket` runs after restarts.
- **Recommended next test.** Give the Flight pods the same CPU as the BEs (8 CPUs, `duckDbThreads=8`) and rerun q09, q88, q42 and q29. If the gap closes, the gap was resources, not the Flight design. Changing the pods invalidates comparison with the existing runs, so it needs a separate labelled dataset.

## Open items

- Synthetic queries are too short (about 1 s) for configuration labels; a larger row count or heavier queries would be needed.
- StarRocks Flight needs an unsigned-int type mapping before ClickBench can be read through the catalog; the unit must check for failed ranges before writing its done marker.
- TPC-DS q04 Stage B is missing; q12 TPC-H outliers unexplained; the aged-pod memory experiment is not done.
- Scale factors beyond SF100 are left for later.
- The ArrowFlight commits are pushed; the StarRocks fork commit a44e138a is still local.

## 12. Flight pods at 8 CPUs (resource-confound test)

Flight pods changed from 4 to 8 CPU limit (request 2 to 4) and `duckDbThreads` 4 to 8; memory unchanged. Same method as section 11 (StarRocks flight mode, tpcds_sf100_asis, 1 warmup + 3 reps, medians). Data: `runs/features/server-profile-8cpu.jsonl`. This is a separate dataset: the other results use 4 CPUs.

| query | Flight wall, 4 CPU | Flight wall, 8 CPU | direct wall (4-CPU run) | DuckDB first-batch seconds, 4 / 8 CPU |
|---|---|---|---|---|
| q09 | 16.8 s | 12.3 s | 6.9 s | 1245 / 789 |
| q88 | 16.9 s | 8.3 s | 5.3 s | 752 / 333 |
| q42 | 5.3 s | 4.4 s | 4.2 s | 34 / 24 |
| q29 | 11.2 s | 11.7 s | 5.9 s | 23 / 50 |

Doubling the Flight CPUs helps the DuckDB-bound queries (q88 halves, q09 -27%, q42 -17%) but does not close the gap to direct on q09 and q88. q29 is unchanged: it is bound by the 446M rows sent, not by DuckDB. Part of the Flight-vs-direct gap is therefore resources (12 vs 24 cores), part is DuckDB being slower than the BE's scan for these queries.

## 13. Why is q09 slower over Flight? It is not DuckDB compute (correction to section 11)

Section 11 called q09/q88/q42 "DuckDB-bound" because the time to the first batch was large. That time is the wait for DuckDB to read from MinIO, not DuckDB compute. Probes (`runs/features/ddb-probe/`, run inside the Flight pods with the server's own DuckDB settings and S3 config):

- **Plan shape is the same.** q09 is 15 scans of `store_sales` (9 endpoints each) over Flight, and 15 scans in direct mode.
- **Compute is small.** One endpoint's scan (16 files, 2 columns, 8 threads) takes 0.16 s alone. A pod's whole q09 share (45 scans in one DuckDB) takes 3.9 s alone with about 40 CPU-seconds, at 8, 16 and 32 threads alike. The BEs spend about 140 CPU-seconds each (about 420 in all) on the same query.
- **Both read the same bytes.** MinIO pod transmit: 9.6 GB for direct and 9.8 GB for Flight. Per node it is 3.2 GB in both modes.
- **Read bandwidth differs by node.** A plain `mc cat` of 24 files reads at 4.0 GB/s on compute1 (MinIO's node) but 0.3-0.85 GB/s on compute2 and compute3, with large run-to-run variation. Running the same 45 scans on all three pods at once, compute1 finishes in about 5 s and the others in 8-12 s, at the same CPU. Added threads do not help.
- **Open question.** The BE on compute2 reads its 3.2 GB in under 6.5 s, while the Flight pod on compute2 needs about 10 s for the same bytes. The cause of that 1.5x difference (connection count and request sizes in DuckDB httpfs versus the BE's reader) is not established; the DuckDB and StarRocks sources have not been read yet.

The 8-CPU result in section 12 fits this: more CPUs shorten the compute share, but the wait for the S3 reads on the two remote nodes stays.

## 14. Fix: httpfs connection caching (commit 3bb34ba)

Cause found in section 13's probes: the DuckDB 1.5.6 httpfs in the pods has `httpfs_connection_caching = false`, so every ranged GET (about 1.4 MB per Parquet column chunk) opens a new TCP connection. Setting it to true: 45 q09 scans on a remote pod took 5.0-5.6 s instead of 7-11 s (same CPU, three pods at once, repeated twice), and the three pods finished together. The server now sets it (`-DduckDbHttpConnectionCaching`, default true). Same method as sections 11/12, Flight pods still at 8 CPUs; data `runs/features/server-profile-8cpu-conncache.jsonl`.

| query | Flight 4 CPU | Flight 8 CPU | 8 CPU + conn. caching | direct |
|---|---|---|---|---|
| q09 | 16.8 s | 12.3 s | 6.4 s | 6.9 s |
| q88 | 16.9 s | 8.3 s | 8.0 s | 5.3 s |
| q42 | 5.3 s | 4.4 s | 3.6 s | 4.2 s |
| q29 | 11.2 s | 11.7 s | 11.1 s | 5.9 s |

q09 and q42 now match direct. q88 and q29 are not explained by this: q29 is bound by the 446M rows sent (BE-side runtime filters do not reach the Flight scan), and q88 has not been probed yet. All earlier Flight numbers in these findings were taken without connection caching, so Flight reads were slower than they need to be.

## 15. q88 and the runtime IN-filter pushdown (StarRocks BE source versus DuckDB)

**q88** is 8 sub-selects, each joining `store_sales` to three dimensions. Over Flight each becomes a `store_sales` scan with 9 endpoints, carrying an `ss_sold_time_sk IN (1800 values)` list from the join-key reduction rule (the list is the whole range 43200-44999). After section 14 it ran 8.0 s against 6.2-6.6 s in direct mode (same session).

What the sources and probes show:
- **DuckDB is not the limit.** A pod's share of q88 (24 `store_sales` scans of 16 files, IN list as sent) runs in 2.9 s at about 23 CPU-seconds, with all three pods running at once. A BETWEEN instead of the IN list changes it by 0.1-0.2 s; dropping the filter altogether is 0.6 s faster per pod but returns 25 times more rows.
- **The scans run in parallel** (9 drivers per scan node, one endpoint each, `MorselsCount` 1), not one after another. Per-endpoint scan time on the BE is 2.4-6.3 s.
- **The BE discards most of what it is sent.** Each endpoint sends about 1.2M rows (88.7M in all, 2.2 GB on the wire) and the BE's join runtime filters keep about 37K of them. The reason is in the BE source (`be/src/connector/flight_connector.cpp`, `common/config.h`): `flight_runtime_in_pushdown` and `flight_runtime_bloom_pushdown` both default to false, so the filters are applied only after the rows have been shipped. They are mutable configs.
- **Planning costs 0.4 s more over Flight** (profile: ExecPlanBuild 402 ms against 4 ms; the FE calls the Flight server once per scan node).

Setting `flight_runtime_in_pushdown=true` on the three BEs (runtime `update_config`, not persisted):

| query | before | with IN pushdown | rows sent before / after |
|---|---|---|---|
| q88 | 8.0 s | 5.3 s | 88.7M / 11.8M |
| q09 | 6.4 s | 6.3 s | none (aggregates) |
| q29 | 11.1 s | 11.0 s | 446M / 446M (no IN filter built) |

Results are identical with the pushdown on and off for q88, q29, q09, q42 and q03 (same result hash). Setting `flight_runtime_bloom_pushdown=true` as well changed nothing for q29 and q88: no bloom filter was offered (no "offering runtime bloom filter" line in the BE log). In the source a bloom filter is offered only if it has already arrived when the scan opens, has no NULL and is a bloom type; why q29's filters do not qualify is still open. Data: `runs/features/server-profile-8cpu-conncache-inpush.jsonl`, `...-inbloom.jsonl`.

The BE configs were left at in_pushdown=true, bloom_pushdown=true on all three BEs; they revert on a BE restart. Flight pods are still at 8 CPUs.

## 16. q29: the filters can be sent, but they do not decide the wall time

q29 joins `store_sales` (288M rows), `catalog_sales` (144M) and `store_returns` (29M); 446M rows are sent to the BEs and the wall is 10-11 s over Flight against 5.8-8.5 s direct. Both modes read the same 2.3 GB from MinIO, so S3 is not the limit here.

- **The join filters arrive after the scans have opened.** With the BE pushdown flags on (section 15) and the default `runtime_filter_scan_wait_time` of 20 ms, the `store_sales` scan opens before the global join filters exist: the BE log shows no or only a few filters offered per run, and the server sends all 446M rows. `store_sales` is joined on three keys and the useful filters are global (the join with `store_returns` is partitioned).
- **Making the scans wait changes what is sent.** With `SET runtime_filter_scan_wait_time` of 3000-10000 ms the BE offers bloom filters on `ss_ticket_number` and `ss_item_sk`, the server sends 188M rows instead of 446M (6.6 GB on the wire instead of 19.2 GB, bloom filtering done in the server's Java row filter), and server execution time over all streams drops from 128 s to 80 s. At 2000 ms and below it made no difference.
- **The wall time does not move** (10-11 s with a wait of 3000 or 5000 ms, 10.3 s with 10000 ms, 11 s without). The transfer is therefore not the bottleneck either. In the BE profile the critical path is a chain of dependent scans (dimension scans, then the `store_returns` build side, one endpoint of which took 6.8 s for 125K rows, then the `store_sales` probe scan, 2.4-7.8 s per endpoint) followed by the joins; with the filters the BE receives 0.3-1.1M rows per `store_sales` endpoint instead of 32M but each endpoint still takes seconds.
- **DuckDB itself is quick on these scans.** One pod's share (3 endpoints of each of the three tables, 9 concurrent streams, 8 threads) exported as Arrow batches in 2.7 s with 17 CPU-seconds; counting only took 0.75 s.

Not established: what makes the individual `store_sales` endpoints take 2.4-7.8 s when the same work takes about 2.7 s per pod in isolation (with all three pods running at once and the BEs consuming the streams). The BE-side conversion of Arrow batches, and the wait for the dimension filters, are the candidates.

State left: BE `flight_runtime_in_pushdown` and `flight_runtime_bloom_pushdown` are on (runtime on the three BEs; default changed to true in the fork, commit e3dd8b96, not built into an image and not pushed). `runtime_filter_scan_wait_time` is a session variable and was only changed in the test connections. Data: `runs/features/server-profile-q29-rfwait*.jsonl`; `profile_server.py` takes `PROFILE_SET` for session settings.

## 17. q29 deeper: where the 10 s go

Probes: `runs/features/ddb-probe/Q29.java` (the three big q29 scans, one pod's share, in DuckDB with the server's settings), `Pull.java` (plain Java Flight client against the real server), per-pod CPU (cgroup `cpu.stat`) and network counters sampled every 0.5 s during the query.

- **No pod is CPU-bound or throttled.** Over a 10 s q29 run the Flight pods use 28-37 core-seconds each (of 80 available) and the BEs about 22 each; there is no CFS throttling anywhere.
- **DuckDB alone is fast on these scans.** One pod's share (150M rows, 9 streams, Arrow export through the same call the server uses) takes 2.7 s with all three pods running at once. The same scans take 4-8 s per endpoint inside the real query, so the server path (Java side of `streamSql`, gRPC, the consumer) adds the rest.
- **Network.** Unfiltered, a q29 moves 19.2 GB on the wire in about 9 s: the BEs receive about 6.9 GB each (average 0.75 GB/s, peak 1.4 GB/s) and the Flight pods send 5.8-7.7 GB each. A plain Java client reading `store_sales` alone from one pod needs 12.5-13.5 s for 11.8 GB, about 0.9 GB/s, which is of the order of what one node's link delivered in the S3 read tests of section 13, so a single node's link may be the limit for that test.
- **With the join filters (wait 5000 ms) the data phase ends at about 5 s** (BE receive rate falls to 0.1 GB/s by then) but the query still ends at 9 s.
- **BE profile with the filters (wait 3000 ms), driver by driver.** The `store_sales` scan drivers live 10-10.8 s: 3.0 s of that is the scan waiting for the join filters and giving up at the timeout (they had not arrived), then 7 s of streaming. The `catalog_sales` scan drivers finish in 3.5 s. The three `store_returns` endpoints on one backend take 7.0 s for about 125K rows each, while the other six take 2.2 s; that scan is the build side of the join whose filter the `store_sales` scan is waiting for, so a 7 s straggler on a tiny scan holds up the filters and with them the probe.
- **A candidate for the straggler, not tested:** DuckDB serves all concurrent queries of a pod from one task queue, so a small scan that arrives next to several large exports may queue behind them. In the isolated probe all nine scans start together and the `store_returns` ones finish in 1.1-1.7 s, so the queueing, if it is the cause, depends on arrival order.

Not established: which of the server path or the BE consumer makes `store_sales` endpoints take 4-8 s instead of 2.7 s, and why one backend's `store_returns` endpoints take 7 s. Next measurements would be a per-stream timeline on the server (start, first batch, last batch per ticket) and an interleaved BE-side counter for time blocked in `Next()` versus converting.

## 18. Per-stream timing on the server (commit 1f3551f)

`DuckDbAdapter.streamSql` now logs one `streamTiming` line per stream: epoch start, then milliseconds for execute (prepare + `executeQuery` + export setup), first batch, total, time waiting in `loadNextBatch` (duck), time in the send path, back-pressure, rows read and sent, and the start of the SQL. Image built by CI and rolled out to the Flight pods (8 CPUs, connection caching on).

**q29 timeline, steady state, no filter wait (wall 11.9 s, 33 streams):**
- All streams start within 0.2 s of each other.
- The three `store_returns` build-side scans per pod finish in 0.6-1.2 s. The 7 s straggler of section 17 is not there; it came from the run with `runtime_filter_scan_wait_time` set, where the scan waits for dimension filters first.
- `catalog_sales` streams finish in 2.0-6.6 s.
- The nine `store_sales` streams (30.5M rows each) take 9.0-10.7 s each: 1.2-2.4 s to the first batch, 6-8 s waiting in `loadNextBatch`, 0.5-0.9 s sending, back-pressure 0. They set the wall time.
- The Java send path and the gRPC consumer are therefore not what holds the stream: the server spends its time inside DuckDB's export, at about 4M rows/s per stream.

**Same three queries read by a plain Java client over loopback in each pod** (`Local.java`, so no BE and no network): `store_sales` streams take 4.5-6.5 s on the server. So of the 9-10.7 s in the real query, about 5-6 s is the server producing the rows and about 4 s depends on the consumer side (BEs, network, 8-core pods shared with the BEs). The DuckDB-only probe of section 17 did the same export in 2.7 s, so the server process adds about 2x over bare DuckDB.

**Warm-up (affects the benchmark method).** Right after restarting the Flight pods, q29 takes 26.0, 20.0, 15.8, 10.9, 10.9, 12.3 s on consecutive runs; the first runs of q09 and q03 on the warmed pods are again slower than their second runs (q09 13.5 then 7.3 and 8.4 s; q03 10.2 then 8.4 s). Server-side stream times show the same drop (`store_sales` streams 14-22 s in the first runs, 5-6 s by the ninth), with the send part constant. The harness restarts the Flight pods before every chunk and uses one warmup run, so the first queries of a chunk are measured on colder pods. The cause (memory growth and trimming in the server process, the first read of a table, or something else) was not identified; a fresh DuckDB-only JVM did not show it.

Not done: a BE-side counter for time blocked in `Next()` versus converting (it needs a BE rebuild).

## 19. The warm-up: first use of a table on a restarted Flight pod

Probes: `warm.sh`, `proc.sh`, `Local.java`, `Plan.java`, `Stats.java` in `runs/features/ddb-probe/`; server timing from `-Dorg.slf4j.simpleLogger.log.Timing=debug`.

**Where the first-run time goes.** In a real q29 run right after restarting the Flight pods the wall was 22.1 s, then 10.2, 11.6, 10.3, 10.3, 11.1 s. In that cold run the nine `store_sales` streams themselves were barely slower (median 8.0 s against 7.2 s), but the last of them started **12.8 s** after the query began. The time is before the streams open, in the server's first answers about each table:
- `GetFlightInfo` (planning) on a cold server: `store_sales` 3.6-3.9 s, `catalog_sales` 1.0 s, `store_returns` 0.25 s; the second call takes 0.04-0.06 s.
- `arrowflight.table_statistics` cold: `store_sales` 2.9 s, `catalog_sales` 2.0 s, `store_returns` 0.4 s; then 0.01 s.
- The first `GROUP BY` key query (the one the join-key reduction uses) 5.1 s, then 0.3-0.6 s.
- Server timing shows where the planning time goes: of the 2.5 s for `store_sales` (144 files), file location takes 20 ms, server validation 32 ms and the endpoints 30 ms in all; the rest is `ParquetAdapter.estimateRowCount`, which reads the Parquet footer of each file one after the other (`parquetMetadata` is cached afterwards, so the second call is fast). That is about 17 ms per file on a cold cache.
- Adding these for the tables of q29 gives about 12 s, the observed delay.

**Smaller effect inside the streams.** With the server driven by a local client (no BE), the first repetition after a restart takes 6.6-7.2 s against 4.8-5.0 s later, using about 30 CPU-seconds of user time instead of 21 (JIT compilation and interpretation); page faults are not higher in the first repetition and neither is system time much. Server-side stream times: 6.2-7.0 s first, 4.7 s from the fifth. `MALLOC_ARENA_MAX=2` is not the cause: with it removed the first repetition was 7.2 and 8.2 s (against 11.1 and 8.9 s with it, a difference within run-to-run noise) and the steady state was slower (6.0 s against 5.3 s).

**Not explained:** the earlier slow decay over four runs (26.0, 20.0, 15.8, 10.9 s) was seen once, right after the new image was rolled out, and did not recur on later restarts, where only the first run was slow.

**Consequences.** (1) The first query to touch each table after a Flight pod restart pays several seconds of server metadata work that later queries do not; the harness restarts the pods before every chunk, so each chunk's first queries carry it, and one warmup run of the same query absorbs it only for that query. (2) A cheap fix on the server is to read the footers of `estimateRowCount` in parallel on the I/O pool. (3) A cheap fix in the protocol is to touch every table once after each restart (planning, statistics and one key query per table) before measuring; whether that counts as a "cache" in the cache-off protocol is a decision for the protocol, since direct mode reads footers on every query.

## 20. Parallel footer reads, and the metacache check

**BE footer cache in direct mode.** `enable_file_metacache` (session variable, default true) is wired only into the Hive/Iceberg connector scan (`hive_connector.cpp`, `file_reader.cpp`); `FILES()` goes through `FileScanNode` and `ParquetReaderWrap`, which have no metacache lookup. Measured on `select count(*), max(ss_ticket_number) from store_sales` (144 files) in `direct_tpcds_sf100_asis`, 3 runs per setting alternating on/off/on/off: on 1.87/1.65/1.61 and 1.66/1.60/1.58 s, off 1.65/1.71/1.69 and 1.67/1.63/1.68 s. No effect, and the first run is not slower. Direct reads are therefore footer-cold on every query without BE restarts; the OS page cache and MinIO's cache are not controlled for either engine.

**Parallel footers (b85c784).** `ParquetAdapter.estimateRowCount` now reads footers on a 16-thread pool (`-DparquetFooterThreads`). Cold `GetFlightInfo` on a restarted pod (client on the pod, 1 run): `store_sales` 3.6-3.9 s before, 1.2 s after when it is not the first call on the pod (2.7 s when it is; the first call of any table pays about 1.5 s of one-off initialisation, `date_dim` first took 1.5 s). `catalog_sales` 0.6-0.7 s. Cold statistics are unchanged (`store_sales` 1.4 s, `catalog_sales` 1.4 s, `store_returns` 0.4 s) and the first key query dropped from 5.1 s to 0.9 s.

**End to end it did not help the first q29.** First q29 after a restart: 23.9 s and 21.6 s (22.1 s before the change); the second run 11.2 and 10.5 s. The bulk of the `store_sales` streams still start 9-10 s after the query began (12.8 s before). So the footer reads were about 3 s of the cold start; the other 9 s of startup (cold statistics, first-call initialisation, FE planning) and the first run's slower streams remain. The first run of each table on a restarted pod stays slow, so the protocol still needs the post-restart table touch (or an extra warmup) to compare cold-start-free numbers.

## 21. StarRocks direct vs Flight with the fixes (TPC-DS sf100 asis)

Flight pods at 8 CPUs / 8 DuckDB threads with connection caching, parallel footers and the BE IN/bloom pushdown flags on; direct through the `FILES()` views with the BE data cache off. One warmup per query and mode (it absorbs the first-use cost of section 19), then 3 alternating repetitions; medians in seconds, result hashes equal for all ten queries. Data: `runs/features/direct-vs-flight-now.jsonl`.

| query | flight | direct | flight/direct |
|---|---|---|---|
| q03 | 2.05 | 5.36 | 0.38 |
| q07 | 6.95 | 8.82 | 0.79 |
| q19 | 2.45 | 5.42 | 0.45 |
| q42 | 1.46 | 3.71 | 0.39 |
| q52 | 1.32 | 3.82 | 0.35 |
| q55 | 1.30 | 4.58 | 0.28 |
| q96 | 0.92 | 2.66 | 0.35 |
| q09 | 6.34 | 6.90 | 0.92 |
| q88 | 5.50 | 5.27 | 1.04 |
| q29 | 10.56 | 5.93 | 1.78 |

Flight wins by 2-3x on the small star joins, ties on q09 and q88, and loses on q29 (three large fact tables, 188-446M rows crossing the wire). One run set, n=3 per cell, one cluster state; Flight pods have 8 CPUs against the BEs' shared 16-core nodes.

## 22. q29 at the original pod size (4 CPUs, 2 CPU request, 18Gi, 4 DuckDB threads)

Flight pods put back to the manifest values of `31-flight-cluster-s3.yaml` (cgroup `cpu.max` 400000, `duckDbThreads=4`, `endpointsPerServer=3`; memory was never changed). Parallel footers, connection caching and the BE pushdown flags are on.

- **Same wall as with 8 CPUs.** q29 flight 10.1-10.8 s (3 runs, 10.6/10.5/10.1 s in the server profile), direct 5.6-5.7 s; hashes equal. Data: `runs/features/server-profile-q29-4cpu.jsonl`, `q29-4cpu-cmp.jsonl`. So neither CPU (4 or 8), nor DuckDB threads (4 or 8) moves the wall.
- **More endpoints do not help.** `endpointsPerServer` 3 / 6 / 12 gives 10.1 / 10.6-12.2 / 11.3-12.2 s (56 and 92 streams); back-pressure drops at 12 (0.4-1.3 s summed) but the wall does not. One stream per BE scan thread is therefore not the limit either. Data: `server-profile-q29-4cpu-eps{6,12}.jsonl`.
- **What the BE receives (query profile, `pipeline_profile_level=2`).** Of the 446M rows crossing, the BE keeps 114M after its own join runtime filters: `store_sales` 275M in, 7.6M out; `catalog_sales` 143M in, 105M out; `store_returns` 28M in, 1.8M out. The `store_sales` scan drivers spend IOTaskExecTime of up to 9.1 s each (the query is 9.7 s), `catalog_sales` up to 5.1 s, so the `store_sales` scan is the critical path and the filter evaluation inside the BE is cheap (3.3 s summed over drivers).
- **Raw network, same traffic pattern.** TCP between the Flight pods and the BE pods (each pod 3 connections to each BE, 0.71 GB per connection, 19.2 GB in total, probe `sink.pl` / `src.pl`) completes in 4 s and 6 s (two runs). q29 moves its 19.2 GB in about 9 s while also reading Parquet from MinIO over the same links, so the wire volume alone is roughly half to two thirds of the wall time and is the one factor that stayed constant across all the variations above.

What this says: q29 in Flight is bounded by moving 446M rows (19.2 GB) over the network to the BEs, 97% of the `store_sales` rows to be thrown away there, while direct filters while decoding. Server CPU, thread count and stream count are not the limits. The two ways to reduce the volume were tried before: waiting for the BE bloom filter (6.6 GB on the wire, but the wait costs what it saves, section 17) and nothing else. Open options that change the volume without waiting: compress the Arrow IPC stream (LZ4/ZSTD), narrower wire types, or a static semi-join reduction computed by the FE before the scan opens (the dimension side of this query is small). Not measured here: how much of the time inside `Next()` is waiting for the wire and how much is Arrow-to-chunk conversion in the BE.

## 23. Would a filter function inside DuckDB help? (probe `ddb-probe/Func.java`)

One pod's `store_sales` share (48 files, 91.7M rows after the null filters, 3 concurrent streams, 4 threads, Arrow export as in the server, 3 reps; wall / CPU):

| extra predicate | rows out | wall | CPU |
|---|---|---|---|
| none | 91.7M | 2.7 s | 9.5 s |
| `ss_item_sk <= 6000` (plain comparison, pushed into the scan) | 2.7M | 1.2 s | 4.7 s |
| `hash(ss_item_sk) % 100 < 3` (function, not pushable) | 2.7M | 1.4 s | 5.6 s |
| `ss_ticket_number IN (SELECT ... range(0,730000))` (semi-join) | 2.8M | 1.6 s | 6.2 s |
| `ss_ticket_number <= 720000` (clustered column) | 8.3M | 0.4 s | 0.9 s |

- A function filter that DuckDB cannot push into the scan costs about the same as the pushed comparison (1.4 vs 1.2 s): the saving is not exporting 97% of the rows to Arrow, not late materialisation. The floor of about 1.2 s is reading and decoding the six columns.
- `ss_ticket_number` is clustered in the files, so a min/max range prunes row groups (0.4 s). A bloom filter alone cannot do this; the min/max of the build side would have to be sent too.
- A bloom function registered in DuckDB (native extension) would therefore be expected to land at about 1.4 s per pod instead of 2.7 s, with half the CPU, if the filter exists when the scan opens. It does not change when the filter arrives (section 17), and the current Java filter already avoids the wire cost, so the gain is the server-side export work only.

## 24. Min/max with the bloom filter: not built, because the data says it cannot help q29

Checked before touching the BE (a BE image build takes 20 minutes to 3.5 hours in CI):

- **The bloom filter's key range is the whole table.** The build side of the `store_sales`/`store_returns` join (returns of months 9-12 of 1999) has `sr_ticket_number` from 4 to 23,999,968, the full range of `ss_ticket_number`. In `Func.java` a `BETWEEN 4 AND 23999968` predicate leaves the scan at 91.7M rows and 2.7 s. Min/max of the bloom filter would prune nothing here.
- **The filter that does the work is a Bitset filter, and the connector never sends it.** The BE profile of q29 lists four runtime filters on the `store_sales` scan: one `BloomFilter` and three `BitsetFilter` (`RuntimeFilterDesc: <2: BloomFilter> <3: BitsetFilter> <5: BitsetFilter> <6: BitsetFilter>`). `collect_flight_bloom_filters` only takes `BLOOM_FILTER` and `collect_flight_in_filters` only join-built IN predicates, so Bitset filters stay in the BE. The date filter (`d1`, 30 contiguous keys 2451423-2451452 out of 1827 days) takes the scan from 93.6M to 2.6M rows in the BE.
- **Upper bound for sending it.** DuckDB, one pod's share, `ss_sold_date_sk BETWEEN 2451423 AND 2451452` or the 30-value IN list: 1.9M rows, 1.2-1.3 s instead of 2.7 s (the column is not clustered, so no row-group pruning, but 98% of the export is gone).
- **End to end with a static predicate.** q29 with `AND ss_sold_date_sk BETWEEN 2451423 AND 2451452` added (same hash as the plain query), warm pods, two rounds of 3 runs: plain 9.9-11.2 s, with the date range 7.7-9.0 s, direct 5.7-5.9 s. So a delivered date filter is worth about 2 s of the 4-5 s gap; `catalog_sales` (143M rows in, 105M kept) and the remaining `store_sales` transfer account for the rest.
- Pods had just been restarted: the first plain runs were 23.5, 17.4, 12.1, 11.7 s, the same decay as in section 19.

## 25. Bitset runtime filters sent as IN lists (fork 419d6c2a, server RuntimeInFilters)

**Change.** The BE connector now turns a join's `BitsetFilter` (broadcast join over a dense integer key) into an IN-filter (`collect_flight_bitset_filters`; skipped when it holds NULL, no member, more than 2^26 values or 1M members). BE image built by CI (run 42850033923, 3 h 50 min with a cold compiler cache) and rolled out to the three BEs only. For q29 the `store_sales` scan then sends `ss_sold_date_sk` (30 values), `ss_store_sk` (402) and `ss_item_sk` (204,000 = the whole item domain); `store_returns` sends `sr_returned_date_sk` (122) and `sr_item_sk` (204,000).

**A trap found on the way.** With the filters delivered (`runtime_filter_scan_wait_time` >= 500 ms) q29 got slower, 13-16 s, with the same 93M rows and 3.2 GB on the wire. The per-stream timing showed streams of 0.1-0.7M rows with `duckMs` 20-600 ms but `executeMs` 6-14 s: DuckDB needs seconds to plan a SQL `IN` list of 204,000 literals. Fix in the server (`RuntimeInFilters`, `MAX_IN_VALUES` = 4096): a longer value set goes in as `BETWEEN min AND max`. Unit tests added; the test file passes locally.

**q29 at the original pod size (4 CPUs, 4 threads), median of 4 runs after a warmup, hash equal to direct:**

| runtime_filter_scan_wait_time | wall | rows sent | wire |
|---|---|---|---|
| 20 ms (default) | 9.7 s (first runs on fresh pods 13.5-14.9 s) | 446M | 19.2 GB |
| 200 ms | 4.3 s | 93M | 3.2 GB |
| 500 ms | 4.4 s | 93M | 3.2 GB |
| 1000 ms | 4.5 s | | |
| 3000 ms | 4.7 s | | |
| direct | 5.8-5.9 s | | |

Flight is 25% faster than direct once the filters arrive before the scan opens; the default wait of 20 ms is too short for them (they come from the dimension scans, which take 50-200 ms here). Data: `runs/features/q29-bitset-in.jsonl` (before the range fix: 4.3-4.5 s at 200 ms and 13-16 s at 500 ms and up), `q29-bitset-in-range.jsonl`, `server-profile-q29-bitset-wait{200,500}.jsonl`.

**Does a longer wait hurt the other queries?** Ten TPC-DS queries, wait 20 vs 500 ms, 3 runs each after a warmup, flight only, all hashes equal to direct. Data: `runs/features/wait-sweep-bitset.jsonl` (4 CPUs), `wait-sweep-bitset-8cpu.jsonl` (8 CPUs / 8 threads).

| query | 4 CPUs wait20 | 4 CPUs wait500 | 8 CPUs wait20 | 8 CPUs wait500 |
|---|---|---|---|---|
| q03 | 4.7 | 3.4 | 1.4 | 1.3 |
| q07 | 11.7 | 7.7 | 5.4 | 5.0 |
| q19 | 3.9 | 4.1 | 2.4 | 2.7 |
| q42 | 3.3 | 3.3 | 1.4 | 1.4 |
| q52 | 3.0 | 3.0 | 1.3 | 1.3 |
| q55 | 2.8 | 3.0 | 1.3 | 1.2 |
| q96 | 2.1 | 1.7 | 0.8 | 0.8 |
| q09 | 13.4 | 13.2 | 6.9 | 6.6 |
| q88 | 14.5 | 14.5 | 6.4 | 6.7 |
| q29 | 12.4 | 6.9 | 12.1 | 6.8 |

- A wait of 500 ms costs nothing measurable on the other queries (differences within the run-to-run noise; q88 6.4 vs 6.7 s at 8 CPUs is the largest) and helps q03, q07 and q29.
- **Pod size matters a lot for everything except q29:** at 4 CPUs / 4 threads q09, q88 and the small star joins take about 2x as long as at 8 CPUs (q09 13.2 vs 6.6 s, q88 14.5 vs 6.7 s, q42 3.3 vs 1.4 s). The cross-engine table of section 21 was taken at 8 CPUs; at the original size Flight loses to direct on q09 (6.9 s) and q88 (5.3 s) as well as on the heaviest joins, and the section 21 wins on the small joins shrink (q42 3.3 s vs direct 3.7 s). q29's gain does not depend on pod CPU because the rows no longer cross the wire.
- At 8 CPUs q29 with the wait is 3.9-8.7 s (noisy), at 4 CPUs 4.3-7.8 s.

State left: Flight pods back at 4 CPUs / 2 CPU request / 18Gi / 4 threads with the range fix image; BEs on the new image (flag defaults on, bitset filters sent); the session variable `runtime_filter_scan_wait_time` is only set in test sessions, default still 20 ms.

## 26. All 99 TPC-DS queries: direct against Flight at two CPU splits (SR/Arrow per node)

Setup: `tpcds_sf100_asis`, cache off, one warmup and 3 repetitions per query, median of the 3, 180 s timeout, fresh Flight pods before each Flight pass, Flight scans wait 500 ms for runtime filters (`sr_client.FLIGHT_SCAN_WAIT_MS`), direct at the StarRocks default of 20 ms. BE and Flight CPU are cgroup limits per pod (3 BE pods, 3 Flight pods); DuckDB threads equal the Flight CPUs. Data: `runs/features/equal-budget.jsonl` (configs `direct-sr8`, `flight-sr8-arrow4`, `flight-sr6-arrow6`). No errors or timeouts in any pass; all 297 results of each Flight config have the hash of direct.

| config | BE CPU | Flight CPU | total CPU | sum of 99 medians | median query | geomean of flight/direct | faster / even / slower than direct (10% band) |
|---|---|---|---|---|---|---|---|
| direct | 8 | idle | 24 | 669 s | 5.6 s | | |
| Flight 8/4 (as is) | 8 | 4 | 36 | 568 s | 4.0 s | 0.83 | 53 / 19 / 27 |
| Flight 6/6 | 6 | 6 | 36 | 542 s | 3.9 s | 0.79 | 56 / 16 / 27 |

**This is not an equal-budget comparison.** Direct used the BEs at 24 CPUs and left the Flight pods idle, while both Flight configs use 36 CPUs. The two Flight rows are equal to each other (12 CPUs per node) but not to direct. An equal-budget direct run needs the BEs at 12 CPUs per pod (36 in total); the earlier request "0/8 and 4/4" (8 CPUs per node) would be direct at BE 8 against Flight at BE 4 / Flight 4. Neither has been run.

- Biggest Flight losses against direct (8/4, then 6/6): q88 14.6 / 9.2 s against 5.3 s, q58 15.9 / 11.2 against 8.6, q76 14.7 / 14.9 against 8.9, q77 18.9 / 12.3 against 13.3, q09 12.9 / 8.3 against 7.8.
- Biggest wins: q72 7.8 / 10.5 s against 44.5, q04 13.8 / 17.6 against 24.8, q71 4.0 / 4.7 against 12.0, q27 4.0 / 3.9 against 9.5, q32 0.6 / 0.7 against 5.6.
- Moving two CPUs per node from the BEs to Flight (8/4 to 6/6) helped 43 queries and hurt 25 (10% band); CPU-bound Flight queries gain (q56 15.7 to 5.6 s, q88 14.6 to 9.2 s, q09 12.9 to 8.3 s), some BE-heavy ones lose.

**FE bug found while running this.** After the 8/4 pass the next `DROP CATALOG flight` failed with `StarRocksConnectorException: Failed to close Flight client: Memory was leaked by query` (`CatalogFlightService.close`, called from `FlightConnector.shutdown`); the connector had already been removed, so every later `DROP` and query failed with `Connector of catalog 'flight' doesn't exist` and the whole 6/6 pass (297 runs) returned that error. The leak most likely comes from a join-key fetch that hit its 1000 ms limit (`FlightJoinKeyReductionRule: keys of ... took longer than 1000 ms`) and left its buffers behind. Workaround used: restart the FE (it reloads the catalog without a live connector); the failed run was repeated. Fix to do in the fork: release the client allocator buffers on a timed-out key fetch and make `close()` log the leak instead of throwing.
