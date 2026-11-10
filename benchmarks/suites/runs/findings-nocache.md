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
