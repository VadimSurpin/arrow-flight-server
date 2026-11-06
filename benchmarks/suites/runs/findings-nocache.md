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

## Open items

- The synthetic `syn_*` schemas (24 schemas x 8 queries) are not collected yet.
- TPC-DS q04 Stage B is missing; q12 TPC-H outliers unexplained; the aged-pod memory experiment is not done.
- Scale factors beyond SF100 are left for later.
- The ArrowFlight commits are pushed; the StarRocks fork commit a44e138a is still local.
