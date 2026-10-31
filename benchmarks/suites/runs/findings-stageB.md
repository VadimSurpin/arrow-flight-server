# Stage B: one-factor-at-a-time knob changes, TPC-DS SF100 `asis`, Flight mode

37 queries (the ones where Flight took more than 20 s, failed, or lost to Direct in Stage A), default plus 11 variations of `configs/phase1.json`, one repetition each, driver restarted per query. Per-query table and per-knob counts: `analysis-stageB.md` (made by `analyze_knobs.py`). Raw runs: `tpcds-asis-phase1.jsonl`.

## Results

- On 24 of 37 queries some knob beats the default by more than 10% or turns a failure into a result. A classifier has something to learn here.
- No single knob wins everywhere. Faster/slower counts (more than 10%): `retain_off` 13/6, `bcast_256m` 12/3, `bcast_10m` 9/5, `rtf_all` 8/6, `reuse_bcast_only` 7/3, `bloom_off` 6/1, `cbo_off` 6/8, `cbo_reorder_off` 6/10, `shuffle_192` 5/6, `complete_agg_off` 5/5.
- `shuffle_48` never helps (0 faster, 28 slower): fewer partitions are bad for these queries.
- Queries failing under the default (broadcast out of memory): q11 completes with `bcast_10m`, q57 and q82 with `retain_off`, q74 with `cbo_off`, q78 with `shuffle_48`. `cbo_off` and `cbo_reorder_off` each fix four of the five failures that were run, consistent with the Stage A finding that CBO join reordering puts large fact joins on the broadcast side.
- `partial_decimal_sum_on` returns wrong rows for q06 (0 against 52, hash `e3b0c442…`): this is the known overflow when a partial SUM is cast back to decimal(7,2), which the default now avoids. Treat those runs as a labelled incorrect-result case, not as a speedup.
- Largest gains: q06 `bcast_256m` 42.6 s to 11.7 s, q91 `bcast_10m` 3.7 to 1.9, q22 `bcast_10m` 55 to 29, q17 `bcast_10m` 35 to 21, q67 `bcast_10m` 95 to 61, q09 `retain_off` 22 to 13, q81 `cbo_off` 15 to 9.7.

## Caveats

- Single repetition per cell, no noise estimate. Gains under about 15% and sub-5 s queries are within noise, and the q06 default (42.6 s) is far above its 2-4 s in Stage A, so that gain is probably default noise.
- q04: the Stage B runs were dropped as cluster-invalid (driver and connection errors after repeated timeouts). In the rerun only `cbo_off` completed (137 s); the other settings hit the broadcast out-of-memory error. These records are not in the file; rerun q04 if it matters.
- q01–q97 timeouts were 300 s. Q14, q23, q64, q67 and q78 are close to it, so their best/default ratios are truncated.

## Sorted and shuffled layouts, and across layouts

Same 37-query sweep on `sorted` and `shuffled` (`stageB2.sh`, reruns `redoB3.sh`). Tables: `analysis-stageB-asis.md`, `-sorted.md`, `-shuffled.md` (per query and per knob), `analysis-stageB-layouts.md` (cross-layout, made by `analyze_layouts.py`). q04 is only in no layout (excluded), q74 on `shuffled` was dropped as cluster-invalid on both attempts, so 36 queries are compared.

Queries where some knob beats default by more than 10% or fixes a failure: asis 24 of 37, sorted 27 of 37, shuffled 23 of 36.

### Per knob, faster/slower (more than 10%)

| knob | asis | sorted | shuffled |
|---|---|---|---|
| retain_off | 13/6 | 13/4 | 13/4 |
| bcast_10m | 9/5 | 10/5 | 11/3 |
| bcast_256m | 12/3 | 10/3 | 9/2 |
| reuse_bcast_only | 7/3 | 10/4 | 10/2 |
| rtf_all | 8/6 | 9/4 | 9/5 |
| bloom_off | 6/1 | 5/3 | 6/0 |
| complete_agg_off | 5/5 | 12/3 | 9/5 |
| cbo_off | 6/8 | 5/11 | 9/11 |
| cbo_reorder_off | 6/10 | 5/11 | 7/10 |
| shuffle_192 | 5/6 | 7/4 | 9/6 |
| shuffle_48 | 0/28 | 2/26 | 1/29 |

`retain_off`, the broadcast limits, `reuse_bcast_only` and `bloom_off` behave the same in every layout. `complete_agg_off` is the one that depends on the layout: it helps on 5 queries when the keys are clustered as generated and on 12 when sorted. `shuffle_48` never helps.

### Does the best configuration depend on the layout?

- The best configuration (fastest, if it beats default by more than 10%) is identical in all three layouts for 18 of 36 queries (50%). On 6 queries default is best everywhere. Among the 30 queries where any layout wants a non-default configuration, 12 agree.
- A layout's best configuration is rarely harmful in another: applied elsewhere it is more than 10% slower than default on 0-1 queries per layout pair, more than 10% faster on 18-21, and flat on 3-6. So most of the gain transfers, but the best choice does change on half the queries (for example q09: retain_off / complete_agg_off / bcast_10m).
- Headroom, total wall over the 36 queries (failed default counted as 300 s): default 2672 / 2473 / 2954 s, best configuration per query 1439 / 1364 / 1821 s (about 45% less), best single configuration for all queries 2299 s (`cbo_off`) / 2190 s (`bcast_10m`) / 2556 s (`cbo_off`) (about 14% less). The per-query choice is worth roughly three times as much as the best single fixed configuration. This is an upper bound: it picks the minimum of 12 noisy single runs.

### Caveats

- One repetition per cell, so the best-of-12 selection is optimistic (winner's curse), and small differences between configurations are noise. The default ran slightly earlier than the others within a query and a fresh driver (median default / median of the others: 1.02-1.03), a small cold-start bias against default.
- Failures (broadcast out of memory) on q04, q11, q57, q74, q82 and q78 are the same in all layouts except where a knob fixes them (`retain_off`, `cbo_off`, `bcast_10m`, `shuffle_48`; see per-layout tables).
- q74 `shuffled` and q04 are missing; q78 on `shuffled` has no result under any configuration (300 s timeouts).
