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
