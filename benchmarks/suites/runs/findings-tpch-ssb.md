# TPC-H and SSB at SF100: Flight vs Direct, and what the knobs do

Single repetition per cell (reps 2-3 are collected afterwards and will replace these with medians), so differences under about 10% are noise. Tables: `analysis-stageA-{tpch,ssb}.md`, `analysis-stageB-{tpch,ssb}-{asis,sorted,shuffled}.md`, `analysis-stageB-{tpch,ssb}-layouts.md`.

## Stage A: Flight vs Direct (default configuration)

| suite | layout | queries in both modes | Flight/Direct total | Flight faster / slower |
|---|---|---|---|---|
| TPC-H | asis | 19 | 0.31 | 17 / 2 |
| TPC-H | sorted | 22 | 0.73 | 16 / 6 |
| TPC-H | shuffled | 22 | 0.87 | 14 / 8 |
| SSB | asis | 13 | 1.17 | 7 / 6 |
| SSB | sorted | 13 | 1.22 | 7 / 6 |
| SSB | shuffled | 13 | 0.95 | 8 / 5 |

- TPC-H asis: Direct times out on q17, q18 and q21, so the 0.31 ratio is over 19 queries only and flatters Flight.
- TPC-H queries where Flight is slower than Direct: q13 (1.2x asis, 1.4x shuffled, 2.0x sorted), q02, q11, q19 and q14 on sorted/shuffled.
- SSB: Flight loses mainly on q12 (2.9x) and q07 (2.3x), then q13 (1.6x), q11 and q04 (1.2x).

## Stage B: do the knobs matter?

Queries where some knob beats default by more than 10% (or fixes a failure):

| suite | asis | sorted | shuffled |
|---|---|---|---|
| TPC-H (22) | 16 | 6 | 9 |
| SSB (13) | 6 | 6 | 5 |

- SSB: the loss on q07 and q12 is a broadcast-join threshold problem. `bcast_256m` takes q07 from 124 s to 26 s and q12 from 117 s to 44 s (asis). It is also the best single configuration in every SSB layout and helps 4-6 queries per layout.
- TPC-H asis has by far the most tunable queries (16 of 22), but the sorted layout has only 6, so on well-clustered data the defaults are mostly fine.
- `shuffle_48` is almost never worth it (slower on 19 of 22 TPC-H and 9-10 of 13 SSB queries); `cbo_off` and `cbo_reorder_off` lose more often than they win.

## Does the best configuration depend on the layout?

| | best config identical in all 3 layouts | default best in all 3 |
|---|---|---|
| TPC-H | 5 of 22 (23%) | 3 |
| SSB | 10 of 13 (77%) | 6 |

Total wall time, seconds (default / best config per query / best single configuration):

| suite | layout | default | per-query best | best single (config) |
|---|---|---|---|---|
| TPC-H | asis | 635 | 529 | 571 (reuse_bcast_only) |
| TPC-H | sorted | 616 | 555 | 581 (reuse_bcast_only) |
| TPC-H | shuffled | 964 | 898 | 944 (bloom_off) |
| SSB | asis | 553 | 349 | 367 (bcast_256m) |
| SSB | sorted | 541 | 359 | 370 (bcast_256m) |
| SSB | shuffled | 609 | 384 | 407 (bcast_256m) |

- For a classifier this means the two suites behave differently. On SSB one rule ("raise the broadcast limit") captures nearly all of the gain and transfers across layouts. On TPC-H the per-query choice is layout-dependent and the headroom is small (6-16%), so a classifier will find little to gain there once noise is removed.
- Applying one layout's best configuration in another rarely hurts on TPC-H (1 of 6 pairs has one regressed query) and never hurts on SSB.

## Caveats and open items

- One repetition: the per-query "best" is a minimum over 13 noisy runs (winner's curse), and the default has a cold-start penalty of about 2-3%. Medians from reps 2-3 will correct this.
- SSB asis q12 has 3 invalid runs (cluster disturbance); to be rerun after the reps finish.
- `partial_decimal_sum_on` returns wrong rows on TPC-DS q06 (known bug); it still counts in these tables for timing only.
- The SSB queries are SSB-shaped queries over a TPC-H-derived star schema, not the official SSB data.
