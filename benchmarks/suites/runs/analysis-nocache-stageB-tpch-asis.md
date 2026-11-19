| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 21.7 | | retain_off | 15.0 | +31% |
| q02 | 6.9 | | rtf_all | 6.6 | +4% |
| q03 | 38.9 | | bcast_256m | 34.3 | +12% |
| q04 | 38.1 | | shuffle_48 | 30.2 | +21% |
| q06 | 14.4 | | reuse_bcast_only | 14.1 | +2% |
| q07 | 21.5 | | reuse_bcast_only | 19.3 | +10% |
| q08 | 25.9 | | shuffle_48 | 25.7 | +1% |
| q09 | 45.7 | | bloom_off | 43.8 | +4% |
| q10 | 29.6 | | default | 29.6 | +0% |
| q11 | 5.0 | | reuse_bcast_only | 4.7 | +5% |
| q12 | 23.4 | | bcast_256m | 22.1 | +6% |
| q13 | 39.4 | | shuffle_48 | 36.6 | +7% |
| q14 | 18.0 | | reuse_bcast_only | 17.8 | +1% |
| q15 | 27.5 | | rtf_all | 26.7 | +3% |
| q16 | 8.9 | | retain_off | 8.6 | +3% |
| q18 | 48.8 | | bcast_256m | 45.9 | +6% |
| q19 | 17.9 | | retain_off | 15.9 | +11% |
| q20 | 25.0 | | retain_off | 18.0 | +28% |
| q21 | 96.4 | | retain_off | 88.6 | +8% |
| q22 | 14.8 | | shuffle_48 | 14.2 | +4% |

Queries where some knob beats default by more than 10% (or fixes a failure): 6 of 20

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 0 | 10 | 0 |
| bcast_256m | 2 | 1 | 0 |
| bloom_off | 2 | 1 | 0 |
| cbo_off | 0 | 16 | 0 |
| cbo_reorder_off | 2 | 2 | 0 |
| complete_agg_off | 2 | 1 | 0 |
| partial_decimal_sum_on | 0 | 8 | 0 |
| retain_off | 3 | 1 | 0 |
| reuse_bcast_only | 2 | 0 | 0 |
| rtf_all | 3 | 1 | 0 |
| shuffle_192 | 1 | 1 | 0 |
| shuffle_48 | 2 | 1 | 0 |
