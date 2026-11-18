| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 8.8 | | cbo_reorder_off | 7.9 | +10% |
| q02 | 9.0 | | rtf_all | 8.4 | +6% |
| q03 | 38.3 | | shuffle_48 | 37.8 | +1% |
| q04 | 29.4 | | bcast_256m | 27.8 | +6% |
| q06 | 7.0 | | cbo_reorder_off | 6.9 | +1% |
| q07 | 22.7 | | shuffle_48 | 18.1 | +20% |
| q08 | 26.4 | | rtf_all | 24.1 | +9% |
| q09 | 45.6 | | complete_agg_off | 44.9 | +2% |
| q10 | 37.2 | | bcast_10m | 33.6 | +10% |
| q11 | 4.6 | | bcast_256m | 4.3 | +6% |
| q12 | 22.2 | | bcast_256m | 16.8 | +24% |
| q13 | 32.9 | | reuse_bcast_only | 31.8 | +3% |
| q14 | 14.0 | | shuffle_192 | 12.7 | +9% |
| q15 | 27.4 | | default | 27.4 | +0% |
| q16 | 13.1 | | bcast_256m | 10.6 | +19% |
| q17 | 31.8 | | bcast_10m | 29.6 | +7% |
| q19 | 8.3 | | cbo_reorder_off | 5.9 | +29% |
| q20 | 14.5 | | bcast_256m | 12.5 | +14% |
| q22 | 13.7 | | cbo_reorder_off | 13.0 | +5% |

Queries where some knob beats default by more than 10% (or fixes a failure): 6 of 19

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 0 | 8 | 0 |
| bcast_256m | 4 | 2 | 0 |
| bloom_off | 2 | 2 | 0 |
| cbo_off | 0 | 13 | 0 |
| cbo_reorder_off | 3 | 3 | 0 |
| complete_agg_off | 1 | 0 | 0 |
| partial_decimal_sum_on | 1 | 6 | 0 |
| retain_off | 1 | 1 | 0 |
| reuse_bcast_only | 2 | 3 | 0 |
| rtf_all | 2 | 1 | 0 |
| shuffle_192 | 2 | 1 | 0 |
| shuffle_48 | 4 | 3 | 0 |
