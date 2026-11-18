| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 5.1 | | bcast_256m | 4.8 | +7% |
| q02 | 3.8 | | complete_agg_off | 3.7 | +2% |
| q03 | 4.1 | | rtf_all | 4.0 | +1% |
| q04 | 24.1 | | bcast_256m | 17.9 | +26% |
| q05 | 18.6 | | complete_agg_off | 15.8 | +15% |
| q06 | 20.5 | | retain_off | 19.0 | +8% |
| q07 | 55.7 | | bcast_256m | 13.9 | +75% |
| q08 | 11.3 | | cbo_reorder_off | 11.1 | +2% |
| q09 | 12.2 | | shuffle_48 | 11.6 | +5% |
| q10 | 12.6 | | bloom_off | 11.2 | +11% |
| q11 | 29.4 | | bcast_256m | 23.6 | +20% |
| q13 | 22.6 | | reuse_bcast_only | 19.9 | +12% |

Queries where some knob beats default by more than 10% (or fixes a failure): 6 of 12

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 0 | 7 | 0 |
| bcast_256m | 3 | 2 | 0 |
| bloom_off | 2 | 0 | 0 |
| cbo_off | 0 | 11 | 0 |
| cbo_reorder_off | 0 | 4 | 0 |
| complete_agg_off | 1 | 1 | 0 |
| partial_decimal_sum_on | 0 | 6 | 0 |
| retain_off | 2 | 2 | 0 |
| reuse_bcast_only | 1 | 1 | 0 |
| rtf_all | 2 | 1 | 0 |
| shuffle_192 | 1 | 2 | 0 |
| shuffle_48 | 3 | 1 | 0 |
