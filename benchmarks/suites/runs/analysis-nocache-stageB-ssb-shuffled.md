| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 5.3 | | bcast_256m | 4.9 | +7% |
| q02 | 3.8 | | shuffle_192 | 3.7 | +3% |
| q03 | 3.8 | | cbo_reorder_off | 3.8 | +1% |
| q04 | 23.3 | | bcast_256m | 17.2 | +26% |
| q05 | 19.3 | | partial_decimal_sum_on | 15.6 | +19% |
| q06 | 16.0 | | reuse_bcast_only | 13.6 | +15% |
| q08 | 12.4 | | retain_off | 12.0 | +3% |
| q09 | 15.7 | | complete_agg_off | 12.8 | +18% |
| q10 | 12.8 | | complete_agg_off | 12.2 | +5% |
| q11 | 31.5 | | bcast_256m | 24.9 | +21% |
| q13 | 20.7 | | rtf_all | 19.9 | +4% |

Queries where some knob beats default by more than 10% (or fixes a failure): 5 of 11

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 0 | 7 | 0 |
| bcast_256m | 3 | 2 | 0 |
| bloom_off | 1 | 3 | 0 |
| cbo_off | 0 | 11 | 0 |
| cbo_reorder_off | 1 | 6 | 0 |
| complete_agg_off | 3 | 1 | 0 |
| partial_decimal_sum_on | 1 | 5 | 0 |
| retain_off | 2 | 2 | 0 |
| reuse_bcast_only | 2 | 1 | 0 |
| rtf_all | 2 | 1 | 0 |
| shuffle_192 | 2 | 0 | 0 |
| shuffle_48 | 2 | 1 | 0 |
