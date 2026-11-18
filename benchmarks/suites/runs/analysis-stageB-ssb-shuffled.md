| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 16.8 | | cbo_reorder_off | 12.6 | +25% |
| q02 | 12.1 | | complete_agg_off | 10.9 | +9% |
| q03 | 11.1 | | default | 11.1 | +0% |
| q04 | 41.3 | | bcast_256m | 33.7 | +18% |
| q05 | 38.0 | | reuse_bcast_only | 36.0 | +5% |
| q06 | 29.2 | | retain_off | 26.8 | +8% |
| q07 | 136.7 | | bcast_256m | 37.1 | +73% |
| q08 | 28.0 | | retain_off | 25.9 | +8% |
| q09 | 32.4 | | complete_agg_off | 26.9 | +17% |
| q10 | 28.2 | | complete_agg_off | 25.7 | +9% |
| q11 | 54.5 | | bcast_256m | 46.4 | +15% |
| q12 | 136.8 | | bcast_256m | 48.4 | +65% |
| q13 | 44.2 | | rtf_all | 42.9 | +3% |

Queries where some knob beats default by more than 10% (or fixes a failure): 6 of 13

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 1 | 5 | 0 |
| bcast_256m | 6 | 1 | 0 |
| bloom_off | 2 | 4 | 0 |
| cbo_off | 3 | 6 | 0 |
| cbo_reorder_off | 3 | 6 | 0 |
| complete_agg_off | 2 | 1 | 0 |
| partial_decimal_sum_on | 2 | 1 | 0 |
| retain_off | 2 | 1 | 0 |
| reuse_bcast_only | 2 | 2 | 0 |
| rtf_all | 2 | 3 | 0 |
| shuffle_192 | 2 | 3 | 0 |
| shuffle_48 | 1 | 9 | 0 |
