| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 15.6 | | bloom_off | 13.8 | +12% |
| q02 | 12.2 | | retain_off | 10.1 | +17% |
| q03 | 4.1 | | shuffle_192 | 3.9 | +6% |
| q04 | 3.2 | | shuffle_48 | 3.1 | +4% |
| q05 | 2.0 | | shuffle_192 | 1.9 | +3% |
| q06 | 3.9 | | bloom_off | 2.0 | +47% |

Queries where some knob beats default by more than 10% (or fixes a failure): 3 of 6

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 2 | 3 | 0 |
| bcast_256m | 0 | 1 | 0 |
| bloom_off | 3 | 0 | 0 |
| cbo_off | 1 | 4 | 0 |
| cbo_reorder_off | 1 | 2 | 0 |
| complete_agg_off | 3 | 2 | 0 |
| partial_decimal_sum_on | 1 | 3 | 0 |
| retain_off | 3 | 2 | 0 |
| reuse_bcast_only | 2 | 2 | 0 |
| rtf_all | 2 | 1 | 0 |
| shuffle_192 | 2 | 0 | 0 |
| shuffle_48 | 3 | 0 | 0 |
