| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 15.1 | | retain_off | 12.9 | +14% |
| q02 | 12.8 | | bcast_256m | 11.7 | +8% |
| q03 | 12.5 | | rtf_all | 11.5 | +8% |
| q04 | 42.9 | | bcast_256m | 34.6 | +19% |
| q05 | 37.1 | | default | 37.1 | +0% |
| q06 | 27.6 | | rtf_all | 25.4 | +8% |
| q07 | 99.1 | | bcast_256m | 26.9 | +73% |
| q08 | 23.5 | | bcast_256m | 22.0 | +7% |
| q09 | 25.1 | | rtf_all | 23.1 | +8% |
| q10 | 23.8 | | reuse_bcast_only | 22.6 | +5% |
| q11 | 52.0 | | bcast_256m | 44.3 | +15% |
| q12 | 127.2 | | bcast_256m | 46.5 | +63% |
| q13 | 42.1 | | rtf_all | 40.1 | +5% |

Queries where some knob beats default by more than 10% (or fixes a failure): 5 of 13

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 1 | 6 | 0 |
| bcast_256m | 5 | 1 | 0 |
| bloom_off | 0 | 2 | 0 |
| cbo_off | 1 | 5 | 0 |
| cbo_reorder_off | 2 | 5 | 0 |
| complete_agg_off | 0 | 3 | 0 |
| partial_decimal_sum_on | 0 | 1 | 0 |
| retain_off | 1 | 4 | 0 |
| reuse_bcast_only | 1 | 3 | 0 |
| rtf_all | 0 | 3 | 0 |
| shuffle_192 | 0 | 4 | 0 |
| shuffle_48 | 1 | 11 | 0 |
