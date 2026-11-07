| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 5.0 | | complete_agg_off | 4.9 | +4% |
| q02 | 10.4 | | default | 10.4 | +0% |
| q03 | 4.2 | | cbo_reorder_off | 3.9 | +8% |
| q04 | 23.6 | | bcast_256m | 15.9 | +33% |
| q05 | 17.9 | | bcast_256m | 15.0 | +16% |
| q06 | 14.2 | | reuse_bcast_only | 11.6 | +18% |
| q08 | 12.9 | | bloom_off | 9.9 | +24% |
| q09 | 12.2 | | retain_off | 10.6 | +13% |
| q10 | 12.3 | | bcast_256m | 10.3 | +16% |
| q11 | 31.9 | | bcast_256m | 23.9 | +25% |
| q12 | 87.0 | | bcast_256m | 23.1 | +73% |
| q13 | 22.3 | | shuffle_48 | 17.2 | +23% |

Queries where some knob beats default by more than 10% (or fixes a failure): 9 of 12

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 0 | 7 | 0 |
| bcast_256m | 7 | 0 | 0 |
| bloom_off | 2 | 1 | 0 |
| cbo_off | 1 | 11 | 0 |
| cbo_reorder_off | 2 | 5 | 0 |
| complete_agg_off | 2 | 2 | 0 |
| partial_decimal_sum_on | 3 | 3 | 0 |
| retain_off | 6 | 0 | 0 |
| reuse_bcast_only | 4 | 1 | 0 |
| rtf_all | 1 | 1 | 0 |
| shuffle_192 | 4 | 3 | 0 |
| shuffle_48 | 4 | 0 | 0 |
