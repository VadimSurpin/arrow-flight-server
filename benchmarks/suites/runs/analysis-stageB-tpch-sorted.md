| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 11.7 | | retain_off | 10.2 | +13% |
| q02 | 9.8 | | shuffle_192 | 8.8 | +10% |
| q03 | 31.8 | | bcast_256m | 30.4 | +4% |
| q04 | 35.9 | | rtf_all | 34.4 | +4% |
| q05 | 33.3 | | rtf_all | 32.3 | +3% |
| q06 | 3.7 | | rtf_all | 3.2 | +14% |
| q07 | 17.1 | | rtf_all | 15.6 | +9% |
| q08 | 23.3 | | bcast_10m | 22.8 | +2% |
| q09 | 46.0 | | default | 46.0 | +0% |
| q10 | 26.6 | | cbo_reorder_off | 22.9 | +14% |
| q11 | 5.6 | | reuse_bcast_only | 5.2 | +7% |
| q12 | 20.4 | | bcast_256m | 10.8 | +47% |
| q13 | 55.0 | | bloom_off | 54.0 | +2% |
| q14 | 10.7 | | rtf_all | 8.8 | +17% |
| q15 | 15.4 | | rtf_all | 14.1 | +9% |
| q16 | 13.9 | | cbo_reorder_off | 12.3 | +12% |
| q17 | 33.5 | | bcast_10m | 28.2 | +16% |
| q18 | 42.8 | | bloom_off | 41.4 | +3% |
| q19 | 11.6 | | rtf_all | 10.7 | +7% |
| q20 | 20.1 | | cbo_reorder_off | 18.1 | +10% |
| q21 | 126.0 | | retain_off | 103.7 | +18% |
| q22 | 21.5 | | reuse_bcast_only | 20.6 | +4% |

Queries where some knob beats default by more than 10% (or fixes a failure): 9 of 22

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 2 | 6 | 0 |
| bcast_256m | 1 | 3 | 0 |
| bloom_off | 1 | 1 | 0 |
| cbo_off | 2 | 5 | 0 |
| cbo_reorder_off | 4 | 2 | 0 |
| complete_agg_off | 2 | 1 | 0 |
| partial_decimal_sum_on | 0 | 3 | 0 |
| retain_off | 5 | 6 | 0 |
| reuse_bcast_only | 4 | 0 | 0 |
| rtf_all | 5 | 3 | 0 |
| shuffle_192 | 2 | 4 | 0 |
| shuffle_48 | 0 | 20 | 0 |
