| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 13.9 | | retain_off | 13.0 | +7% |
| q02 | 13.7 | | bcast_256m | 12.9 | +6% |
| q03 | 38.3 | | bloom_off | 36.9 | +4% |
| q04 | 36.7 | | partial_decimal_sum_on | 35.9 | +2% |
| q05 | 38.3 | | bloom_off | 36.2 | +5% |
| q06 | 4.6 | | cbo_reorder_off | 4.2 | +7% |
| q07 | 20.6 | | default | 20.6 | +0% |
| q08 | 33.1 | | rtf_all | 28.2 | +15% |
| q09 | 53.7 | | cbo_off | 53.1 | +1% |
| q10 | 30.6 | | bcast_10m | 27.9 | +9% |
| q11 | 7.8 | | rtf_all | 7.1 | +9% |
| q12 | 26.0 | | bcast_256m | 14.3 | +45% |
| q13 | 66.5 | | default | 66.5 | +0% |
| q14 | 12.5 | | shuffle_192 | 10.4 | +17% |
| q15 | 15.8 | | rtf_all | 15.1 | +5% |
| q16 | 19.4 | | complete_agg_off | 17.9 | +8% |
| q17 | 34.1 | | bcast_10m | 30.0 | +12% |
| q18 | failed | | shuffle_48 | 281.1 | fixes |
| q19 | 10.8 | | partial_decimal_sum_on | 10.1 | +6% |
| q20 | 20.7 | | reuse_bcast_only | 18.0 | +13% |
| q21 | 145.5 | | retain_off | 137.9 | +5% |
| q22 | 21.4 | | bcast_256m | 20.7 | +3% |

Queries where some knob beats default by more than 10% (or fixes a failure): 6 of 22

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 2 | 3 | 0 |
| bcast_256m | 2 | 1 | 0 |
| bloom_off | 1 | 1 | 0 |
| cbo_off | 1 | 4 | 0 |
| cbo_reorder_off | 2 | 6 | 0 |
| complete_agg_off | 2 | 0 | 0 |
| partial_decimal_sum_on | 0 | 3 | 0 |
| retain_off | 2 | 2 | 0 |
| reuse_bcast_only | 1 | 0 | 0 |
| rtf_all | 2 | 1 | 0 |
| shuffle_192 | 1 | 1 | 0 |
| shuffle_48 | 0 | 19 | 1 |
