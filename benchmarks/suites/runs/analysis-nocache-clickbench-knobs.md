| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 0.8 | | retain_off | 0.4 | +54% |
| q02 | 0.4 | | retain_off | 0.3 | +11% |
| q03 | 0.9 | | complete_agg_off | 0.8 | +6% |
| q04 | 1.3 | | bloom_off | 0.8 | +38% |
| q05 | 3.6 | | retain_off | 3.5 | +3% |
| q06 | 3.3 | | reuse_bcast_only | 3.1 | +5% |
| q07 | 1.0 | | complete_agg_off | 0.9 | +8% |
| q08 | 0.5 | | reuse_bcast_only | 0.5 | +9% |
| q09 | 4.6 | | shuffle_192 | 4.5 | +2% |
| q10 | 6.0 | | shuffle_48 | 5.9 | +2% |
| q11 | 1.5 | | cbo_reorder_off | 1.4 | +10% |
| q12 | 1.7 | | complete_agg_off | 1.5 | +13% |
| q13 | 3.1 | | retain_off | 2.9 | +7% |
| q14 | 5.6 | | rtf_all | 5.5 | +2% |
| q15 | 3.3 | | shuffle_48 | 3.0 | +9% |
| q16 | 3.5 | | shuffle_192 | 3.5 | +2% |
| q17 | 6.7 | | shuffle_48 | 6.6 | +1% |
| q18 | 5.5 | | shuffle_48 | 5.3 | +4% |
| q20 | 0.4 | | cbo_reorder_off | 0.3 | +7% |
| q21 | 7.0 | | shuffle_192 | 5.5 | +22% |
| q22 | 6.1 | | reuse_bcast_only | 5.7 | +6% |
| q23 | 11.2 | | shuffle_192 | 10.7 | +5% |
| q24 | 55.7 | | bcast_256m | 43.9 | +21% |
| q25 | 1.7 | | partial_decimal_sum_on | 1.6 | +9% |
| q26 | 1.3 | | reuse_bcast_only | 1.2 | +6% |
| q27 | 1.6 | | reuse_bcast_only | 1.6 | +3% |
| q28 | 6.0 | | default | 6.0 | +0% |
| q29 | 22.5 | | retain_off | 22.2 | +1% |
| q30 | 19.0 | | bloom_off | 18.6 | +2% |
| q31 | 5.1 | | cbo_reorder_off | 2.7 | +47% |
| q32 | 4.4 | | retain_off | 3.8 | +13% |
| q33 | 15.1 | | shuffle_48 | 14.5 | +4% |
| q34 | 11.4 | | complete_agg_off | 11.1 | +3% |
| q35 | 13.2 | | shuffle_48 | 10.8 | +18% |
| q36 | 4.7 | | shuffle_48 | 4.3 | +8% |

Queries where some knob beats default by more than 10% (or fixes a failure): 10 of 35

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 5 | 6 | 0 |
| bcast_256m | 5 | 4 | 0 |
| bloom_off | 3 | 5 | 0 |
| cbo_off | 3 | 18 | 0 |
| cbo_reorder_off | 4 | 3 | 0 |
| complete_agg_off | 7 | 7 | 0 |
| partial_decimal_sum_on | 4 | 4 | 0 |
| retain_off | 6 | 3 | 0 |
| reuse_bcast_only | 5 | 3 | 0 |
| rtf_all | 2 | 8 | 0 |
| shuffle_192 | 5 | 6 | 0 |
| shuffle_48 | 5 | 5 | 0 |
