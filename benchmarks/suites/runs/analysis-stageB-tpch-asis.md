| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 20.3 | | cbo_reorder_off | 10.0 | +51% |
| q02 | 9.3 | | bcast_256m | 8.1 | +12% |
| q03 | 37.1 | | complete_agg_off | 32.2 | +13% |
| q04 | 35.2 | | bcast_256m | 32.6 | +7% |
| q05 | 40.0 | | rtf_all | 33.1 | +17% |
| q06 | 4.6 | | rtf_all | 3.1 | +34% |
| q07 | 18.7 | | retain_off | 13.9 | +26% |
| q08 | 23.9 | | reuse_bcast_only | 22.4 | +6% |
| q09 | 50.6 | | cbo_off | 43.8 | +14% |
| q10 | 28.4 | | reuse_bcast_only | 24.0 | +15% |
| q11 | 5.6 | | complete_agg_off | 4.9 | +13% |
| q12 | 24.3 | | bcast_256m | 9.6 | +61% |
| q13 | 43.8 | | default | 43.8 | +0% |
| q14 | 13.2 | | shuffle_192 | 8.5 | +36% |
| q15 | 19.2 | | rtf_all | 13.9 | +27% |
| q16 | 10.9 | | reuse_bcast_only | 10.3 | +5% |
| q17 | 45.7 | | bcast_10m | 34.3 | +25% |
| q18 | 44.2 | | cbo_reorder_off | 43.7 | +1% |
| q19 | 10.8 | | shuffle_192 | 8.7 | +19% |
| q20 | 19.7 | | cbo_reorder_off | 13.8 | +30% |
| q21 | 110.8 | | reuse_bcast_only | 96.8 | +13% |
| q22 | 18.6 | | bcast_256m | 17.3 | +7% |

Queries where some knob beats default by more than 10% (or fixes a failure): 16 of 22

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 4 | 4 | 0 |
| bcast_256m | 8 | 2 | 0 |
| bloom_off | 11 | 0 | 0 |
| cbo_off | 5 | 5 | 0 |
| cbo_reorder_off | 10 | 2 | 0 |
| complete_agg_off | 12 | 1 | 0 |
| partial_decimal_sum_on | 1 | 10 | 0 |
| retain_off | 8 | 3 | 0 |
| reuse_bcast_only | 10 | 0 | 0 |
| rtf_all | 12 | 2 | 0 |
| shuffle_192 | 8 | 1 | 0 |
| shuffle_48 | 0 | 20 | 0 |
