| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 6.5 | | retain_off | 5.0 | +24% |
| q05 | 23.2 | | reuse_bcast_only | 19.7 | +15% |
| q06 | 41.7 | | bcast_256m | 12.1 | +71% |
| q09 | 26.0 | | complete_agg_off | 12.1 | +53% |
| q11 | failed | | bcast_10m | 48.3 | fixes |
| q14 | 92.6 | | rtf_all | 89.9 | +3% |
| q16 | 32.3 | | bcast_256m | 30.5 | +6% |
| q17 | 35.8 | | bcast_10m | 21.0 | +41% |
| q21 | 5.4 | | bcast_256m | 5.1 | +6% |
| q22 | 25.8 | | bcast_10m | 18.5 | +28% |
| q23 | 185.8 | | bcast_10m | 176.1 | +5% |
| q24 | 24.3 | | bcast_256m | 21.2 | +13% |
| q25 | 25.1 | | bcast_256m | 25.0 | +0% |
| q27 | 36.2 | | rtf_all | 32.8 | +10% |
| q31 | 27.4 | | retain_off | 18.4 | +33% |
| q36 | 26.8 | | retain_off | 21.2 | +21% |
| q39 | 30.4 | | rtf_all | 16.4 | +46% |
| q41 | 1.0 | | rtf_all | 0.6 | +37% |
| q47 | 19.1 | | retain_off | 16.6 | +13% |
| q49 | 21.4 | | complete_agg_off | 16.6 | +22% |
| q51 | 23.6 | | shuffle_192 | 22.2 | +6% |
| q57 | failed | | retain_off | 11.5 | fixes |
| q64 | 199.0 | | shuffle_48 | 175.8 | +12% |
| q65 | 14.7 | | complete_agg_off | 12.8 | +13% |
| q67 | 85.8 | | bcast_10m | 67.5 | +21% |
| q71 | 13.7 | | retain_off | 9.9 | +28% |
| q72 | 26.9 | | reuse_bcast_only | 21.3 | +21% |
| q74 | failed | | cbo_off | 69.7 | fixes |
| q75 | 49.6 | | cbo_reorder_off | 34.3 | +31% |
| q78 | failed | | shuffle_48 | 241.9 | fixes |
| q80 | 63.1 | | complete_agg_off | 60.2 | +5% |
| q81 | 15.2 | | cbo_off | 6.8 | +56% |
| q82 | failed | | retain_off | 6.8 | fixes |
| q88 | 24.7 | | retain_off | 23.0 | +7% |
| q91 | 3.5 | | bcast_10m | 2.3 | +34% |
| q95 | 38.6 | | bcast_10m | 36.6 | +5% |
| q97 | 27.5 | | shuffle_192 | 24.5 | +11% |

Queries where some knob beats default by more than 10% (or fixes a failure): 27 of 37

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 10 | 5 | 1 |
| bcast_256m | 10 | 3 | 0 |
| bloom_off | 5 | 3 | 0 |
| cbo_off | 5 | 11 | 4 |
| cbo_reorder_off | 5 | 11 | 4 |
| complete_agg_off | 12 | 3 | 0 |
| partial_decimal_sum_on | 1 | 8 | 0 |
| retain_off | 13 | 4 | 2 |
| reuse_bcast_only | 10 | 4 | 0 |
| rtf_all | 9 | 4 | 0 |
| shuffle_192 | 7 | 4 | 0 |
| shuffle_48 | 2 | 26 | 1 |
