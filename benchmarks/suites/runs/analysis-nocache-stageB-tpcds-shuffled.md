| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 4.3 | | shuffle_48 | 4.1 | +5% |
| q05 | 29.3 | | partial_decimal_sum_on | 27.3 | +7% |
| q06 | 33.5 | | bcast_256m | 10.2 | +69% |
| q09 | 13.3 | | reuse_bcast_only | 12.7 | +5% |
| q16 | 31.3 | | shuffle_48 | 30.4 | +3% |
| q17 | 34.2 | | bcast_10m | 20.9 | +39% |
| q21 | 7.8 | | bloom_off | 5.7 | +27% |
| q22 | 22.8 | | bcast_10m | 18.3 | +20% |
| q24 | 23.4 | | complete_agg_off | 20.2 | +13% |
| q25 | 36.8 | | partial_decimal_sum_on | 36.1 | +2% |
| q27 | 27.4 | | shuffle_192 | 26.8 | +2% |
| q31 | 24.0 | | cbo_off | 20.4 | +15% |
| q36 | 30.5 | | bcast_10m | 27.9 | +8% |
| q39 | 24.2 | | cbo_off | 15.7 | +35% |
| q41 | 0.8 | | retain_off | 0.6 | +27% |
| q47 | 36.4 | | retain_off | 25.9 | +29% |
| q49 | 25.2 | | retain_off | 19.1 | +24% |
| q51 | 28.0 | | rtf_all | 27.3 | +2% |
| q65 | 17.7 | | bcast_256m | 17.4 | +2% |
| q67 | 78.0 | | bcast_10m | 61.8 | +21% |
| q71 | 12.6 | | reuse_bcast_only | 11.4 | +10% |
| q75 | 45.6 | | bcast_10m | 42.9 | +6% |
| q80 | 103.8 | | bloom_off | 102.3 | +1% |
| q81 | 14.1 | | cbo_off | 7.4 | +47% |
| q88 | 30.6 | | partial_decimal_sum_on | 28.8 | +6% |
| q91 | 2.0 | | bloom_off | 1.5 | +26% |
| q95 | 38.7 | | complete_agg_off | 38.1 | +1% |
| q97 | 30.6 | | rtf_all | 29.4 | +4% |

Queries where some knob beats default by more than 10% (or fixes a failure): 13 of 28

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 3 | 4 | 0 |
| bcast_256m | 2 | 4 | 0 |
| bloom_off | 3 | 4 | 0 |
| cbo_off | 4 | 14 | 0 |
| cbo_reorder_off | 3 | 10 | 0 |
| complete_agg_off | 4 | 7 | 0 |
| partial_decimal_sum_on | 1 | 4 | 0 |
| retain_off | 8 | 5 | 0 |
| reuse_bcast_only | 3 | 5 | 0 |
| rtf_all | 2 | 5 | 0 |
| shuffle_192 | 3 | 8 | 0 |
| shuffle_48 | 5 | 2 | 0 |
