| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 4.1 | | retain_off | 3.9 | +5% |
| q05 | 20.6 | | shuffle_192 | 19.4 | +6% |
| q06 | 32.5 | | bcast_256m | 7.6 | +77% |
| q09 | 14.1 | | rtf_all | 13.0 | +8% |
| q14 | 94.8 | | shuffle_48 | 89.9 | +5% |
| q16 | 31.9 | | rtf_all | 31.3 | +2% |
| q17 | 31.9 | | bcast_10m | 20.1 | +37% |
| q21 | 6.7 | | rtf_all | 5.0 | +25% |
| q22 | 10.3 | | cbo_reorder_off | 9.7 | +5% |
| q24 | 19.3 | | shuffle_48 | 18.9 | +2% |
| q25 | failed | | bcast_10m | 23.8 | fixes |
| q27 | 45.5 | | bcast_256m | 30.6 | +33% |
| q31 | 24.6 | | retain_off | 17.1 | +30% |
| q36 | 25.7 | | cbo_off | 18.9 | +26% |
| q39 | 22.2 | | rtf_all | 12.5 | +44% |
| q41 | 0.8 | | retain_off | 0.5 | +37% |
| q47 | 18.6 | | retain_off | 15.7 | +15% |
| q49 | 15.9 | | complete_agg_off | 14.8 | +7% |
| q51 | 22.1 | | reuse_bcast_only | 20.6 | +7% |
| q65 | 13.5 | | shuffle_192 | 11.9 | +12% |
| q67 | 61.5 | | shuffle_48 | 57.3 | +7% |
| q71 | 11.1 | | retain_off | 9.6 | +14% |
| q75 | 42.7 | | shuffle_48 | 38.1 | +11% |
| q80 | 63.6 | | retain_off | 61.7 | +3% |
| q81 | 13.3 | | cbo_off | 6.8 | +49% |
| q88 | 32.2 | | retain_off | 29.6 | +8% |
| q91 | 3.3 | | bcast_10m | 2.2 | +35% |
| q95 | 38.4 | | reuse_bcast_only | 37.5 | +2% |
| q97 | 22.5 | | shuffle_48 | 18.4 | +18% |

Queries where some knob beats default by more than 10% (or fixes a failure): 16 of 29

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 3 | 10 | 1 |
| bcast_256m | 6 | 3 | 0 |
| bloom_off | 3 | 4 | 0 |
| cbo_off | 5 | 15 | 1 |
| cbo_reorder_off | 7 | 6 | 0 |
| complete_agg_off | 2 | 3 | 0 |
| partial_decimal_sum_on | 2 | 10 | 1 |
| retain_off | 11 | 3 | 0 |
| reuse_bcast_only | 2 | 3 | 0 |
| rtf_all | 4 | 5 | 0 |
| shuffle_192 | 3 | 8 | 0 |
| shuffle_48 | 6 | 2 | 0 |
