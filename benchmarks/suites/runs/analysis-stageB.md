| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 5.1 | | reuse_bcast_only | 4.3 | +15% |
| q05 | 26.5 | | bcast_256m | 24.8 | +7% |
| q06 | 42.6 | | bcast_256m | 11.7 | +72% |
| q09 | 21.8 | | retain_off | 12.7 | +42% |
| q11 | failed | | bcast_10m | 33.3 | fixes |
| q14 | 139.8 | | bcast_256m | 138.4 | +1% |
| q16 | 24.9 | | cbo_reorder_off | 24.0 | +4% |
| q17 | 35.2 | | bcast_10m | 21.4 | +39% |
| q21 | 10.2 | | rtf_all | 9.5 | +7% |
| q22 | 55.0 | | bcast_10m | 29.4 | +47% |
| q23 | 247.6 | | bcast_256m | 207.2 | +16% |
| q24 | 21.1 | | bcast_256m | 18.8 | +11% |
| q25 | 30.2 | | bloom_off | 24.9 | +17% |
| q27 | 32.3 | | bloom_off | 30.5 | +6% |
| q31 | 23.0 | | retain_off | 14.9 | +35% |
| q36 | 26.9 | | retain_off | 20.5 | +24% |
| q39 | 24.9 | | rtf_all | 18.1 | +27% |
| q41 | 1.2 | | retain_off | 0.6 | +48% |
| q47 | 32.1 | | retain_off | 22.5 | +30% |
| q49 | 19.2 | | rtf_all | 12.6 | +35% |
| q51 | 28.5 | | shuffle_192 | 26.6 | +7% |
| q57 | failed | | retain_off | 15.1 | fixes |
| q64 | 226.9 | | cbo_off | 215.8 | +5% |
| q65 | 18.3 | | complete_agg_off | 17.6 | +4% |
| q67 | 95.1 | | bcast_10m | 61.4 | +35% |
| q71 | 13.6 | | rtf_all | 11.4 | +17% |
| q72 | 37.9 | | reuse_bcast_only | 34.2 | +10% |
| q74 | failed | | cbo_off | 63.1 | fixes |
| q75 | 35.1 | | bcast_10m | 32.3 | +8% |
| q78 | failed | | shuffle_48 | 157.5 | fixes |
| q80 | 94.9 | | bcast_10m | 93.9 | +1% |
| q81 | 15.0 | | cbo_off | 9.7 | +36% |
| q82 | failed | | retain_off | 6.7 | fixes |
| q88 | 24.4 | | bcast_10m | 23.5 | +4% |
| q91 | 3.7 | | bcast_10m | 1.9 | +49% |
| q95 | 36.6 | | complete_agg_off | 30.0 | +18% |
| q97 | 22.4 | | cbo_reorder_off | 22.0 | +2% |

Queries where some knob beats default by more than 10% (or fixes a failure): 24 of 37

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 9 | 5 | 1 |
| bcast_256m | 12 | 3 | 0 |
| bloom_off | 6 | 1 | 0 |
| cbo_off | 6 | 8 | 4 |
| cbo_reorder_off | 6 | 10 | 4 |
| complete_agg_off | 5 | 5 | 0 |
| partial_decimal_sum_on | 2 | 6 | 0 |
| retain_off | 13 | 6 | 2 |
| reuse_bcast_only | 7 | 3 | 0 |
| rtf_all | 8 | 6 | 0 |
| shuffle_192 | 5 | 6 | 0 |
| shuffle_48 | 0 | 28 | 1 |
