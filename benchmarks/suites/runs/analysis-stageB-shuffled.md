| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 6.2 | | retain_off | 4.9 | +21% |
| q05 | 27.9 | | retain_off | 24.8 | +11% |
| q06 | 41.0 | | bcast_256m | 10.8 | +74% |
| q09 | 20.5 | | bcast_10m | 11.5 | +44% |
| q11 | failed | | bcast_10m | 46.0 | fixes |
| q14 | 147.6 | | cbo_reorder_off | 141.1 | +4% |
| q16 | 31.3 | | default | 31.3 | +0% |
| q17 | 37.7 | | bcast_10m | 23.4 | +38% |
| q21 | 10.4 | | reuse_bcast_only | 9.7 | +6% |
| q22 | 58.1 | | bcast_10m | 28.0 | +52% |
| q23 | 276.1 | | retain_off | 270.2 | +2% |
| q24 | 27.9 | | bcast_256m | 24.0 | +14% |
| q25 | 35.1 | | bcast_10m | 33.5 | +4% |
| q27 | 35.5 | | bcast_256m | 35.0 | +1% |
| q31 | 35.8 | | cbo_off | 23.4 | +35% |
| q36 | 50.4 | | retain_off | 45.2 | +10% |
| q39 | 37.1 | | rtf_all | 21.3 | +43% |
| q41 | 3.7 | | rtf_all | 0.6 | +83% |
| q47 | 35.9 | | retain_off | 24.1 | +33% |
| q49 | 22.8 | | shuffle_192 | 12.7 | +45% |
| q51 | 30.3 | | shuffle_192 | 27.0 | +11% |
| q57 | failed | | retain_off | 16.3 | fixes |
| q64 | 286.1 | | cbo_off | 246.6 | +14% |
| q65 | 18.4 | | reuse_bcast_only | 17.3 | +6% |
| q67 | 101.7 | | bcast_10m | 63.4 | +38% |
| q71 | 17.5 | | rtf_all | 11.6 | +34% |
| q72 | 85.4 | | retain_off | 82.1 | +4% |
| q75 | 65.7 | | rtf_all | 38.6 | +41% |
| q78 | failed | | - | - | still fails |
| q80 | 99.3 | | shuffle_192 | 94.8 | +5% |
| q81 | 16.7 | | cbo_off | 7.1 | +58% |
| q82 | failed | | retain_off | 8.9 | fixes |
| q88 | 25.9 | | shuffle_192 | 24.8 | +4% |
| q91 | 2.2 | | reuse_bcast_only | 1.8 | +19% |
| q95 | 38.7 | | reuse_bcast_only | 36.2 | +6% |
| q97 | 25.2 | | rtf_all | 23.1 | +8% |

Queries where some knob beats default by more than 10% (or fixes a failure): 23 of 36

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 11 | 3 | 1 |
| bcast_256m | 9 | 2 | 0 |
| bloom_off | 6 | 0 | 0 |
| cbo_off | 9 | 11 | 3 |
| cbo_reorder_off | 7 | 10 | 3 |
| complete_agg_off | 9 | 5 | 0 |
| partial_decimal_sum_on | 3 | 6 | 0 |
| retain_off | 13 | 4 | 2 |
| reuse_bcast_only | 10 | 2 | 0 |
| rtf_all | 9 | 5 | 0 |
| shuffle_192 | 9 | 6 | 0 |
| shuffle_48 | 1 | 29 | 0 |
