| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 4.1 | | shuffle_48 | 3.9 | +7% |
| q05 | 26.4 | | reuse_bcast_only | 25.2 | +5% |
| q06 | 26.7 | | bcast_256m | 7.0 | +74% |
| q09 | 14.3 | | reuse_bcast_only | 12.3 | +14% |
| q16 | 24.8 | | reuse_bcast_only | 24.2 | +2% |
| q17 | failed | | bcast_10m | 17.0 | fixes |
| q21 | 7.2 | | bcast_256m | 4.9 | +32% |
| q22 | 24.9 | | bcast_10m | 22.7 | +9% |
| q24 | 16.6 | | bcast_256m | 15.2 | +8% |
| q25 | failed | | bcast_10m | 26.2 | fixes |
| q27 | 30.9 | | bcast_10m | 27.4 | +11% |
| q31 | 21.2 | | retain_off | 13.7 | +35% |
| q36 | 24.6 | | cbo_off | 18.2 | +26% |
| q39 | 22.2 | | cbo_reorder_off | 14.3 | +36% |
| q41 | 0.8 | | retain_off | 0.5 | +31% |
| q47 | 27.5 | | retain_off | 23.3 | +15% |
| q49 | 20.6 | | retain_off | 14.3 | +30% |
| q51 | 31.5 | | shuffle_192 | 26.7 | +15% |
| q65 | 16.4 | | retain_off | 15.4 | +6% |
| q67 | 76.2 | | bcast_10m | 61.6 | +19% |
| q71 | 13.9 | | shuffle_48 | 9.4 | +32% |
| q75 | 32.1 | | shuffle_48 | 29.8 | +7% |
| q80 | 98.4 | | cbo_reorder_off | 97.4 | +1% |
| q81 | 13.6 | | cbo_off | 7.8 | +42% |
| q88 | 22.6 | | bloom_off | 22.6 | +0% |
| q91 | 3.1 | | bcast_10m | 1.9 | +40% |
| q95 | 32.0 | | complete_agg_off | 31.7 | +1% |
| q97 | 23.0 | | shuffle_48 | 22.3 | +3% |

Queries where some knob beats default by more than 10% (or fixes a failure): 17 of 28

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 4 | 6 | 2 |
| bcast_256m | 5 | 0 | 0 |
| bloom_off | 5 | 0 | 0 |
| cbo_off | 4 | 8 | 2 |
| cbo_reorder_off | 5 | 4 | 0 |
| complete_agg_off | 3 | 2 | 0 |
| partial_decimal_sum_on | 1 | 7 | 2 |
| retain_off | 11 | 2 | 0 |
| reuse_bcast_only | 4 | 1 | 0 |
| rtf_all | 5 | 2 | 0 |
| shuffle_192 | 6 | 4 | 0 |
| shuffle_48 | 5 | 0 | 0 |
