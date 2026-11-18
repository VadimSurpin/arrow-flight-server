| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 7.2 | | rtf_all | 7.2 | +1% |
| q02 | 6.8 | | cbo_reorder_off | 6.6 | +2% |
| q03 | 28.1 | | bcast_256m | 26.2 | +7% |
| q04 | 29.6 | | shuffle_48 | 23.1 | +22% |
| q06 | 3.0 | | default | 3.0 | +0% |
| q07 | 15.7 | | shuffle_48 | 11.8 | +25% |
| q08 | 19.6 | | rtf_all | 17.4 | +11% |
| q09 | 48.5 | | complete_agg_off | 39.8 | +18% |
| q10 | 21.1 | | shuffle_48 | 18.5 | +12% |
| q11 | 4.1 | | shuffle_48 | 3.9 | +6% |
| q12 | 13.5 | | bcast_256m | 8.8 | +35% |
| q13 | 34.1 | | cbo_off | 31.9 | +7% |
| q14 | 11.2 | | complete_agg_off | 8.6 | +23% |
| q15 | 16.5 | | retain_off | 13.0 | +21% |
| q16 | 8.2 | | shuffle_192 | 8.0 | +3% |
| q17 | 33.8 | | bcast_10m | 31.3 | +7% |
| q18 | 41.0 | | cbo_reorder_off | 40.3 | +2% |
| q19 | 7.6 | | shuffle_192 | 6.4 | +16% |
| q20 | 19.6 | | bcast_256m | 17.5 | +11% |
| q22 | 13.3 | | cbo_reorder_off | 12.7 | +4% |

Queries where some knob beats default by more than 10% (or fixes a failure): 10 of 20

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 1 | 8 | 0 |
| bcast_256m | 6 | 1 | 0 |
| bloom_off | 4 | 0 | 0 |
| cbo_off | 2 | 15 | 0 |
| cbo_reorder_off | 3 | 3 | 0 |
| complete_agg_off | 4 | 2 | 0 |
| partial_decimal_sum_on | 2 | 5 | 0 |
| retain_off | 4 | 4 | 0 |
| reuse_bcast_only | 4 | 1 | 0 |
| rtf_all | 5 | 2 | 0 |
| shuffle_192 | 2 | 2 | 0 |
| shuffle_48 | 4 | 0 | 0 |
