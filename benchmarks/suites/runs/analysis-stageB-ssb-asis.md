| query | default (s) | | best config | best (s) | gain |
|---|---|---|---|---|---|
| q01 | 15.9 | | rtf_all | 12.5 | +21% |
| q02 | 12.6 | | cbo_off | 12.2 | +3% |
| q03 | 11.8 | | bloom_off | 10.0 | +15% |
| q04 | 42.6 | | bcast_256m | 32.1 | +25% |
| q05 | 35.1 | | default | 35.1 | +0% |
| q06 | 26.4 | | rtf_all | 26.2 | +0% |
| q07 | 124.3 | | bcast_256m | 26.0 | +79% |
| q08 | 23.8 | | cbo_reorder_off | 22.1 | +7% |
| q09 | 23.8 | | rtf_all | 22.6 | +5% |
| q10 | 23.5 | | reuse_bcast_only | 22.7 | +3% |
| q11 | 53.9 | | bcast_256m | 43.6 | +19% |
| q12 | 117.2 | | bcast_256m | 43.8 | +63% |
| q13 | 42.3 | | complete_agg_off | 40.4 | +4% |

Queries where some knob beats default by more than 10% (or fixes a failure): 6 of 13

| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |
|---|---|---|---|
| bcast_10m | 1 | 5 | 0 |
| bcast_256m | 4 | 1 | 0 |
| bloom_off | 2 | 2 | 0 |
| cbo_off | 1 | 5 | 0 |
| cbo_reorder_off | 2 | 5 | 0 |
| complete_agg_off | 1 | 1 | 0 |
| retain_off | 1 | 1 | 0 |
| reuse_bcast_only | 1 | 2 | 0 |
| rtf_all | 1 | 3 | 0 |
| shuffle_192 | 1 | 1 | 0 |
| shuffle_48 | 1 | 10 | 0 |
