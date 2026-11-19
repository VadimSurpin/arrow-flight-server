Queries with a default result in all three layouts: 11

- best configuration identical in all three layouts: 5 of 11 (45%); of the 8 queries where some layout wants a non-default configuration: 2 identical
- default is best in all three layouts: 3

| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |
|---|---|---|---|---|
| asis | sorted | 1 | 2 | 5 |
| asis | shuffled | 2 | 3 | 3 |
| sorted | asis | 0 | 4 | 1 |
| sorted | shuffled | 0 | 3 | 2 |
| shuffled | asis | 0 | 5 | 0 |
| shuffled | sorted | 0 | 2 | 3 |

Queries whose best configuration differs between layouts (up to 40):

| query | asis | sorted | shuffled |
|---|---|---|---|
| q05 | bcast_256m | complete_agg_off | partial_decimal_sum_on |
| q06 | reuse_bcast_only | default | reuse_bcast_only |
| q08 | bloom_off | default | default |
| q09 | retain_off | default | complete_agg_off |
| q10 | bcast_256m | bloom_off | default |
| q13 | shuffle_48 | reuse_bcast_only | default |

Total wall time over these queries, seconds (failed or timed out default counted as 300 s):

| layout | default | best config per query | best single configuration | single configuration |
|---|---|---|---|---|
| asis | 167 | 134 | 142 | bcast_256m |
| sorted | 164 | 143 | 151 | bcast_256m |
| shuffled | 164 | 140 | 153 | bcast_256m |
