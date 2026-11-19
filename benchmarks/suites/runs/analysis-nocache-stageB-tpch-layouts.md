Queries with a default result in all three layouts: 18

- best configuration identical in all three layouts: 5 of 18 (28%); of the 13 queries where some layout wants a non-default configuration: 0 identical
- default is best in all three layouts: 5

| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |
|---|---|---|---|---|
| asis | sorted | 1 | 3 | 2 |
| asis | shuffled | 0 | 2 | 4 |
| sorted | asis | 0 | 2 | 8 |
| sorted | shuffled | 1 | 4 | 5 |
| shuffled | asis | 0 | 2 | 4 |
| shuffled | sorted | 0 | 4 | 2 |

Queries whose best configuration differs between layouts (up to 40):

| query | asis | sorted | shuffled |
|---|---|---|---|
| q01 | retain_off | default | cbo_reorder_off |
| q03 | bcast_256m | default | default |
| q04 | shuffle_48 | shuffle_48 | default |
| q07 | reuse_bcast_only | shuffle_48 | shuffle_48 |
| q08 | default | rtf_all | default |
| q09 | default | complete_agg_off | default |
| q10 | default | shuffle_48 | default |
| q12 | default | bcast_256m | bcast_256m |
| q14 | default | complete_agg_off | default |
| q15 | default | retain_off | default |
| q16 | default | default | bcast_256m |
| q19 | retain_off | shuffle_192 | cbo_reorder_off |
| q20 | retain_off | bcast_256m | bcast_256m |

Total wall time over these queries, seconds (failed or timed out default counted as 300 s):

| layout | default | best config per query | best single configuration | single configuration |
|---|---|---|---|---|
| asis | 422 | 383 | 404 | shuffle_48 |
| sorted | 308 | 264 | 287 | shuffle_48 |
| shuffled | 375 | 345 | 367 | complete_agg_off |
