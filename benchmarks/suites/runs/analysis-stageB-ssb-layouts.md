Queries with a default result in all three layouts: 13

- best configuration identical in all three layouts: 10 of 13 (77%); of the 7 queries where some layout wants a non-default configuration: 4 identical
- default is best in all three layouts: 6

| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |
|---|---|---|---|---|
| asis | sorted | 0 | 4 | 2 |
| asis | shuffled | 0 | 5 | 1 |
| sorted | asis | 0 | 5 | 0 |
| sorted | shuffled | 0 | 5 | 0 |
| shuffled | asis | 0 | 5 | 1 |
| shuffled | sorted | 0 | 5 | 1 |

Queries whose best configuration differs between layouts (up to 40):

| query | asis | sorted | shuffled |
|---|---|---|---|
| q01 | rtf_all | retain_off | cbo_reorder_off |
| q03 | bloom_off | default | default |
| q09 | default | default | complete_agg_off |

Total wall time over these queries, seconds (failed or timed out default counted as 300 s):

| layout | default | best config per query | best single configuration | single configuration |
|---|---|---|---|---|
| asis | 553 | 349 | 367 | bcast_256m |
| sorted | 541 | 359 | 370 | bcast_256m |
| shuffled | 609 | 384 | 407 | bcast_256m |
