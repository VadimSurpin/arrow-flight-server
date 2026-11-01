Queries with a default result in all three layouts: 22

- best configuration identical in all three layouts: 5 of 22 (23%); of the 19 queries where some layout wants a non-default configuration: 2 identical
- default is best in all three layouts: 3

| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |
|---|---|---|---|---|
| asis | sorted | 0 | 6 | 10 |
| asis | shuffled | 0 | 4 | 12 |
| sorted | asis | 0 | 8 | 1 |
| sorted | shuffled | 0 | 4 | 5 |
| shuffled | asis | 1 | 4 | 1 |
| shuffled | sorted | 1 | 3 | 2 |

Queries whose best configuration differs between layouts (up to 40):

| query | asis | sorted | shuffled |
|---|---|---|---|
| q01 | cbo_reorder_off | retain_off | default |
| q02 | bcast_256m | default | default |
| q03 | complete_agg_off | default | default |
| q05 | rtf_all | default | default |
| q06 | rtf_all | rtf_all | default |
| q07 | retain_off | default | default |
| q08 | default | default | rtf_all |
| q09 | cbo_off | default | default |
| q10 | reuse_bcast_only | cbo_reorder_off | default |
| q11 | complete_agg_off | default | default |
| q14 | shuffle_192 | rtf_all | shuffle_192 |
| q15 | rtf_all | default | default |
| q16 | default | cbo_reorder_off | default |
| q18 | default | default | shuffle_48 |
| q19 | shuffle_192 | default | default |
| q20 | cbo_reorder_off | cbo_reorder_off | reuse_bcast_only |
| q21 | reuse_bcast_only | retain_off | default |

Total wall time over these queries, seconds (failed or timed out default counted as 300 s):

| layout | default | best config per query | best single configuration | single configuration |
|---|---|---|---|---|
| asis | 635 | 529 | 571 | reuse_bcast_only |
| sorted | 616 | 555 | 581 | reuse_bcast_only |
| shuffled | 964 | 898 | 944 | bloom_off |
