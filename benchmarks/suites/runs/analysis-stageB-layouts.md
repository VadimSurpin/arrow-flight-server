Queries with a default result in all three layouts: 36

- best configuration identical in all three layouts: 18 of 36 (50%); of the 30 queries where some layout wants a non-default configuration: 12 identical
- default is best in all three layouts: 6

| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |
|---|---|---|---|---|
| asis | sorted | 0 | 19 | 4 |
| asis | shuffled | 1 | 19 | 3 |
| sorted | asis | 0 | 20 | 6 |
| sorted | shuffled | 0 | 21 | 5 |
| shuffled | asis | 0 | 19 | 4 |
| shuffled | sorted | 1 | 18 | 4 |

Queries whose best configuration differs between layouts (up to 40):

| query | asis | sorted | shuffled |
|---|---|---|---|
| q01 | reuse_bcast_only | retain_off | retain_off |
| q05 | default | reuse_bcast_only | retain_off |
| q09 | retain_off | complete_agg_off | bcast_10m |
| q23 | bcast_256m | default | default |
| q25 | bloom_off | default | default |
| q31 | retain_off | retain_off | cbo_off |
| q41 | retain_off | rtf_all | rtf_all |
| q49 | rtf_all | complete_agg_off | shuffle_192 |
| q51 | default | default | shuffle_192 |
| q64 | default | shuffle_48 | cbo_off |
| q65 | default | complete_agg_off | default |
| q71 | rtf_all | retain_off | rtf_all |
| q72 | default | reuse_bcast_only | default |
| q75 | default | cbo_reorder_off | rtf_all |
| q78 | shuffle_48 | shuffle_48 | None |
| q91 | bcast_10m | bcast_10m | reuse_bcast_only |
| q95 | complete_agg_off | default | default |
| q97 | default | shuffle_192 | default |

Total wall time over these queries, seconds (failed or timed out default counted as 300 s):

| layout | default | best config per query | best single configuration | single configuration |
|---|---|---|---|---|
| asis | 2672 | 1439 | 2299 | cbo_off |
| sorted | 2473 | 1364 | 2190 | bcast_10m |
| shuffled | 2954 | 1821 | 2556 | cbo_off |
