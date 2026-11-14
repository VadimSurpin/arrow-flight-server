Queries with a default result in all three layouts: 28

- best configuration identical in all three layouts: 11 of 28 (39%); of the 22 queries where some layout wants a non-default configuration: 5 identical
- default is best in all three layouts: 6

| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |
|---|---|---|---|---|
| asis | sorted | 1 | 11 | 5 |
| asis | shuffled | 0 | 8 | 9 |
| sorted | asis | 0 | 12 | 4 |
| sorted | shuffled | 1 | 7 | 8 |
| shuffled | asis | 0 | 10 | 3 |
| shuffled | sorted | 2 | 7 | 4 |

Queries whose best configuration differs between layouts (up to 40):

| query | asis | sorted | shuffled |
|---|---|---|---|
| q09 | reuse_bcast_only | default | default |
| q21 | bcast_256m | rtf_all | bloom_off |
| q22 | default | default | bcast_10m |
| q24 | default | default | complete_agg_off |
| q25 | bcast_10m | bcast_10m | default |
| q27 | bcast_10m | bcast_256m | default |
| q31 | retain_off | retain_off | cbo_off |
| q36 | cbo_off | cbo_off | default |
| q39 | cbo_reorder_off | rtf_all | cbo_off |
| q49 | retain_off | default | retain_off |
| q51 | shuffle_192 | default | default |
| q65 | default | shuffle_192 | default |
| q67 | bcast_10m | default | bcast_10m |
| q71 | shuffle_48 | retain_off | default |
| q75 | default | shuffle_48 | default |
| q91 | bcast_10m | bcast_10m | bloom_off |
| q97 | default | shuffle_48 | default |

Total wall time over these queries, seconds (failed or timed out default counted as 300 s):

| layout | default | best config per query | best single configuration | single configuration |
|---|---|---|---|---|
| asis | 1256 | 597 | 706 | bcast_10m |
| sorted | 949 | 557 | 755 | bcast_10m |
| shuffled | 807 | 691 | 804 | bcast_10m |
