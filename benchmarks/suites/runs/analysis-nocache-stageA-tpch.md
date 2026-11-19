| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 19 | 1651 | 458 | 0.28 | 17 | 2 | 0.35 |
| sorted | 21 | 656 | 413 | 0.63 | 16 | 5 | 0.76 |
| shuffled | 20 | 674 | 442 | 0.66 | 13 | 7 | 0.77 |

Layout effect on the 19 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| asis | 1651 | 458 |
| sorted | 478 | 329 |
| shuffled | 576 | 408 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q14: sorted 1.2x, shuffled 1.3x
- q13: asis 1.3x
- q03: shuffled 1.2x

Failures (valid runs):

