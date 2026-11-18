| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 19 | 1636 | 508 | 0.31 | 17 | 2 | 0.40 |
| shuffled | 22 | 1008 | 878 | 0.87 | 14 | 8 | 0.89 |
| sorted | 22 | 826 | 599 | 0.73 | 16 | 6 | 0.83 |

Layout effect on the 19 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| asis | 1636 | 508 |
| shuffled | 591 | 450 |
| sorted | 519 | 425 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q13: asis 1.2x, shuffled 1.4x, sorted 2.0x
- q02: shuffled 1.4x, sorted 1.7x
- q19: shuffled 1.3x, sorted 1.7x
- q11: shuffled 1.4x, sorted 1.6x
- q18: shuffled 1.5x
- q14: shuffled 1.4x, sorted 1.2x
- q20: shuffled 1.3x
- q22: sorted 1.2x

Failures (valid runs):

- asis, direct, timeout: q17, q18, q21
