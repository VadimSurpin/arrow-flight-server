| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 13 | 270 | 315 | 1.17 | 7 | 6 | 0.94 |
| shuffled | 13 | 356 | 340 | 0.95 | 8 | 5 | 0.94 |
| sorted | 13 | 252 | 307 | 1.22 | 7 | 6 | 0.93 |

Layout effect on the 13 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| asis | 270 | 315 |
| shuffled | 356 | 340 |
| sorted | 252 | 307 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q12: asis 2.9x, shuffled 2.8x, sorted 3.0x
- q07: asis 2.3x, shuffled 1.3x, sorted 2.5x
- q13: asis 1.6x, sorted 1.3x
- q04: asis 1.2x, shuffled 1.3x, sorted 1.2x
- q11: asis 1.2x, shuffled 1.2x

Failures (valid runs):

