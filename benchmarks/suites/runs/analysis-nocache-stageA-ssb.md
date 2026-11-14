| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 12 | 214 | 228 | 1.06 | 9 | 3 | 0.69 |
| sorted | 12 | 205 | 215 | 1.05 | 8 | 4 | 0.88 |
| shuffled | 11 | 230 | 171 | 0.74 | 9 | 2 | 0.74 |

Layout effect on the 11 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| asis | 196 | 158 |
| sorted | 179 | 163 |
| shuffled | 230 | 171 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q12: asis 4.0x
- q07: sorted 2.0x
- q13: asis 1.5x, sorted 1.6x
- q11: asis 1.2x

Failures (valid runs):

