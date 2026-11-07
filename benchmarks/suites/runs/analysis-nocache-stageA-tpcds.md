| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 29 | 1771 | 861 | 0.49 | 19 | 10 | 0.57 |
| sorted | 30 | 1969 | 852 | 0.43 | 24 | 6 | 0.56 |
| shuffled | 29 | 1939 | 1043 | 0.54 | 22 | 7 | 0.61 |

Layout effect on the 29 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| asis | 1771 | 861 |
| sorted | 1765 | 750 |
| shuffled | 1939 | 1043 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q39: asis 4.3x, sorted 3.2x, shuffled 3.3x
- q81: asis 3.4x, sorted 3.4x, shuffled 3.8x
- q67: asis 1.6x, shuffled 1.7x
- q21: asis 1.5x, sorted 1.4x, shuffled 1.2x
- q47: asis 1.4x
- q01: asis 1.4x, sorted 1.3x, shuffled 1.3x
- q36: asis 1.3x
- q91: asis 1.3x, sorted 1.3x
- q41: sorted 1.2x, shuffled 1.2x

Failures (valid runs):

