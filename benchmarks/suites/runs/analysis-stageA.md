| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 94 | 4331 | 2318 | 0.54 | 81 | 13 | 0.40 |
| sorted | 93 | 4978 | 2160 | 0.43 | 80 | 13 | 0.34 |
| shuffled | 90 | 5043 | 2459 | 0.49 | 78 | 12 | 0.41 |

Layout effect on the 89 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| asis | 4003 | 1869 |
| sorted | 4617 | 1874 |
| shuffled | 4979 | 2433 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q41: asis 4.4x, sorted 2.4x, shuffled 2.7x
- q81: asis 2.4x, sorted 3.0x, shuffled 1.6x
- q91: asis 1.8x, sorted 2.7x
- q21: asis 2.5x, sorted 1.9x, shuffled 2.1x
- q39: asis 2.4x, sorted 2.3x, shuffled 1.9x
- q23: asis 2.3x
- q22: shuffled 2.2x
- q30: shuffled 2.1x
- q01: asis 2.0x, sorted 1.5x, shuffled 1.4x
- q64: asis 1.8x, sorted 1.4x, shuffled 1.7x
- q78: asis 1.3x, sorted 1.6x, shuffled 1.7x
- q67: asis 1.3x
- q06: asis 1.2x
- q80: shuffled 1.2x

Failures (valid runs):

- asis, flight, broadcast out of memory: q04, q11, q57, q74, q82
- sorted, direct, other: An error occurred while calling o731.collectToPython. : java.util.conc: q04
- sorted, flight, broadcast out of memory: q04, q11, q57, q74, q82
- shuffled, flight, broadcast out of memory: q04, q11, q57, q74, q82
- shuffled, direct, broadcast out of memory: q11
- shuffled, flight, timeout: q23, q65, q67, q68
