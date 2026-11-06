| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| default.jsonl | 4 | 51 | 31 | 0.61 | 3 | 1 | 0.64 |

Layout effect on the 4 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| default.jsonl | 51 | 31 |

Flight slower than Direct by more than 15% (ratio, per layout):


Failures (valid runs):

- default.jsonl, direct, other: An error occurred while calling o358.collectToPython. : org.apache.spa: q03
- default.jsonl, direct, other: An error occurred while calling o246.collectToPython. : org.apache.spa: q06
