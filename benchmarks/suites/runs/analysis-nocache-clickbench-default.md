| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| default.jsonl | 35 | 223 | 239 | 1.07 | 17 | 18 | 1.01 |

Layout effect on the 35 queries that succeeded in both modes in every layout:

| layout | Direct total (s) | Flight total (s) |
|---|---|---|
| default.jsonl | 223 | 239 |

Flight slower than Direct by more than 15% (ratio, per layout):

- q30: default.jsonl 2.2x
- q22: default.jsonl 1.4x
- q07: default.jsonl 1.3x
- q01: default.jsonl 1.2x
- q23: default.jsonl 1.2x
- q29: default.jsonl 1.2x
- q09: default.jsonl 1.2x

Failures (valid runs):

- default.jsonl, flight, other: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "minute(Event: q19
- default.jsonl, direct, other: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "minute(Event: q19
- default.jsonl, direct, other: [CAST_INVALID_INPUT] The value '2013-07-01' of the type "STRING" canno: q37, q38, q39, q40, q41, q42
- default.jsonl, flight, other: [CAST_INVALID_INPUT] The value '2013-07-01' of the type "STRING" canno: q37, q38, q39, q40, q41, q42
- default.jsonl, direct, other: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "date_trunc(m: q43
- default.jsonl, flight, other: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "date_trunc(m: q43
