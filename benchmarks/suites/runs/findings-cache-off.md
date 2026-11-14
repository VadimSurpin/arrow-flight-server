# DuckDB file cache off: Flight vs Direct, stability, memory

TPC-H SF100 asis, queries q06, q12, q14, default configuration, Flight pods started with `-DduckDbExternalFileCache=false -DduckDbObjectCache=false` (commit 325a26b). Data: `runs/fresh/cache-off/`.

## Flight and Direct, seconds

| query | Flight cache-off, cold run | Flight cache-off, 3 reps | Flight cache-on warm (earlier, fresh pods) | Direct, 3 reps |
|---|---|---|---|---|
| q06 | 33.4 | 15.1 / 17.6 / 18.1 | 3.3 / 3.4 | 43.8 / 45.2 / 48.6 |
| q12 | 33.4 | 21.1 / 22.3 / 27.6 | 12.2 / 12.6 | 66.0 / 68.9 / 75.5 |
| q14 | 22.6 | 17.3 / 17.5 / 18.0 | 6.0 / 6.9 | 78.1 / 81.3 / 85.3 |

- Cache off makes Flight 2-5x slower than cached on warm pods, and it is still 2.5-4.5x faster than Direct. The Flight advantage over Direct is not only the cache.
- Direct is unchanged by the Flight setting.
- The cold first run costs about twice a cache-off repetition, so cold start is not only S3 reads (connections, JIT, metadata setup).

## Stability (second round, 6 more reps on the same pods, no restart)

| query | reps 4-9 |
|---|---|
| q06 | 15.5 15.0 13.9 12.8 13.9 14.0 |
| q12 | 29.2 61.6 49.4 29.3 33.7 23.7 |
| q14 | 20.0 21.8 14.5 19.5 19.9 16.4 |

q06 and q14 stay in a narrow band; q12 shows slow outliers (49, 62 s against 21-29 s) that are not caused by the cache setting.

## Memory

Pod memory (`kubectl top`, once a minute) peaked at 3.5-5.7 GB during queries and fell back to about 2.1 GB when idle, 25 minutes in. The cache-on pods aged 32 h sat at 4-8 GB while idle. That supports the buffer-pool cache as the cause of the retained memory; the 7-8 GB growth was not reproduced here because the test ran only about 25 minutes.
