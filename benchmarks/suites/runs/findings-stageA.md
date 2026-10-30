# Stage A findings: TPC-DS SF100, default configuration, Flight vs Direct

Status: `asis` and `sorted` are complete; `shuffled` has 68 of 99 Flight queries (collection interrupted when the cluster became unreachable, see the end). One run per query and mode, no warm-up, 300 s timeout, 3 Flight servers and 3 Spark workers. Direct records come from `tpcds-<layout>-default.jsonl`, Flight records from `tpcds-<layout>-flight2.jsonl`, merged by `merge_runs.py` into `tpcds-<layout>-v2.jsonl`; tables are produced by `analyze.py`. Every query that succeeded in both modes returned the same rows (row count, hash or rows within 1e-6): 0 mismatches.

## Flight against Direct

| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |
|---|---|---|---|---|---|---|---|
| asis | 94 | 4331 | 2318 | 0.54 | 81 | 13 | 0.40 |
| sorted | 93 | 4978 | 2160 | 0.43 | 80 | 13 | 0.34 |

Layout effect on the 93 queries that worked in both modes in both layouts: Direct 4307 s (asis) against 4978 s (sorted), Flight 2299 s against 2160 s. Sorting the large tables by their join key made Direct 16% slower and Flight 6% faster in these single runs; noise on single runs is not measured yet, so treat the Direct number with care.

Flight is slower than Direct on 13 queries per layout. The ones that stay slow in both layouts: q41 (4.4x / 2.4x, both under 10 s), q81 (2.4x / 3.0x), q91 (1.8x / 2.7x), q21 (2.5x / 1.9x), q39 (2.4x / 2.3x), q01 (2.0x / 1.5x), q64 (1.8x / 1.4x), q78 (1.3x / 1.6x). q23 and q67 are slower only on asis. These are the first candidates for the knob search in Stage B.

## Failures

- **Flight, broadcast out of memory (q04, q11, q57, q74, q82 in both layouts).** With column statistics from the Flight server, Spark's cost-based join reorder puts big fact joins on the broadcast side and the 64 MB threshold lets Spark choose broadcast on sizes that are far too low. Direct has no column statistics and keeps a conservative plan. This depends on the configuration (`cbo_reorder_off`, `cbo_off`, lower broadcast thresholds), so it stays in the data as a failure label.
- **q09** timed out (300 s) in Direct and Flight in an early attempt and completed in later runs; q04 also fails in Direct on `sorted`.

## Bugs found by this collection (all fixed)

1. **Narrow decimal SUM pushdown.** Spark casts a partially pushed SUM back to the input column type. For TPC-DS `decimal(7,2)` any total above 99,999.99 overflowed: q44 failed, q06 returned 0 rows (52 expected) once ANSI was relaxed. The connector no longer pushes partial sums of decimals with fewer than 12 integer digits (`spark.flight.partialDecimalSum` auto|true|false). TPC-H's decimal(15,2) is unaffected.
2. **Column statistics in the wrong types.** Spark expects `Decimal` and day numbers; the connector sent `BigDecimal` and `LocalDate`, which broke every query with a UNION ALL over Flight tables at planning time.
3. **Server startup on a table with an unmappable column type** (ClickBench `Binary`): the whole server aborted. Such tables are now skipped with a warning (`clickbench.hits` is therefore not served by Flight yet).
4. **Prepared-statement cache.** The server cached every query plan per thread (about 7 MB per distinct TPC-DS query); after about 60 queries DuckDB's allocator memory hit its 5 GB limit and every later query failed. The cache is off by default now (`duckDbStatementCache=N` turns it on); idle allocator memory stays at 0 MB. The `asis` Flight records were collected before this fix but never reached the limit (peak about 700 MB); the `sorted` records are from after it.
5. **Harness and driver:** a dead Spark driver (heap exhausted by broadcast failures) poisoned the queries after it; the harness now restarts it, cancels timed-out queries and does not repeat them. Out-of-memory and broadcast failures count as valid results; only lost connections, pod restarts and NotReady nodes invalidate a run.

## Open

- `shuffled` layout: 68 of 99 Flight queries collected (1 invalid), rest pending. The cluster API (`control.mtuci.cloud.ru`) stopped resolving from the working environment, so collection stopped.
- Stage B (one knob at a time on the slow and failing queries) has not started.
- Unexplained: a 1 GB per hour anonymous-memory growth on the Flight pods after about 11 hours of uptime (plateau at about 10 GB, no slowdown). The statement-cache leak does not explain it, because that run repeated one query.

## Shuffled layout (completed)

Flight/Direct 0.49 over 90 queries (Flight faster on 78, slower on 12, 0 result mismatches). Over the 89 queries that succeed everywhere: Direct 4003 / 4617 / 4979 s and Flight 1869 / 1874 / 2433 s for asis / sorted / shuffled. Shuffling hurts Flight (+30%) and Direct (+24%) versus asis. Extra Flight-only timeouts (300 s) on shuffled: q23, q65, q67, q68. Full tables: `analysis-stageA.md`.
