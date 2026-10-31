# ADR 0010: Join pushdown over key-range-partitioned Parquet files

Status: proposed, for review. Nothing in this document is implemented.

## Problem

The Flight scan is fast when StarRocks reads little from it. Every join between two Flight tables still ships both
inputs through StarRocks: Q4 moves the whole of `lineitem` (600M rows) to evaluate `EXISTS`, Q21 moves it three times,
Q3/Q10/Q12/Q18 move `orders` and `lineitem` to join them on `orderkey`. The Flight servers can see every file, so they
could join locally and return only the result, if a join can be split into independent pieces.

## What the data looks like

Reading the Parquet footers of the SF100 dataset (20 files per table):

| Table | Key | File *i* covers |
|---|---|---|
| orders | o_orderkey | [i·30M+1, (i+1)·30M] |
| lineitem | l_orderkey | [i·30M+1, (i+1)·30M] |
| customer | c_custkey | [i·750k+1, (i+1)·750k] |
| partsupp | ps_partkey | [i·1M+1, (i+1)·1M] |

`orders` file *i* and `lineitem` file *i* hold exactly the same key range, because the generator emitted them chunk by
chunk. Footer min/max values are free (a few KB per file, already cached for row counts) and reveal this.

## Decision

Split an equi-join into **units** found from footer statistics, and run each unit's join on a Flight server.

### Units

For a join `A.a = B.b` take the `[min, max]` of the key in every file of both tables. Two files are *connected* when
their intervals overlap. A **unit** is a connected component of that graph: the files of A and B whose key ranges chain
together.

* Correctness needs no alignment assumption. Rows with the same key sit in files whose intervals both contain that key,
  so they overlap and land in the same unit. Every matching pair is inside one unit.
* Outer, semi and anti joins stay correct: a preserved row belongs to exactly one file, hence one unit, and finding no
  partner there means finding none anywhere. NULL keys sit in some file and are preserved or dropped by the same rules.
* Usefulness depends on layout. Aligned ranges give many small units (20 here). Unaligned files give one big unit, and
  the rule declines.
* Only integer and date keys, and only when every row group has statistics. Parquet bounds may be looser than the data
  but never tighter, and looser bounds only merge units.

### Execution

1. **FE rule** (`FlightJoinPushdownRule`): a join of two Flight scans from the same catalog, equi-key on one column per
   side (integer/date), join type inner/left/right/semi/anti, non-equi conditions only if the converter can render them.
   Both scans' pushed predicates are carried. The join node is replaced by one scan whose output is the joined columns the
   parent needs (`LogicalJDBCScan` with a `flightJoin` spec, like `flightAggregation` today).
2. **Planning call**: the FE sends both tables, key columns, predicates and needed columns; the server builds units from
   the footers, balances them over endpoints by bytes (the existing `splitByBytes`) and returns one FlightInfo whose
   tickets are signed like any other.
3. **Server query per endpoint**: `SELECT ... FROM read_parquet(<A files of its units>) a JOIN read_parquet(<B files>) b
   ON a.k = b.k WHERE <pushed predicates>`, with DuckDB parallelism inside the endpoint and the existing streaming path
   out. Output types are mapped the way scans are, so StarRocks sees the schema it planned.
4. **Aggregates above**: `PushDownAggToFlightScanRule` already merges partial aggregates; extended to match a scan with a
   join spec, so `GROUP BY` queries (Q3, Q4, Q10, Q12, Q21) return a handful of rows per endpoint. When the group key
   contains the partition key (Q18's `GROUP BY l_orderkey HAVING ...`) each endpoint's groups are final.
5. **Chains** (`lineitem ⋈ orders ⋈ customer`) in phase 2: the first join pushes, the result joins the rest in StarRocks.

### Switches and safety

* `flight.join_pushdown` (default off), applied only when the unit count is at least twice the endpoint count.
* Server option `verifyRanges`: guards every unit scan with a range check that fails the query if a row falls outside
  its unit's bounds, so a writer that reports bounds tighter than the data cannot produce silent wrong answers.
* Ticket contents stay HMAC-signed; the server validates table and column names as it does for scans.

## Expected effect (to be measured)

| Query | Today ships | After |
|---|---|---|
| Q4 | all `lineitem` (3 cols) | 5 rows per endpoint |
| Q12 | `lineitem` + `orders` slices | a few rows per endpoint |
| Q18 | `lineitem` aggregated by orderkey, then joined | the 57 qualifying orders |
| Q21 | `lineitem` three times | one row per supplier per endpoint |
| Q3, Q10 | filtered `orders` and `lineitem` | partial revenue groups |

## Risks

* Server CPU and memory: the joins now run on three 8-core pods, next to the scans. DuckDB spills past its memory limit;
  unit skew is bounded by balancing on bytes.
* Real-world layouts may not be range-partitioned. Then the feature does nothing; it must never be slower, so it declines.
* Correctness of outer-join output types and NULL handling needs differential tests against StarRocks' own join.

## Test plan

* Unit builder against a brute-force oracle on random layouts: aligned, overlapping, gaps, single-file, NULL keys.
* Server integration: joined result equals an unsplit DuckDB join for every join type.
* FE rule tests for what pushes and what declines; E2E on TPC-H SF1 comparing all affected queries with direct mode.
* Cluster run of Q3, Q4, Q10, Q12, Q18, Q21 with the switch on and off.

## Phases

1. Server: unit builder, `planJoin`, per-endpoint join execution, tests. (about 2 days)
2. FE: rule for two-table inner and semi joins, aggregate merge over a join scan. (about 2 days)
3. Outer and anti joins, chains, cost threshold, `verifyRanges`. (about 2 days)
