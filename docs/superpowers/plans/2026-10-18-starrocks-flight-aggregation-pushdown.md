# Flight catalog aggregation pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (chosen by the user: inline, no subagents). Steps use checkbox syntax.

**Goal:** Push COUNT/SUM/MIN/MAX/AVG with plain-column GROUP BY from StarRocks into the Flight server as per-endpoint partial aggregates, merged by a StarRocks aggregate above the scan.

**Architecture:** A Cascades transformation rule rewrites `Aggregate(Flight scan)` into `[Project](Aggregate'(Flight scan'))`. The scan' carries a `FlightAggregation` (group keys + partials) and exposes the keys and new partial column refs. `FlightScanNode` renders it with `ScanSql`. The BE maps Arrow columns to slots **by name**, so partial columns are aliased to their slot (synthetic column) names: no Thrift or BE change.

**Tech Stack:** Java 17/21 (flight-starrocks lib, StarRocks FE), JUnit 5, Maven, existing CI workflow.

**Spec:** `docs/superpowers/specs/2026-10-18-starrocks-flight-aggregation-pushdown-design.md` (supersedes its "Thrift and BE" section: no Thrift field, no BE logic change; the name-based slot mapping makes it unnecessary).

## Global Constraints

* Pushable: COUNT(*), COUNT(col), SUM, MIN, MAX, AVG; no DISTINCT; group keys and agg inputs are plain scan columns; no HAVING-style expressions.
* Partial types: COUNT -> BIGINT; SUM(integral) -> BIGINT; SUM(float/double) -> DOUBLE; AVG -> DOUBLE sum + BIGINT count; MIN/MAX only on numeric, date, datetime.
* Not pushed: string MIN/MAX, DECIMAL SUM/AVG, COUNT(DISTINCT), grouping sets.
* Catalog property `flight.aggregation_pushdown`, default `true`.
* Partial aliases are the synthetic column names `__agg0`, `__agg1`, ... (match `[A-Za-z_][A-Za-z0-9_]*`).
* Repos: library work in `/home/vadim/arrowflight-server/ArrowFlight` (branch `feat/starrocks-native-integration`); FE work in `/home/vadim/arrowflight-server/starrocks` (branch `feat/flight-connector`, remote `fork`). After FE work, run `scripts/export-starrocks-fork-patch.sh` in ArrowFlight.
* Commit trailer: `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.

## Review Focus

* Empty table / ungrouped aggregate: every endpoint returns one row (count 0, sum NULL); merge must yield count 0 and NULL sum.
* NULL group keys form one group across endpoints.
* Multi-endpoint table: partials from several endpoints for the same key merge correctly.
* Optimizer must actually choose the pushed plan (cost), and must never loop on re-applying the rule.
* Server rejecting or retyping a function (e.g. HUGEINT SUM): that function must drop out of the pushable set, not fail queries.

---

### Task 1: Live-server contract probe

**Files:**
- Create: `flight-starrocks/src/test/java/pro/surpin/data/arrowflight/starrocks/AggregateContractTest.java`

**Interfaces:** Produces: knowledge of which aggregate SQL the server accepts and the Arrow types returned; later tasks' pushable set must match the verified results.

- [ ] **Step 1:** Write a JUnit test (tag `real-services`, env `FLIGHT_IT_HOST/PORT/SCHEMA/TABLE` like `FlightCatalogServiceIntegrationTest`) that, through `FlightCatalog.connect(...)`, plans and reads these raw queries on the test table (`test_schema.test_table`), printing the Arrow schema of each and asserting it succeeds:
  `SELECT COUNT(*) AS __agg0 FROM s.t`, `SELECT int_col, COUNT(id) AS __agg0, CAST(SUM(bigint_col) AS BIGINT) AS __agg1, CAST(SUM(float_col) AS DOUBLE) AS __agg2, MIN(int_col) AS __agg3, MAX(double_col) AS __agg4 FROM s.t WHERE id > 5 GROUP BY int_col`, and the same with `GROUP BY` on a NULL-able key. Use `FlightCatalog.client` access that exists (add a package-private `rawQuery(String sql)` helper on `FlightCatalog` if none, returning `FlightInfo`).
- [ ] **Step 2:** Run against a standalone server: start `StandaloneTestFlightServer` (see `scripts/run-starrocks-native-e2e.sh` for the command) and run `./mvnw -B -pl flight-starrocks test -Dgroups=real-services -DFLIGHT_IT...` with the env set. Expected: PASS, with the printed Arrow types. If any query fails or returns an unexpected type (e.g. decimal), record it in `docs/integrations/starrocks-native.md` under "Aggregation pushdown" and remove that function from the pushable set used in Tasks 2 and 4.
- [ ] **Step 3:** Commit `test: contract probe for aggregate SQL against the Flight server`.

### Task 2: `ScanAggregate` + SQL rendering

**Files:**
- Create: `flight-starrocks/src/main/java/pro/surpin/data/arrowflight/starrocks/ScanAggregate.java`
- Modify: `ScanSql.java`, `FlightCatalog.java`
- Test: `ScanSqlTest.java`, new `ScanAggregateTest.java`

**Interfaces:**
- Produces: `public record ScanAggregate(List<String> groupKeys, List<Partial> partials)`; `public record Partial(Func func, String column, String alias)` where `column` is null for COUNT(*); `public enum Func { COUNT_STAR, COUNT, SUM_BIGINT, SUM_DOUBLE, MIN, MAX }`. `ScanSql.select(String schema, String table, ScanAggregate aggregate, ScanPredicate where)` and `FlightCatalog.planScan(String schema, String table, List<String> columns, ScanPredicate where, ScanAggregate aggregate)` (aggregate non-null ignores `columns`).

- [ ] **Step 1: failing test** in `ScanSqlTest`:
```java
@Test
void rendersGroupedPartialAggregates() {
    ScanAggregate agg = new ScanAggregate(List.of("k"), List.of(
        new ScanAggregate.Partial(ScanAggregate.Func.COUNT_STAR, null, "__agg0"),
        new ScanAggregate.Partial(ScanAggregate.Func.SUM_BIGINT, "x", "__agg1"),
        new ScanAggregate.Partial(ScanAggregate.Func.SUM_DOUBLE, "d", "__agg2"),
        new ScanAggregate.Partial(ScanAggregate.Func.MIN, "x", "__agg3"),
        new ScanAggregate.Partial(ScanAggregate.Func.COUNT, "x", "__agg4")));
    ScanPredicate p = new ScanPredicate.IsNull("k", true);
    assertEquals("SELECT k, COUNT(*) AS __agg0, CAST(SUM(x) AS BIGINT) AS __agg1, "
        + "CAST(SUM(d) AS DOUBLE) AS __agg2, MIN(x) AS __agg3, COUNT(x) AS __agg4 "
        + "FROM s.t WHERE k IS NOT NULL GROUP BY k", ScanSql.select("s", "t", agg, p));
}
@Test
void ungroupedAggregateHasNoGroupBy() { /* keys empty -> "SELECT COUNT(*) AS __agg0 FROM s.t" */ }
@Test
void rejectsBadIdentifiersInAggregates() { /* alias "a b" and column "x; y" -> IllegalArgumentException */ }
```
- [ ] **Step 2:** `./mvnw -B -pl flight-starrocks test -Dtest=ScanSqlTest` -> Expected FAIL (method missing).
- [ ] **Step 3:** Implement the records (validate identifiers with `ScanSql.identifier` made package-private) and `ScanSql.select(...)` building `SELECT [keys, ]partials FROM s.t[ WHERE ...][ GROUP BY keys]`; add `FlightCatalog.planScan` overload using it.
- [ ] **Step 4:** run `./mvnw -B -pl flight-starrocks test` -> Expected PASS (whole module).
- [ ] **Step 5:** Commit `feat(flight-starrocks): render partial aggregate queries`; push.

### Task 3: FE scan operator carries the aggregation

**Files (StarRocks repo, `fe/fe-core/src/main/java/com/starrocks/`):**
- Create: `connector/flight/FlightAggregation.java` (FE-side description)
- Modify: `sql/optimizer/operator/logical/LogicalJDBCScanOperator.java`, `.../physical/PhysicalJDBCScanOperator.java`, `planner/FlightScanNode.java`, `sql/plan/PlanFragmentBuilder.java`
- Test: `fe/fe-core/src/test/java/com/starrocks/planner/FlightScanNodeTest.java`

**Interfaces:**
- Produces: `FlightAggregation` (immutable; `groupKeys: List<String>`, `partials: List<Partial>` mirroring `ScanAggregate`, with `Partial(Func, String column, String alias, ColumnRefOperator ref)`; `equals/hashCode`; `toScanAggregate()`); `LogicalJDBCScanOperator.getFlightAggregation()/Builder.setFlightAggregation(...)`; same getter on `PhysicalJDBCScanOperator`; `FlightScanNode.setAggregation(ScanAggregate)`.

- [ ] **Step 1: failing tests** in `FlightScanNodeTest`: (a) with an aggregation set, `getNodeExplainString` contains `QUERY: SELECT k, COUNT(*) AS __agg0 FROM ... GROUP BY k` and the node asks the mock `FlightService.planScan(..., aggregate)`; (b) a builder round trip keeps the aggregation (`LogicalJDBCScanOperator.builder().withOperator(op)` preserves it; `equals` differs with/without).
- [ ] **Step 2:** run `mvn -pl fe-core test -Dtest=FlightScanNodeTest` (see `scripts/` / earlier runs for the exact FE test command and JDK 17 env) -> FAIL.
- [ ] **Step 3:** Implement; `FlightService` gets `planScan(..., ScanAggregate)` (default delegating), `CatalogFlightService` calls the new `FlightCatalog` overload; `PlanFragmentBuilder.buildFlightScan` passes `node.getFlightAggregation()` converted via `toScanAggregate()` and sets the tuple's columns from the aggregation (`computeColumns()` uses slot column names, which equal key names / aliases).
- [ ] **Step 4:** Run `FlightScanNodeTest` + all `com.starrocks.connector.flight.*` and JDBC scan tests -> PASS.
- [ ] **Step 5:** Commit in the StarRocks repo `feat(fe): flight scan operator carries a pushed aggregation`.

### Task 4: The pushdown rule

**Files:**
- Create: `sql/optimizer/rule/transformation/PushDownAggToFlightScanRule.java`
- Modify: `rule/RuleType.java` (new `TF_PUSH_DOWN_AGG_TO_FLIGHT_SCAN`), `rule/RuleSet.java` (register next to the meta-scan rule), `connector/flight/FlightConfig.java` (`aggregationPushdown` from `flight.aggregation_pushdown`, default true)
- Test: `sql/optimizer/rule/transformation/PushDownAggToFlightScanRuleTest.java` (plan-level, modelled on `FlightPushdownRuleTest`), `FlightConfigTest`

**Interfaces:** Consumes Task 3. Pattern: `LOGICAL_AGGR` over optional `LOGICAL_PROJECT` over `LOGICAL_JDBC_SCAN` (table is a `FlightTable`, scan has no aggregation yet, config flag true).

- [ ] **Step 1: failing tests**, one per behaviour:
  1. `select k, count(*), sum(i), min(i), max(i) from t group by k` -> EXPLAIN contains `SELECT k, COUNT(*) AS __agg0, CAST(SUM(i) AS BIGINT) ...GROUP BY k` and a merge aggregate above.
  2. `avg(i)` splits into SUM-double + COUNT partials and the plan has a divide in a Project.
  3. Declines (no `GROUP BY` text in QUERY): `count(distinct i)`, `group by k+1`, `min(string_col)`, `sum(decimal_col)`, flag off, aggregate over a scan already carrying an aggregation (no loop), a pushed filter still appears in `WHERE`.
  4. Pushed plan is chosen over the original (assert on the final plan, not just memo contents).
- [ ] **Step 2:** Run the new test -> FAIL (rule absent).
- [ ] **Step 3:** Implement. `check`: pattern conditions above, every agg call in the pushable set (`FunctionSet.COUNT/SUM/MIN/MAX/AVG`, not distinct, argument a `ColumnRefOperator` resolvable (through the Project) to a scan column whose type allows the function). `transform`, following `PushDownAggToMetaScanRule`: create partial refs with `columnRefFactory.create("__aggN", type, true)`, matching `Column` objects (name `__aggN`, nullable) in the new scan's `colRefToColumnMetaMap`/`columnMetaToColRefMap`, scan output = keys + partials, build `FlightAggregation`; new merge calls: COUNT/COUNT(*) -> `SUM` over partial (BIGINT builtin like the meta rule), SUM -> SUM, MIN/MAX -> same fn, AVG -> `SUM(sumPartial)` and `SUM(cntPartial)` plus a Project `old avg ref := divide(sum, cast(cnt as DOUBLE))`; reuse the original aggregate's type (`agg.getType()`), grouping keys and output refs for the non-AVG outputs.
- [ ] **Step 4:** If test 4 fails (cost picks the original), add a statistics tweak in `StatisticsCalculator` for a `LogicalJDBCScanOperator`/`PhysicalJDBCScanOperator` with an aggregation (output rows = max(1, input rows / 10)); re-run until the pushed plan wins. Record in the ledger.
- [ ] **Step 5:** Run all Flight and JDBC planner tests, then the broader `com.starrocks.sql.plan.*Aggregate*` tests that already exist and finish quickly -> PASS.
- [ ] **Step 6:** Commit `feat(fe): push aggregates into Flight scans`; push the `fork` branch.

### Task 5: E2E scenario, docs, CI

**Files:** `scripts/starrocks_native_e2e.py`, `docs/integrations/starrocks-native.md`, `docs/integrations/starrocks-fork-notes.md`, `starrocks-fork/*` (re-exported), the workflow if its triggers need a path.

- [ ] **Step 1:** Extend the `--with-be` scenario. For each shape compare pushed vs the same query made un-pushable by wrapping the input (`sum(abs(int_col))` style where equivalent is not available, otherwise `SET` the catalog property off via `ALTER CATALOG ... SET ("flight.aggregation_pushdown"="false")` if supported, else a second catalog created with the flag false): ungrouped `count(*), sum, min, max, avg`; grouped by a low-cardinality key (`year`, `month`); a filtered grouped aggregate; an empty-result ungrouped aggregate (count 0, sum NULL); grouped on a NULL-able key if the data has one. EXPLAIN assertion: contains `GROUP BY` and `__agg0` in `QUERY:`. Floating `avg`: compare with `abs(a-b) < 1e-9 * max(1,abs(a))`.
- [ ] **Step 2:** FE-only local run: `scripts/run-starrocks-native-e2e.sh` (non-BE parts: EXPLAIN assertions) -> PASS.
- [ ] **Step 3:** Docs: add "Aggregation pushdown" to `starrocks-native.md` (what is pushed, partial/merge model, property, limits, verified server behaviour from Task 1); update the fork notes' file list.
- [ ] **Step 4:** `scripts/export-starrocks-fork-patch.sh`; confirm the patch applies to pristine upstream at `BASE_SHA` (same check as before); commit and push the ArrowFlight branch.
- [ ] **Step 5:** Run the CI workflow (push triggers it); watch with `gh run view`; fix until the `--with-be` scenario passes. Expected: `RESULT: all checks passed`.
