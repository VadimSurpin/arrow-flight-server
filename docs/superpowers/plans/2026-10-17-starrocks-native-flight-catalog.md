# StarRocks Native Flight Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A StarRocks `branch-4.0` fork with a native `flight` external catalog that reads this repo's Flight SQL server, with projection and filter pushdown.

**Architecture:** Pure-Java pieces that do not depend on StarRocks internals (predicate model, SQL rendering, plan call) are built and tested in this repo's `flight-starrocks` module and consumed by the fork's FE as a jar. The fork gets a `flight` connector (FE metadata + scan node + Thrift messages) modelled on the Elasticsearch connector, and a BE `FlightDataSource` (modelled on `es_connector`/`jdbc_connector`) that pulls Arrow batches via a vendored pull-style `FlightScanReader` and converts them to StarRocks chunks.

**Tech Stack:** Java 17 + Maven (FE, `flight-starrocks`), C++17 + CMake/Ninja + gtest (BE), Thrift, Arrow 19 (Java) / Arrow C++ with Flight SQL, StarRocks `branch-4.0`.

**Spec:** `docs/superpowers/specs/2026-10-17-starrocks-native-flight-catalog-design.md`

## Global Constraints

- Fork base is StarRocks `branch-4.0`, cloned to `/home/vadim/arrowflight-server/starrocks`, work branch `feat/flight-connector`.
- Toolchain: `export PATH=/home/vadim/arrowflight-server/ArrowFlight/tools/.local/env/bin:$PATH` (OpenJDK 17, Maven 3.9, CMake 4.4, Ninja, Make, g++ 14). No Docker, no sudo.
- `flight-starrocks` compiles with `<release>17</release>` (StarRocks FE runs Java 17); do not use Java 21 APIs there.
- Identifiers sent to the Flight server are unquoted ASCII `[A-Za-z_][A-Za-z0-9_]*` only (existing `ScanSql` rule).
- Credentials (token, basic auth, tickets) must never appear in logs, `EXPLAIN`, `SHOW CREATE CATALOG` output or exception messages.
- The BE re-applies ALL conjuncts to returned chunks; pushdown may only reduce rows, never be the sole filter.
- Arrow type must match the FE-declared type; mismatch is a hard error, never a silent cast.
- Out of scope: per-user identity, limit/aggregate pushdown, sinks/writes, list/struct/map types, upstreaming.
- Commits in this repo end with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.

## Review Focus

- String literal containing `'`, `\`, newline or unicode in a pushed filter: must be escaped so the filter returns the same rows as StarRocks would (Task 1).
- `NULL` semantics: `col <> 1`, `NOT (col = 1)` and `col IN (...)` with NULL rows must match StarRocks three-valued logic; BE residual filter guards the rest (Tasks 1, 6).
- Server returns zero endpoints or an empty stream: `SELECT COUNT(*)` must return 0, not hang or error (Tasks 3, 7).
- Column names the server accepts but `ScanSql` rejects (reserved words, mixed case with spaces): `getTable` must fail with a clear message naming the column, not produce broken SQL (Task 5).
- Arrow `Timestamp` with a time zone, or decimal wider than StarRocks supports (> 38 digits): clear unsupported-type error at `getTable` (Task 5).
- Token expired mid-scan or wrong token: error mentions the endpoint but contains neither the token nor ticket bytes (Tasks 7, 8).

---

## Task 0: Fork bootstrap and risk gate

**Files:**
- Create: `/home/vadim/arrowflight-server/starrocks/` (git clone)
- Create: `scripts/build-starrocks-fork.sh` (this repo)
- Create: `docs/integrations/starrocks-fork-notes.md` (this repo; findings)

**Interfaces:**
- Produces: a cloned fork on branch `feat/flight-connector`; a recorded answer to "does the BE thirdparty include the Arrow Flight client?" and "can the BE be built on this machine?"; the confirmed paths/signatures that Tasks 5–7 reference.

- [ ] **Step 1: Clone the fork base**

```bash
cd /home/vadim/arrowflight-server
git clone --depth 1 --branch branch-4.0 https://github.com/StarRocks/starrocks.git starrocks
cd starrocks && git checkout -b feat/flight-connector
```
Expected: `branch-4.0` checked out; `ls` shows `fe/ be/ thirdparty/ gensrc/ build.sh`.

- [ ] **Step 2: Confirm the reference files this plan models exist**

```bash
cd /home/vadim/arrowflight-server/starrocks
ls fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/ fe/fe-core/src/main/java/com/starrocks/connector/jdbc/
ls be/src/connector/es_connector.cpp be/src/connector/jdbc_connector.cpp be/src/connector/connector.cpp
grep -n "ES\|JDBC" fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java | head
grep -rn "TConnectorScanNode\|TEsScanNode\|TJDBCScanNode" gensrc/thrift/PlanNodes.thrift | head
```
Expected: all files exist. Write the Thrift struct names found into `docs/integrations/starrocks-fork-notes.md`.

- [ ] **Step 3: Risk gate — Arrow Flight in the BE thirdparty**

```bash
cd /home/vadim/arrowflight-server/starrocks
grep -n "arrow" thirdparty/build-thirdparty.sh | head -20
grep -n "ARROW_FLIGHT\|FLIGHT" thirdparty/build-thirdparty.sh be/CMakeLists.txt | head -20
```
Record in the notes file whether `-DARROW_FLIGHT=ON` and `ARROW_FLIGHT_SQL=ON` are set in the thirdparty arrow build and whether `be/CMakeLists.txt` links `arrow_flight`. If either is OFF, add a note describing the needed thirdparty change; Task 7 then starts with that change.

- [ ] **Step 4: Risk gate — can this machine build the BE?**

```bash
free -g | head -2; df -h /home | tail -1; nproc
curl -sI -m 10 https://releases.starrocks.io/ | head -1
```
Record results. If a prebuilt thirdparty archive is not reachable and RAM < 16 GB without swap, STOP and report to the user before continuing to Task 5; Tasks 1–4 are unaffected.

- [ ] **Step 5: Write `scripts/build-starrocks-fork.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$repo_root/tools/.local/env/bin:$PATH"
export JAVA_HOME="$repo_root/tools/.local/env"
fork="${STARROCKS_FORK:-$repo_root/../starrocks}"
cd "$fork"
exec ./build.sh "$@"
```
`chmod +x scripts/build-starrocks-fork.sh`

- [ ] **Step 6: Baseline FE build of the unmodified fork**

```bash
cd /home/vadim/arrowflight-server/ArrowFlight && ./scripts/build-starrocks-fork.sh --fe --clean 2>&1 | tail -30
```
Expected: FE builds (`output/fe` exists). If not, fix the toolchain before proceeding and record the fix in the notes.

- [ ] **Step 7: Commit (this repo only; the fork is a separate repo)**

```bash
cd /home/vadim/arrowflight-server/ArrowFlight
git add scripts/build-starrocks-fork.sh docs/integrations/starrocks-fork-notes.md
git commit -m "build: add StarRocks fork build wrapper and bootstrap notes

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

## Task 1: Predicate model and SQL rendering (`flight-starrocks`)

**Files:**
- Create: `flight-starrocks/src/main/java/pro/surpin/data/arrowflight/starrocks/ScanPredicate.java`
- Modify: `flight-starrocks/src/main/java/pro/surpin/data/arrowflight/starrocks/ScanSql.java`
- Test: `flight-starrocks/src/test/java/pro/surpin/data/arrowflight/starrocks/ScanSqlTest.java`

**Interfaces:**
- Produces:
  - `sealed interface ScanPredicate` with records `Compare(String column, Op op, Literal value)`, `In(String column, List<Literal> values)`, `IsNull(String column, boolean negated)`, `And(List<ScanPredicate> terms)`, `Or(List<ScanPredicate> terms)`, `Not(ScanPredicate term)`.
  - `enum Op { EQ, NE, LT, LE, GT, GE }`.
  - `sealed interface Literal` with records `IntLit(long)`, `DecimalLit(java.math.BigDecimal)`, `DoubleLit(double)`, `StringLit(String)`, `BoolLit(boolean)`, `DateLit(java.time.LocalDate)`, `TimestampLit(java.time.LocalDateTime)`.
  - `String ScanPredicate.toSql()`.
  - `static String ScanSql.select(String schema, String table, List<String> columns, ScanPredicate where)` (null `where` = no WHERE).

- [ ] **Step 1: Write failing tests (append to `ScanSqlTest`)**

```java
@Test
void rendersComparisonAndEscapesStrings() {
    ScanPredicate p = new ScanPredicate.Compare("name", ScanPredicate.Op.EQ,
            new ScanPredicate.StringLit("O'Brien\\x\n"));
    assertEquals("SELECT a FROM s.t WHERE (name = 'O''Brien\\\\x\n')",
            ScanSql.select("s", "t", List.of("a"), p));
}

@Test
void rendersBooleanTreeInAndNotIn() {
    ScanPredicate p = new ScanPredicate.And(List.of(
            new ScanPredicate.Or(List.of(
                    new ScanPredicate.Compare("a", ScanPredicate.Op.GE, new ScanPredicate.IntLit(1)),
                    new ScanPredicate.IsNull("b", false))),
            new ScanPredicate.Not(new ScanPredicate.In("c",
                    List.of(new ScanPredicate.IntLit(1), new ScanPredicate.IntLit(2))))));
    assertEquals("(((a >= 1) OR (b IS NULL)) AND (NOT (c IN (1, 2))))", p.toSql());
}

@Test
void rendersTemporalAndDecimalLiterals() {
    assertEquals("(d = DATE '2026-10-17')", new ScanPredicate.Compare("d", ScanPredicate.Op.EQ,
            new ScanPredicate.DateLit(java.time.LocalDate.of(2026, 10, 17))).toSql());
    assertEquals("(t < TIMESTAMP '2026-10-17 01:02:03.5')", new ScanPredicate.Compare("t",
            ScanPredicate.Op.LT, new ScanPredicate.TimestampLit(
                    java.time.LocalDateTime.of(2026, 10, 17, 1, 2, 3, 500_000_000))).toSql());
    assertEquals("(x = 12.50)", new ScanPredicate.Compare("x", ScanPredicate.Op.EQ,
            new ScanPredicate.DecimalLit(new java.math.BigDecimal("12.50"))).toSql());
}

@Test
void rejectsBadColumnAndEmptyBooleanTerms() {
    assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.IsNull("a b", false).toSql());
    assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.And(List.of()).toSql());
    assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.In("a", List.of()).toSql());
    assertThrows(IllegalArgumentException.class, () -> new ScanPredicate.Compare("a",
            ScanPredicate.Op.EQ, new ScanPredicate.DoubleLit(Double.NaN)).toSql());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `export PATH=$PWD/tools/.local/env/bin:$PATH; ./mvnw -q test -pl flight-starrocks -Dtest=ScanSqlTest`
Expected: compilation FAIL (`ScanPredicate` not found).

- [ ] **Step 3: Implement `ScanPredicate`**

```java
package pro.surpin.data.arrowflight.starrocks;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** A filter the frontend may push into the Flight SQL query; renders to restricted SQL. */
public sealed interface ScanPredicate {
    /** Comparison operators. */
    enum Op {
        EQ("="), NE("<>"), LT("<"), LE("<="), GT(">"), GE(">=");
        private final String sql;
        Op(String sql) { this.sql = sql; }
        String sql() { return sql; }
    }

    /** A constant. */
    sealed interface Literal {
        String toSql();
    }

    /** Integer constant. */
    record IntLit(long value) implements Literal {
        @Override public String toSql() { return Long.toString(value); }
    }

    /** Exact decimal constant. */
    record DecimalLit(BigDecimal value) implements Literal {
        @Override public String toSql() { return value.toPlainString(); }
    }

    /** Floating point constant; NaN and infinities cannot be pushed. */
    record DoubleLit(double value) implements Literal {
        @Override public String toSql() {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new IllegalArgumentException("Cannot push non-finite double");
            }
            return Double.toString(value);
        }
    }

    /** String constant. */
    record StringLit(String value) implements Literal {
        @Override public String toSql() {
            return "'" + value.replace("'", "''") .replace("\\", "\\\\") + "'";
        }
    }

    /** Boolean constant. */
    record BoolLit(boolean value) implements Literal {
        @Override public String toSql() { return value ? "TRUE" : "FALSE"; }
    }

    /** Date constant. */
    record DateLit(LocalDate value) implements Literal {
        @Override public String toSql() { return "DATE '" + value + "'"; }
    }

    /** Timestamp constant without time zone. */
    record TimestampLit(LocalDateTime value) implements Literal {
        private static final DateTimeFormatter FMT = new java.time.format.DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart().appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 1, 9, true)
                .optionalEnd().toFormatter();
        @Override public String toSql() { return "TIMESTAMP '" + FMT.format(value) + "'"; }
    }

    /** {@code column op literal}. */
    record Compare(String column, Op op, Literal value) implements ScanPredicate {
        @Override public String toSql() {
            return "(" + ident(column) + " " + op.sql() + " " + value.toSql() + ")";
        }
    }

    /** {@code column IN (...)}. */
    record In(String column, List<Literal> values) implements ScanPredicate {
        @Override public String toSql() {
            if (values.isEmpty()) {
                throw new IllegalArgumentException("IN list must not be empty");
            }
            return "(" + ident(column) + " IN ("
                    + values.stream().map(Literal::toSql).collect(Collectors.joining(", ")) + "))";
        }
    }

    /** {@code column IS [NOT] NULL}. */
    record IsNull(String column, boolean negated) implements ScanPredicate {
        @Override public String toSql() {
            return "(" + ident(column) + (negated ? " IS NOT NULL)" : " IS NULL)");
        }
    }

    /** Conjunction. */
    record And(List<ScanPredicate> terms) implements ScanPredicate {
        @Override public String toSql() { return join(terms, " AND "); }
    }

    /** Disjunction. */
    record Or(List<ScanPredicate> terms) implements ScanPredicate {
        @Override public String toSql() { return join(terms, " OR "); }
    }

    /** Negation. */
    record Not(ScanPredicate term) implements ScanPredicate {
        @Override public String toSql() { return "(NOT " + term.toSql() + ")"; }
    }

    /**
     * Renders this predicate as SQL.
     *
     * @return SQL boolean expression
     */
    String toSql();

    /** Identifier rule shared with {@link ScanSql}. */
    Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private static String ident(String name) {
        Objects.requireNonNull(name, "column");
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Unsupported SQL identifier: " + name);
        }
        return name;
    }

    private static String join(List<ScanPredicate> terms, String sep) {
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("Boolean predicate needs at least one term");
        }
        return "(" + terms.stream().map(ScanPredicate::toSql).collect(Collectors.joining(sep)) + ")";
    }
}
```
NOTE: `StringLit` doubles backslashes; the existing server's SQL engine is DuckDB-backed, where backslash is NOT an escape in standard strings. Before relying on it, Step 5 checks the server's actual behaviour and fixes the expectation in the test and implementation together.

- [ ] **Step 4: Add the overload to `ScanSql`**

```java
public static String select(String schema, String table, List<String> columns,
        ScanPredicate where) {
    String base = select(schema, table, columns);
    return where == null ? base : base + " WHERE " + where.toSql();
}
```
Expected rendering for the first test: `SELECT a FROM s.t WHERE (name = 'O''Brien\\x\n')` (adjust per Step 5).

- [ ] **Step 5: Verify string-escaping against the real server**

Start the server fixture used by `FlightCatalogServiceIntegrationTest` (see that test for how), run `SELECT 'a\b'` and `SELECT 'O''Brien'` through Flight SQL, and record whether `\` is an escape. If it is NOT an escape, remove the backslash doubling from `StringLit.toSql` and from the first test's expected string. Newlines are passed through literally in both cases.

- [ ] **Step 6: Run tests**

Run: `./mvnw -q test -pl flight-starrocks -Dtest=ScanSqlTest`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add flight-starrocks/src
git commit -m "feat: add pushdown predicate model and SQL rendering for Flight scans

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

## Task 2: `FlightCatalog.planScan` with filter

**Files:**
- Modify: `flight-starrocks/src/main/java/pro/surpin/data/arrowflight/starrocks/FlightCatalog.java` (the `planScan` method, ~line 129)
- Test: `flight-starrocks/src/test/java/pro/surpin/data/arrowflight/starrocks/FlightCatalogTest.java`

**Interfaces:**
- Consumes: `ScanSql.select(schema, table, columns, where)` (Task 1).
- Produces: `FlightInfo FlightCatalog.planScan(String schema, String table, List<String> columns, ScanPredicate where)`; the 3-arg overload delegates with `null`.

- [ ] **Step 1: Failing test** — in `FlightCatalogTest`, following the existing mock style there (read the file first and mirror how `sqlClient.execute` is verified), assert that `planScan("s","t",List.of("a"), new ScanPredicate.IsNull("a", true))` calls `sqlClient.execute("SELECT a FROM s.t WHERE (a IS NOT NULL)", callOptions)`.

- [ ] **Step 2: Run, expect FAIL** — `./mvnw -q test -pl flight-starrocks -Dtest=FlightCatalogTest`

- [ ] **Step 3: Implement**

```java
public FlightInfo planScan(String schema, String table, List<String> columns) {
    return planScan(schema, table, columns, null);
}

public FlightInfo planScan(String schema, String table, List<String> columns,
        ScanPredicate where) {
    return sqlClient.execute(ScanSql.select(schema, table, columns, where), callOptions);
}
```

- [ ] **Step 4: Run, expect PASS** — same command.
- [ ] **Step 5: Commit** — `git commit -am "feat: plan Flight scans with pushed-down filters"` (with the Co-Authored-By trailer).

---

## Task 3: Pull-style C++ reader

**Files:**
- Modify: `flight-starrocks/cpp/include/flight_starrocks/flight_scan_reader.h`
- Modify: `flight-starrocks/cpp/src/flight_scan_reader.cc`
- Test: `flight-starrocks/cpp/tests/flight_scan_reader_test.cc`

**Interfaces:**
- Consumes: existing `FlightScanReader::Connect`, `EndpointLocation`.
- Produces:
  - `class TicketStream { public: arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next(); /* nullptr batch == end of stream */ arrow::Status Cancel(); }`
  - `arrow::Result<std::unique_ptr<TicketStream>> FlightScanReader::OpenTicket(const arrow::flight::Ticket& ticket);`

- [ ] **Step 1: Read `flight_scan_reader.cc` and the existing test** to reuse the in-process test server and helpers (the test already stands up a Flight server; extend it, do not add a second one).

- [ ] **Step 2: Failing tests** in `flight_scan_reader_test.cc`, using that existing fixture:
  - `OpenTicket` + repeated `Next()` returns the same batches as `ScanTicket` and then a null batch.
  - a ticket the server rejects returns a non-OK `Result` from `OpenTicket` or `Next` (assert the error text contains neither the ticket bytes nor the bearer token).
  - an empty stream returns a null batch on the first `Next()`.

- [ ] **Step 3: Build, expect FAIL** — `cmake -S flight-starrocks/cpp -B flight-starrocks/cpp/build -G Ninja && cmake --build flight-starrocks/cpp/build` (needs Arrow C++ with Flight SQL; install via `micromamba install -p tools/.local/env -c conda-forge libarrow-flight-sql` if absent, and record the version in the notes file).

- [ ] **Step 4: Implement** `TicketStream` holding `std::unique_ptr<arrow::flight::FlightStreamReader>`; `Next()` calls `reader->Next()` and returns `chunk.data` (null at end); `Cancel()` calls `reader->Cancel()`. `OpenTicket` calls `client_->DoGet(options_, ticket)` using the same endpoint-location handling as `ScanTicket`. Errors are wrapped as `arrow::Status::IOError("Flight stream failed at " + location.ToString() + ": " + status.message())` after stripping nothing else; never include `ticket.ticket` or the token.

- [ ] **Step 5: Run** — `ctest --test-dir flight-starrocks/cpp/build --output-on-failure`; expect PASS.
- [ ] **Step 6: Commit** with the trailer.

---

## Task 4: Publish `flight-starrocks` to the fork's FE

**Files:**
- Modify (fork): `fe/fe-core/pom.xml` (add dependency)
- Create (this repo): `scripts/install-flight-starrocks-jar.sh`

**Interfaces:**
- Produces: `pro.surpin.arrowflight:arrow-flight-starrocks-connector:1.0.0` resolvable from the fork's FE build (local Maven repo install).

- [ ] **Step 1:** `./mvnw -q install -DskipTests -pl flight-starrocks -am` (this repo). Expected: artifact in `~/.m2`.
- [ ] **Step 2:** Add to the fork's `fe/fe-core/pom.xml` `<dependencies>` the dependency `pro.surpin.arrowflight:arrow-flight-starrocks-connector:1.0.0` and exclude `flight-sql-jdbc-driver` (the FE does not need the JDBC driver and it shades Arrow).
- [ ] **Step 3:** Check Arrow version conflicts: `cd fork/fe && mvn -q dependency:tree -pl fe-core | grep -i arrow`. If the FE already carries a different Arrow version, record it in the notes; align `flight-starrocks` or shade as needed before continuing.
- [ ] **Step 4:** FE compile: `./scripts/build-starrocks-fork.sh --fe`. Expected PASS.
- [ ] **Step 5:** Commit in the fork (`git -C ../starrocks commit -am "build(fe): depend on arrow-flight-starrocks-connector"`) and the script in this repo.

---

## Task 5: FE connector, metadata and type mapping (fork)

**Files (fork, under `fe/fe-core/src/main/java/com/starrocks/connector/flight/`):**
- Create: `FlightConnector.java`, `FlightMetadata.java`, `FlightConfig.java`, `FlightTypeMapper.java`, `FlightTable.java`
- Modify: `connector/ConnectorType.java` (add `FLIGHT`)
- Test (fork): `fe/fe-core/src/test/java/com/starrocks/connector/flight/FlightTypeMapperTest.java`, `FlightMetadataTest.java`

**Interfaces:**
- Consumes: `FlightCatalog.connect/schemas/tables/tableSchema` (this repo), the structure of `ElasticsearchConnector`/`JDBCConnector` confirmed in Task 0.
- Produces: `FlightTypeMapper.toStarRocks(org.apache.arrow.vector.types.pojo.Field) : com.starrocks.catalog.Type` (throws `StarRocksConnectorException` naming column and Arrow type for unsupported); `FlightMetadata` implementing `listDbNames()`, `listTableNames(ctx, db)`, `getTable(ctx, db, table)`; `FlightTable extends Table` carrying `schema`, `name`.

- [ ] **Step 1: Confirm real signatures** — read `ConnectorMetadata.java`, `JDBCMetadata.java`, `JDBCConnector.java` and `ConnectorFactory.java` in the fork; list the abstract/overridden methods in the notes file. Use these exact signatures in the steps below (adjust the code in this task to them; the shapes below follow `JDBCMetadata`).

- [ ] **Step 2: Failing `FlightTypeMapperTest`** — table-driven JUnit tests: Arrow `Int(8/16/32/64,signed)` → TINYINT/SMALLINT/INT/BIGINT; `FloatingPoint(SINGLE/DOUBLE)` → FLOAT/DOUBLE; `Decimal(p,s)` p≤38 → DECIMAL(p,s) (v3); `Bool` → BOOLEAN; `Utf8/LargeUtf8` → VARCHAR; `Binary` → VARBINARY; `Date(DAY)` → DATE; `Timestamp(*, null tz)` → DATETIME; `Timestamp(*, "UTC")`, `Decimal` p>38, `List`, `Struct`, unsigned ints → exception whose message contains the column name and the Arrow type.

- [ ] **Step 3: Run, expect FAIL**; **Step 4: implement** `FlightTypeMapper` with a `switch` on `field.getType().getTypeID()` (Arrow `ArrowType.ArrowTypeID`), per the table above; run, expect PASS.

- [ ] **Step 5: Failing `FlightMetadataTest`** with a mocked `FlightCatalog`: `listDbNames` returns schemas; `listTableNames` returns table names; `getTable` maps columns via `FlightTypeMapper`, rejects a column whose name violates `ScanPredicate.IDENTIFIER` with a message naming the column (Review Focus), and caches the result so a second call doesn't hit `tableSchema`.

- [ ] **Step 6: Implement** `FlightConfig` (reads `flight.uri`, `flight.auth.type` = `none|token|basic`, `flight.auth.token`, `flight.auth.user`, `flight.auth.password`, `flight.tls.enabled`, `flight.tls.ca`, `flight.cache.ttl.seconds` default 300; `toString()` redacts secrets), `FlightMetadata`, `FlightConnector` (`getMetadata()` returns `FlightMetadata`; `shutdown()` closes the `FlightCatalog`), `FlightTable`, and `FLIGHT("flight", FlightConnector.class, null)` in `ConnectorType` plus its entry in `SUPPORT_TYPE_SET`. Build the bearer `CallOption`: `new CredentialCallOption(new BearerCredentialWriter(token))` from `org.apache.arrow.flight.auth2`.

- [ ] **Step 7: Run FE tests** for the new classes, then commit in the fork. Also add a test that `FlightConfig.toString()` and exceptions from `FlightMetadata` never contain the token/password string.

---

## Task 6: FE scan node, filter conversion, Thrift (fork)

**Files (fork):**
- Modify: `gensrc/thrift/PlanNodes.thrift`, `gensrc/thrift/Types.thrift` if a plan-node enum is required (confirmed in Task 0)
- Create: `fe/fe-core/src/main/java/com/starrocks/planner/FlightScanNode.java`, `.../connector/flight/FlightPredicateConverter.java`
- Modify: the place `JDBCScanNode`/`EsScanNode` is created for external tables (find with `grep -rn "new JDBCScanNode\|new EsScanNode" fe/fe-core/src/main/java`) to create `FlightScanNode` for `FlightTable`
- Test (fork): `FlightPredicateConverterTest.java`, `FlightScanNodeTest.java`

**Interfaces:**
- Consumes: `ScanPredicate`, `FlightCatalog.planScan(..., ScanPredicate)` (Tasks 1–2); `FlightTable` (Task 5).
- Produces:
  - Thrift: `struct TFlightScanRange { 1: required list<string> locations; 2: required binary ticket; }`, `struct TFlightScanNode { 1: required Types.TTupleId tuple_id; 2: required string flight_uri; 3: optional string auth_type; 4: optional string auth_token; 5: optional string auth_user; 6: optional string auth_password; 7: optional bool tls_enabled; 8: optional string tls_ca; 9: required list<string> columns; }`, plus `TScanRange.flight_scan_range` and `TPlanNode.flight_scan_node`, and enum `FLIGHT_SCAN_NODE`.
  - `Optional<ScanPredicate> FlightPredicateConverter.convert(com.starrocks.sql.optimizer.operator.scalar.ScalarOperator op)` — empty when any part is inexpressible (never a partial conjunct inside an OR/NOT; for a top-level AND, convert terms individually and keep the pushable ones).

- [ ] **Step 1: Failing `FlightPredicateConverterTest`** — `a = 1` → `Compare`; `a IN (1,2)`; `a IS NULL`; `NOT a = 1`; `a = 1 AND f(b) = 2` pushes only the first; `a = 1 OR f(b) = 2` pushes nothing; `col = 'x'` string; date/datetime literals; a cast on the column side → not pushed; `LIKE` → not pushed.
- [ ] **Step 2:** Run FE test, expect FAIL. **Step 3:** implement the converter over `ScalarOperator` (`BinaryPredicateOperator`, `InPredicateOperator`, `IsNullPredicateOperator`, `CompoundPredicateOperator`, `ColumnRefOperator`, `ConstantOperator`); confirm exact class names with `ls fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/scalar/`. Expect PASS.
- [ ] **Step 4: Thrift** — add the structs above and regenerate (`build.sh --fe` regenerates; `--be` regenerates C++). Commit.
- [ ] **Step 5: `FlightScanNode`** — modelled on `JDBCScanNode`: in `init`/`finalizeStats` take `getColumns()` (pruned) and the pushable conjuncts; call `planScan(schema, table, columns, predicate)` once; build one scan range per `FlightEndpoint` (locations as URIs, `ticket.getBytes()`); `toThrift` fills `TFlightScanNode` (credentials included, only here); `getNodeExplainString` prints uri, columns and the pushed SQL, **never** credentials or ticket bytes. Zero endpoints → zero scan ranges (empty result, no error).
- [ ] **Step 6: Failing/passing `FlightScanNodeTest`** — explain string contains the projection and pushed predicate and does not contain the token; zero endpoints yields no ranges.
- [ ] **Step 7:** Wire into the planner site found above; full FE build `./scripts/build-starrocks-fork.sh --fe`; commit in the fork.

---

## Task 7: BE data source and chunk converter (fork)

**Files (fork):**
- Create: `be/src/connector/flight_connector.h`, `be/src/connector/flight_connector.cpp`, `be/src/connector/flight_chunk_converter.h`, `be/src/connector/flight_chunk_converter.cpp`, `be/src/connector/flight_scan_reader.h`, `be/src/connector/flight_scan_reader.cpp` (vendored from `flight-starrocks/cpp`, namespace `starrocks::connector`)
- Modify: `be/src/connector/CMakeLists.txt`, `be/src/connector/connector.cpp` (register), `be/CMakeLists.txt` (link Flight if Task 0 found it missing), `thirdparty/build-thirdparty.sh` only if Task 0 found `ARROW_FLIGHT` off
- Test (fork): `be/test/connector/flight_chunk_converter_test.cpp`, `be/test/connector/flight_connector_test.cpp`; register in `be/test/CMakeLists.txt`

**Interfaces:**
- Consumes: Thrift `TFlightScanNode`/`TFlightScanRange` (Task 6); pull reader `TicketStream` (Task 3, vendored).
- Produces:
  - `Status convert_batch(const arrow::RecordBatch& batch, const TupleDescriptor& desc, const std::vector<std::string>& columns, size_t chunk_size, std::vector<ChunkPtr>* out)` — maps by column name to slot; hard error on type mismatch (message names column, Arrow type and expected StarRocks type).
  - `class FlightDataSourceProvider : public DataSourceProvider`, `class FlightDataSource : public DataSource` with `open/get_next/close` as in the spec.

- [ ] **Step 1: Confirm signatures** — read `be/src/connector/es_connector.{h,cpp}` and `jdbc_connector.{h,cpp}` plus the `ConnectorManager` registration in `connector.cpp`; note exactly how a provider is created from a `TPlanNode`, how `get_next` signals end of data (`Status::EndOfFile()`), and how slot types are read from `_tuple_desc`. Adapt the code below to those signatures.
- [ ] **Step 2: Failing converter tests (gtest)** — build `arrow::RecordBatch`es with Int8/16/32/64, Float/Double, Decimal128, Bool, String, Binary, Date32, Timestamp (microsecond), each with and without nulls; convert against a tuple descriptor built the way the existing BE connector tests build one (copy the helper from the nearest existing test); assert values and null flags; assert a 10k-row batch splits into chunks of `chunk_size`; assert Arrow Int64 into an INT slot is a hard error naming the column.
- [ ] **Step 3: Implement the converter** with a per-slot-type switch that fills StarRocks columns (use `ColumnHelper`/`NullableColumn` builders as `jdbc_connector`'s result conversion does) and returns `Status::InternalError(...)` on mismatch.
- [ ] **Step 4: Failing data source test** — start an in-process Flight server (reuse the Task 3 fixture code) serving two batches; assert `get_next` returns the rows then `Status::EndOfFile`, that an empty stream gives EOF immediately, that a rejected ticket returns an error not containing the token/ticket, and that `close` cancels a partially read stream.
- [ ] **Step 5: Implement `FlightDataSource`** — `open`: parse the range's first location (`grpc://` or `grpc+tls://`), `FlightScanReader::Connect`, `OpenTicket`; `get_next`: loop `Next()`, convert, queue chunks, return one per call, `EndOfFile` at end; counters feed `raw_rows_read`, `num_rows_read`, `num_bytes_read`, IO time; `close`: `Cancel()` if open. Register in `connector.cpp` under the plan-node type from Task 6.
- [ ] **Step 6: Build and run BE tests** — `./scripts/build-starrocks-fork.sh --be` (long; run in the background and check logs) then `cd ../starrocks/be/build_Release && ctest -R flight` (use the fork's actual BE-test invocation noted in Task 0 if different).
- [ ] **Step 7:** Commit in the fork.

---

## Task 8: Integration, negative cases and docs

**Files:**
- Create (this repo): `scripts/run-starrocks-native-e2e.sh`, `docs/integrations/starrocks-native.md`
- Modify (this repo): `docs/integrations/starrocks.md` (link to the native doc), `flight-starrocks/README.md` (status line)

**Interfaces:**
- Consumes: FE/BE builds from the fork (`output/fe`, `output/be`), the `flight-server` jar and test data `flight-server/src/test/resources/test_db`.

- [ ] **Step 1: Write `scripts/run-starrocks-native-e2e.sh`** that (a) starts `flight-server` locally (see `scripts/run-starrocks-flight-e2e.sh` and `flight-server`'s run script for the exact command), (b) starts the fork's FE and BE from `output/` with a throwaway data dir under `$TMPDIR`, waits for BE readiness via `SHOW BACKENDS`, (c) runs the SQL in Step 2 with the `mariadb`/`mysql` client port 9030, (d) stops everything and fails non-zero on any assertion.
- [ ] **Step 2: Scenario SQL and expected results**
  - `CREATE EXTERNAL CATALOG flight PROPERTIES("type"="flight","flight.uri"="grpc://localhost:<port>", ...);` then `SHOW DATABASES FROM flight;` lists the test schema; `SHOW TABLES`; `DESC`.
  - `SELECT COUNT(*) FROM flight.<db>.lineitem;` equals the row count from the same query run directly against the server.
  - `SELECT l_orderkey, l_quantity FROM ... WHERE l_orderkey = <k>` returns the rows the server returns directly; `EXPLAIN` shows the projection (only those two columns) and the pushed WHERE.
  - A predicate that cannot be pushed (e.g. `WHERE l_comment LIKE '%x%'`) returns the correct rows; `EXPLAIN` shows it as a residual.
  - Negative: wrong token → query fails, error text has no token; unreachable URI → clear connection error; unsupported-type table → error at `DESC`/query naming the column; `grep` of FE/BE logs and `SHOW CREATE CATALOG` output finds no token string.
- [ ] **Step 3: Run it; fix failures at their source**, adding a regression unit test in the owning task's test file for any bug found.
- [ ] **Step 4: Docs** — `docs/integrations/starrocks-native.md` with the build commands, catalog properties, supported types, pushdown rules, limits (no per-user identity), and the exact E2E command and result summary from Step 3; update the two existing docs to point at it.
- [ ] **Step 5: Commit** (this repo) and push the fork branch only if the user asks.

---

## Self-review notes (plan author)

- Spec coverage: catalog registration, metadata, types, projection + filter pushdown, endpoints→ranges, credentials, BE source/converter, residual guard, tests 1–4, doc, build script — Tasks 0–8. Risk gate on Arrow Flight in BE thirdparty and BE buildability — Task 0.
- Tasks 5–7 depend on StarRocks internals not yet cloned; each starts with an explicit "confirm real signatures" step and the code shapes must be adjusted to what it finds. This is the one deliberate deferral and is bounded by those steps.
- Type consistency: `ScanPredicate`/`Literal` (Task 1) used by Tasks 2, 6; `TicketStream`/`OpenTicket` (Task 3) used by Task 7; `FlightTable`/`FlightTypeMapper` (Task 5) used by Task 6; `TFlightScanNode`/`TFlightScanRange` (Task 6) used by Task 7.
