#!/usr/bin/env python3
"""Scenario for the native `flight` catalog against a running StarRocks FE (and BE when --with-be).

Usage: starrocks_native_e2e.py FLIGHT_PORT [--with-be] [--fe-port 9030] [--arrow-port PORT]
FLIGHT_PORT is a server sending DECIMAL columns in the narrow wire form; --arrow-port is a second server over the
same data sending standard Arrow decimals, which the decimal results are compared against.
Exits non-zero on the first failed expectation. Requires `pymysql`.
"""
import sys

import pymysql

SECRET = "live-secret-token-9f3a"


def main() -> int:
    flight_port = int(sys.argv[1])
    with_be = "--with-be" in sys.argv
    fe_port = 9030
    if "--fe-port" in sys.argv:
        fe_port = int(sys.argv[sys.argv.index("--fe-port") + 1])
    arrow_port = None
    if "--arrow-port" in sys.argv:
        arrow_port = int(sys.argv[sys.argv.index("--arrow-port") + 1])

    conn = pymysql.connect(host="127.0.0.1", port=fe_port, user="root", autocommit=True)
    cur = conn.cursor()

    def run(sql):
        cur.execute(sql)
        return cur.fetchall()

    failures = []

    def check(name, ok, detail=""):
        print(("PASS " if ok else "FAIL ") + name + (" -- " + detail if detail and not ok else ""))
        if not ok:
            failures.append(name)

    uri = "grpc://127.0.0.1:%d" % flight_port
    for catalog in ("flight_native", "flight_token", "flight_dead", "flight_noagg", "flight_arrow", "flight_nojoin"):
        run("DROP CATALOG IF EXISTS " + catalog)
    if arrow_port is not None:
        run('CREATE EXTERNAL CATALOG flight_arrow PROPERTIES("type"="flight","flight.uri"="grpc://127.0.0.1:%d",'
            '"flight.aggregation_pushdown"="false")' % arrow_port)
    run('CREATE EXTERNAL CATALOG flight_noagg PROPERTIES("type"="flight","flight.uri"="%s",'
        '"flight.aggregation_pushdown"="false")' % uri)
    run('CREATE EXTERNAL CATALOG flight_nojoin PROPERTIES("type"="flight","flight.uri"="%s",'
        '"flight.join_key_reduction"="false")' % uri)
    run('CREATE EXTERNAL CATALOG flight_native PROPERTIES("type"="flight","flight.uri"="%s")' % uri)

    dbs = [r[0] for r in run("SHOW DATABASES FROM flight_native")]
    check("SHOW DATABASES lists the Flight schema", "test_schema" in dbs, str(dbs))
    tables = [r[0] for r in run("SHOW TABLES FROM flight_native.test_schema")]
    check("SHOW TABLES lists the Flight table", "test_table" in tables, str(tables))
    columns = run("DESC flight_native.test_schema.test_table")
    check("DESC returns columns with StarRocks types", len(columns) > 0 and all(c[1] for c in columns), str(columns))
    print("columns:", [(c[0], c[1]) for c in columns])

    # credentials never show up in SHOW CREATE CATALOG
    run('CREATE EXTERNAL CATALOG flight_token PROPERTIES("type"="flight","flight.uri"="%s",'
        '"flight.auth.type"="token","flight.auth.token"="%s")' % (uri, SECRET))
    created = run("SHOW CREATE CATALOG flight_token")[0][1]
    check("SHOW CREATE CATALOG hides the bearer token", SECRET not in created, created)

    # an unreachable server fails clearly and without leaking the token
    run('CREATE EXTERNAL CATALOG flight_dead PROPERTIES("type"="flight","flight.uri"="grpc://127.0.0.1:1",'
        '"flight.auth.type"="token","flight.auth.token"="%s")' % SECRET)
    try:
        run("SHOW DATABASES FROM flight_dead")
        check("unreachable Flight server is an error", False, "no error raised")
    except pymysql.MySQLError as e:
        msg = str(e)
        check("unreachable Flight server is a clear error", "127.0.0.1:1" in msg or "Flight" in msg, msg)
        check("error message does not contain the token", SECRET not in msg, msg)

    if with_be:
        table = "flight_native.test_schema.test_table"
        count = run("SELECT COUNT(*) FROM " + table)[0][0]
        check("COUNT(*) reads every row", count == 1000, str(count))

        # Strings and several fixed-width types go through the Arrow -> chunk converter.
        rows = run("SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, "
                   "date_string_col, string_col, timestamp_col, year, month FROM " + table + " ORDER BY id LIMIT 5")
        check("rows come back with all columns", len(rows) == 5 and all(len(r) == 13 for r in rows), str(rows))
        check("string columns are populated", all(r[8] and r[9] is not None for r in rows), str(rows))

        # A filter handled by the Flight server must give the same rows as StarRocks evaluating it itself.
        # abs()/upper() cannot be pushed down, so those variants run inside StarRocks.
        pushed = run("SELECT COUNT(*) FROM %s WHERE int_col > 3" % table)[0][0]
        local = run("SELECT COUNT(*) FROM %s WHERE abs(int_col) > 3" % table)[0][0]
        check("pushed numeric filter matches StarRocks' own evaluation", pushed == local and 0 < pushed < 1000,
              "pushed=%s local=%s" % (pushed, local))
        sample = run("SELECT string_col FROM %s ORDER BY id LIMIT 1" % table)[0][0]
        pushed = run("SELECT COUNT(*) FROM %s WHERE string_col = '%s'" % (table, sample))[0][0]
        local = run("SELECT COUNT(*) FROM %s WHERE concat(string_col, '') = '%s'" % (table, sample))[0][0]
        check("pushed string filter matches StarRocks' own evaluation", pushed == local and pushed > 0,
              "pushed=%s local=%s" % (pushed, local))
        pushed = run("SELECT COUNT(*) FROM %s WHERE int_col IN (1, 2) OR bool_col" % table)[0][0]
        local = run("SELECT COUNT(*) FROM %s WHERE abs(int_col) IN (1, 2) OR bool_col" % table)[0][0]
        check("pushed OR / IN filter matches StarRocks' own evaluation", pushed == local, "%s vs %s" % (pushed, local))
        empty = run("SELECT COUNT(*) FROM %s WHERE int_col > 1000000" % table)[0][0]
        check("a filter matching nothing counts 0", empty == 0, str(empty))

        # Aggregation pushdown: the same aggregate through a catalog with pushdown switched off is the reference.
        ref = "flight_noagg.test_schema.test_table"

        def same(a, b):
            if len(a) != len(b):
                return False
            for ra, rb in zip(a, b):
                for x, y in zip(ra, rb):
                    if isinstance(x, float) or isinstance(y, float):
                        if x is None or y is None or abs(x - y) > 1e-6 * max(1.0, abs(x), abs(y)):
                            return False
                    elif x != y:
                        return False
            return True

        shapes = [
            ("ungrouped count/min/max", "SELECT COUNT(*), COUNT(int_col), MIN(int_col), MAX(bigint_col) FROM %s"),
            ("ungrouped float sum and avg", "SELECT SUM(double_col), SUM(float_col), AVG(int_col), AVG(double_col) FROM %s"),
            ("grouped by one key", "SELECT int_col, COUNT(*), SUM(double_col), MIN(smallint_col), MAX(tinyint_col) "
                                   "FROM %s GROUP BY int_col ORDER BY int_col"),
            ("grouped by two keys", "SELECT year, month, COUNT(*), AVG(bigint_col) FROM %s "
                                    "GROUP BY year, month ORDER BY year, month"),
            ("grouped boolean key", "SELECT bool_col, COUNT(*), MAX(id) FROM %s GROUP BY bool_col ORDER BY bool_col"),
            ("filtered grouped", "SELECT int_col, COUNT(*), SUM(double_col) FROM %s WHERE id > 100 AND bool_col "
                                 "GROUP BY int_col ORDER BY int_col"),
            ("sum of a product of columns", "SELECT int_col, COUNT(*), SUM(double_col * double_col) FROM %s "
                                            "GROUP BY int_col ORDER BY int_col"),
            ("sum of a CASE expression", "SELECT year, SUM(CASE WHEN int_col > 3 THEN double_col ELSE CAST(0 AS DOUBLE) END) "
                                         "FROM %s GROUP BY year ORDER BY year"),
            ("empty input, ungrouped", "SELECT COUNT(*), COUNT(int_col), SUM(double_col), MIN(int_col), AVG(int_col) "
                                       "FROM %s WHERE id < 0"),
            ("empty input, grouped", "SELECT int_col, COUNT(*) FROM %s WHERE id < 0 GROUP BY int_col"),
        ]
        for name, template in shapes:
            got = run(template % table)
            want = run(template % ref)
            check("aggregate pushdown matches the unpushed result: " + name, same(got, want),
                  "pushed=%s reference=%s" % (got[:3], want[:3]))
        check("grouped aggregate returned groups", len(run(shapes[2][1] % table)) > 1)
        empty_template = dict(shapes)["empty input, ungrouped"]
        empty_row = run(empty_template % table)[0]
        check("empty ungrouped aggregate is COUNT 0 and NULL sum", tuple(empty_row[:3]) == (0, 0, None), str(empty_row))

        agg_explain = "\n".join(r[0] for r in run(
            "EXPLAIN SELECT int_col, COUNT(*), SUM(double_col) FROM %s GROUP BY int_col" % table))
        check("EXPLAIN shows the pushed partial aggregate",
              "GROUP BY int_col" in agg_explain and "AS __agg0" in agg_explain, agg_explain)
        ref_explain = "\n".join(r[0] for r in run(
            "EXPLAIN SELECT int_col, COUNT(*) FROM %s GROUP BY int_col" % ref))
        check("switching the property off keeps the aggregate in StarRocks", "__agg0" not in ref_explain, ref_explain)
        expr_explain = "\n".join(r[0] for r in run(
            "EXPLAIN SELECT int_col, SUM(double_col * double_col) FROM %s GROUP BY int_col" % table))
        check("EXPLAIN shows the product pushed into the partial aggregate", "(double_col * double_col)" in expr_explain,
              expr_explain)
        int_sum = "\n".join(r[0] for r in run("EXPLAIN SELECT SUM(bigint_col) FROM %s" % table))
        check("integral SUM is not pushed (the server sums as DOUBLE)", "__agg0" not in int_sum, int_sum)

        # DECIMAL aggregates, expressions and AVG: pushed results must equal the unpushed ones digit for digit.
        decimal_tables = [r[0] for r in run("SHOW TABLES FROM flight_native.test_schema")]
        if "decimal_table" in decimal_tables:
            dtable = "flight_native.test_schema.decimal_table"
            dref = "flight_noagg.test_schema.decimal_table"
            dshapes = [
                ("decimal sum/min/max/avg grouped", "SELECT k, COUNT(*), SUM(price), MIN(price), MAX(price), AVG(price) "
                                                    "FROM %s GROUP BY k ORDER BY k"),
                ("decimal expression sums", "SELECT k, SUM(price * (1 - disc)), SUM(price * (1 - disc) * (1 + tax)), "
                                            "SUM(disc) FROM %s GROUP BY k ORDER BY k"),
                ("decimal ungrouped", "SELECT SUM(price * disc), AVG(disc), MIN(tax), MAX(tax) FROM %s"),
                ("decimal CASE sum", "SELECT k, SUM(CASE WHEN mode = 'MAIL' THEN price ELSE 0 END) FROM %s "
                                     "GROUP BY k ORDER BY k"),
                ("decimal filtered", "SELECT mode, SUM(price), AVG(price) FROM %s WHERE disc > 0.03 GROUP BY mode "
                                     "ORDER BY mode"),
            ]
            for name, template in dshapes:
                got = run(template % dtable)
                want = run(template % dref)
                check("decimal aggregate pushdown matches the unpushed result: " + name, same(got, want),
                      "pushed=%s reference=%s" % (got[:3], want[:3]))
            if arrow_port is not None:
                # narrow wire form against standard Arrow decimals, and against values computed here
                dplain = "flight_arrow.test_schema.decimal_table"
                for name, template in [("raw rows", "SELECT k, price, disc, tax, mode FROM %s ORDER BY k, price, disc, tax, mode"),
                                       ("filtered rows", "SELECT price, tax FROM %s WHERE disc > 0.04 AND price IS NOT NULL "
                                                         "ORDER BY price, tax")]:
                    check("narrow decimal wire form matches Arrow decimals: " + name,
                          same(run(template % dref), run(template % dplain)))
                expected = sum(round(((i * 37 % 10000) / 100.0) * 100) for i in range(1000) if i % 17 != 0)
                got = run("SELECT SUM(price) FROM %s" % dref)[0][0]
                check("narrow decimals sum to the independently computed total",
                      abs(float(got) * 100 - expected) < 0.5, "got=%s expected=%s" % (got, expected / 100))
            decimal_explain = "\n".join(r[0] for r in run(
                "EXPLAIN SELECT k, SUM(price * (1 - disc)) FROM %s GROUP BY k" % dtable))
            check("EXPLAIN shows the decimal expression pushed with an exact result type",
                  "AS DECIMAL(38," in decimal_explain and "(price * (1 - disc))" in decimal_explain, decimal_explain)

        # Join key reduction: a selective filter on one side becomes an IN list on the other side's scan.
        join_sql = ("SELECT COUNT(*), SUM(a.bigint_col), MIN(a.double_col) FROM %s a JOIN %s b ON a.id = b.id "
                    "WHERE b.int_col = 3")
        reduced = run(join_sql % (table, table))
        unreduced = run(join_sql % ("flight_nojoin.test_schema.test_table", "flight_nojoin.test_schema.test_table"))
        check("join key reduction does not change the join result", same(reduced, unreduced) and reduced[0][0] > 0,
              "reduced=%s unreduced=%s" % (reduced, unreduced))
        join_explain = "\n".join(r[0] for r in run("EXPLAIN " + join_sql % (table, table)))
        check("EXPLAIN shows the dimension keys pushed to the other scan as an IN list",
              "id IN (" in join_explain, join_explain)
        nojoin_explain = "\n".join(r[0] for r in run(
            "EXPLAIN " + join_sql % ("flight_nojoin.test_schema.test_table", "flight_nojoin.test_schema.test_table")))
        check("switching the property off keeps the plan unreduced", "id IN (" not in nojoin_explain, nojoin_explain)

        first = columns[0][0]
        explain = "\n".join(r[0] for r in run("EXPLAIN SELECT %s FROM %s WHERE %s IS NOT NULL" % (first, table, first)))
        check("EXPLAIN shows the pushed projection and filter",
              "SELECT %s FROM test_schema.test_table WHERE (%s IS NOT NULL)" % (first, first) in explain, explain)
        check("EXPLAIN does not show the token", SECRET not in explain)
    else:
        try:
            run("SELECT COUNT(*) FROM flight_native.test_schema.test_table")
            check("query without a backend fails", False, "unexpectedly succeeded")
        except pymysql.MySQLError as e:
            check("query without a backend fails with a planning error", "backend" in str(e).lower(), str(e))

    for catalog in ("flight_native", "flight_token", "flight_dead", "flight_noagg", "flight_arrow", "flight_nojoin"):
        run("DROP CATALOG IF EXISTS " + catalog)
    print("RESULT:", "FAILED %s" % failures if failures else "all checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
