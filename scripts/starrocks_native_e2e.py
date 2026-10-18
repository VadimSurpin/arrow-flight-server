#!/usr/bin/env python3
"""Scenario for the native `flight` catalog against a running StarRocks FE (and BE when --with-be).

Usage: starrocks_native_e2e.py FLIGHT_PORT [--with-be] [--fe-port 9030]
Exits non-zero on the first failed expectation. Requires `pymysql`.
"""
import sys

import pymysql

SECRET = "live-secret-token-9f3a"


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flight_port = int(args[0])
    with_be = "--with-be" in sys.argv
    fe_port = 9030
    if "--fe-port" in sys.argv:
        fe_port = int(sys.argv[sys.argv.index("--fe-port") + 1])

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
    for catalog in ("flight_native", "flight_token", "flight_dead"):
        run("DROP CATALOG IF EXISTS " + catalog)
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

    for catalog in ("flight_native", "flight_token", "flight_dead"):
        run("DROP CATALOG IF EXISTS " + catalog)
    print("RESULT:", "FAILED %s" % failures if failures else "all checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
