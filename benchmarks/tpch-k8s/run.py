#!/usr/bin/env python3
"""Runs the 22 TPC-H queries against StarRocks in several modes and compares results and timings.

Usage: run.py FE_HOST FE_PORT [--modes direct,flight] [--queries 1,3,6] [--runs 3] [--timeout 1800] [--out results.json]
Modes: direct = Parquet on MinIO through FILES() views; flight = `flight_c` catalog (Flight SQL cluster);
flight_noagg = same with aggregation pushdown off (needs catalog `flight_c_noagg`, created on demand).
Run 1 is cold, the reported time is the median of the runs; results are compared across modes on run 1.
"""
import argparse
import decimal
import json
import pathlib
import statistics
import time

import pymysql

HERE = pathlib.Path(__file__).parent
MODE_DB = {"direct": "direct", "flight": "flight_c.sf100", "flight_noagg": "flight_c_noagg.sf100"}


def normalise(rows):
    """Numbers become floats, everything else strings, so results compare across modes."""
    def cell(x):
        if x is None:
            return None
        if isinstance(x, (int, float, decimal.Decimal)) and not isinstance(x, bool):
            return float(x)
        return str(x)
    return [tuple(cell(x) for x in r) for r in rows]


def same(a, b):
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb):
            if isinstance(x, float) and isinstance(y, float):
                if abs(x - y) > max(0.011, 1e-6 * max(abs(x), abs(y))):
                    return False
            elif x != y:
                return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("port", type=int)
    ap.add_argument("--modes", default="direct,flight")
    ap.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--out", default="results.json")
    args = ap.parse_args()
    modes = args.modes.split(",")
    queries = [int(q) for q in args.queries.split(",")]

    conn = pymysql.connect(host=args.host, port=args.port, user="root", autocommit=True,
                           read_timeout=args.timeout, connect_timeout=10)
    cur = conn.cursor()
    cur.execute("SET query_timeout = %d" % args.timeout)
    if "flight_noagg" in modes:
        cur.execute("DROP CATALOG IF EXISTS flight_c_noagg")
        cur.execute('CREATE EXTERNAL CATALOG flight_c_noagg PROPERTIES("type"="flight",'
                    '"flight.uri"="grpc://flight-0.flight:32010","flight.aggregation_pushdown"="false")')

    results = {}
    for q in queries:
        sql = (HERE / "queries" / ("q%02d.sql" % q)).read_text()
        first_rows = {}
        for mode in modes:
            cur.execute("USE " + MODE_DB[mode])
            times, error = [], None
            for run in range(args.runs):
                t = time.time()
                try:
                    cur.execute(sql)
                    rows = cur.fetchall()
                except pymysql.MySQLError as e:
                    error = str(e)[:300]
                    break
                times.append(round(time.time() - t, 2))
                if run == 0:
                    first_rows[mode] = normalise(rows)
            results.setdefault(q, {})[mode] = {"times": times, "median": statistics.median(times) if times else None,
                                               "rows": len(first_rows.get(mode, [])), "error": error}
            print("q%02d %-12s %s %s" % (q, mode, times, error or ""), flush=True)
        base = modes[0]
        for mode in modes[1:]:
            if base in first_rows and mode in first_rows:
                results[q][mode]["matches_" + base] = same(first_rows[base], first_rows[mode])
                print("q%02d %s matches %s: %s" % (q, mode, base, results[q][mode]["matches_" + base]), flush=True)
        pathlib.Path(args.out).write_text(json.dumps(results, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
