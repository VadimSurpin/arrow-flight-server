#!/usr/bin/env python3
"""profile_scans.py OUT Q...: StarRocks query profile of TPC-DS queries over Flight and over direct FILES() views (schema tpcds_sf100_asis, cache off).
Per query and mode: wall, summed scan time over scan operators, rows output by scans, runtime-filter input/output rows, number of scans. -> OUT (jsonl)."""
import json, re, sys, time
import pymysql
sys.path.insert(0, ".")
import suites
c = pymysql.connect(host="127.0.0.1", port=19030, user="root", password="", autocommit=True, read_timeout=900)
ex = lambda s: (lambda cur: (cur.execute(s), cur.fetchall())[1])(c.cursor())  # noqa: E731
unit = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0}


def secs(t):
    return sum(float(v) * unit[u] for v, u in re.findall(r"([\d.]+)(ms|us|ns|m|s)", t))


def num(t):
    return int(re.search(r"\((\d+)\)", t).group(1)) if "(" in t else int(float(t.replace("K", "e3").replace("M", "e6").replace("B", "e9")))


def parse(txt):
    txt = re.sub(r"\x1b\[[0-9;]*m", "", txt)
    scan_t, out_rows, rf_in, rf_out, n = 0.0, 0, 0, 0, 0
    lines = [re.sub(r"^[│├└─┬ ]+", "", l) for l in txt.splitlines()]
    for i, l in enumerate(lines):
        if re.match(r"(FLIGHT_SCAN|FILE_SCAN|CONNECTOR_SCAN|OLAP_SCAN) \(id=\d+\)\s*$", l):
            n += 1
            for m in lines[i + 1:i + 8]:
                s = re.search(r"ScanTime: ([\d.a-z]+)", m)
                if s: scan_t += secs(s.group(1))
                o = re.match(r"OutputRows: (\S+)", m)
                if o: out_rows += num(o.group(1))
                r = re.match(r"RuntimeFilter: (\S+) \((\d+)\) -> (\S+) \((\d+)\)", m)
                if r: rf_in += int(r.group(2)); rf_out += int(r.group(4))
    return {"scans": n, "scan_s": round(scan_t, 1), "scan_out_rows": out_rows, "rf_in": rf_in, "rf_out": rf_out}


out = open(sys.argv[1], "a")
for q in sys.argv[2:]:
    sql = suites.query_sql("tpcds", q, "starrocks")
    for mode in ("flight", "direct"):
        ex("SET CATALOG flight" if mode == "flight" else "SET CATALOG default_catalog")
        ex("USE tpcds_sf100_asis" if mode == "flight" else "USE direct_tpcds_sf100_asis")
        for s in ("SET enable_profile=true", "SET pipeline_profile_level=1", "SET new_planner_optimize_timeout=300000", "SET query_timeout=300",
                  "SET enable_scan_datacache=false", "SET enable_populate_datacache=false"):
            ex(s)
        rec = {"query": q, "mode": mode}
        try:
            t = time.time(); ex(sql); rec["wall"] = round(time.time() - t, 1)
            time.sleep(3)
            rec.update(parse("\n".join(x[0] for x in ex("ANALYZE PROFILE FROM '%s'" % ex("select last_query_id()")[0][0]))))
        except Exception as e:
            rec["error"] = str(e)[:150]
        out.write(json.dumps(rec) + "\n"); out.flush()
        print(rec, flush=True)
