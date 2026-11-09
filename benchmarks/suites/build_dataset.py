#!/usr/bin/env python3
"""build_dataset.py: one row per (engine, schema, query, config) = plan features + data features of the tables the query scans + the label
-> runs/features/dataset-<engine>.jsonl. Tables per query come from the Spark plan (BatchScan <schema>.<table>); the same SQL runs on both engines."""
import collections, json, math, pathlib, re

F = pathlib.Path(__file__).resolve().parent / "runs" / "features"
tables = {}
for l in open(F / "data-tables.jsonl"):
    r = json.loads(l)
    tables[(r["schema"], r["table"])] = r
scanned = {}
for l in open(F / "spark-plans.jsonl"):
    r = json.loads(l)
    if not r["error"]:
        scanned[(r["schema"], r["query"])] = sorted(set(m[1] for m in re.findall(r"BatchScan (\S+?)\.(\S+)", r["plan"])))


def data_features(schema, query):
    ts = [tables[(schema, t)] for t in scanned.get((schema, query), []) if (schema, t) in tables]
    if not ts:
        return None
    rows = [t["rows"] for t in ts]
    big = max(ts, key=lambda t: t["rows"])
    return {"n_tables": len(ts), "rows_sum": sum(rows), "rows_max": max(rows), "rows_min": min(rows),
            "rows_ratio": max(rows) / max(min(rows), 1), "log_rows_max": math.log10(max(rows) + 1),
            "bytes_sum": sum(t["bytes"] for t in ts), "files_sum": sum(t["files"] for t in ts), "files_big": big["files"],
            "bytes_per_row_big": big["bytes"] / max(big["rows"], 1), "file_cv_big": big["file_cv"],
            "overlap_big": big.get("key_overlap", 0.0), "overlap_per_file_big": big.get("key_overlap", 0.0) / max(big["files"], 1),
            "ndv_big": big.get("key_ndv", 0), "ndv_per_row_big": big.get("key_ndv", 0) / max(big["rows"], 1), "columns_big": big.get("columns", 0)}


cost = {e: {(r["schema"], r["query"]): r["c"] for r in map(json.loads, open(F / ("cost-%s.jsonl" % e)))} for e in ("spark", "starrocks")}
SHARED = ("c_sel_min", "c_sel_mean", "c_sel_big", "c_n_filtered_scans")  # selectivity from the Spark estimates, valid for both engines (same SQL, same Flight statistics)

for engine in ("spark", "starrocks"):
    plan = {(r["schema"], r["query"]): r for r in map(json.loads, open(F / ("features-%s.jsonl" % engine)))}
    out, miss = [], 0
    for l in open(F / ("labels-%s.jsonl" % engine)):
        lab = json.loads(l)
        k = (lab["schema"], lab["query"])
        d = data_features(*k)
        c = dict(cost[engine].get(k) or {})
        if engine == "starrocks":
            c.update({"s_" + n[2:]: v for n, v in (cost["spark"].get(k) or {}).items() if n in SHARED})
        if k not in plan or d is None or not c:
            miss += 1
            continue
        out.append({"engine": engine, "suite": plan[k]["suite"], "schema": k[0], "query": k[1], "config": lab["config"], "ratio": lab["ratio"],
                    "default_s": lab["default_s"], "win": lab["win"], "loss": lab["loss"], "plan": plan[k]["f"], "data": d, "cost": c})
    (F / ("dataset-%s.jsonl" % engine)).write_text("".join(json.dumps(o) + "\n" for o in out))
    print(engine, len(out), "rows,", miss, "labels without features;", collections.Counter(o["suite"] for o in out))
