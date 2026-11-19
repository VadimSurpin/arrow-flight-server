#!/usr/bin/env python3
"""build_cost_features.py: size/selectivity features from the optimizer estimates (runs/features/<engine>-cost.jsonl) -> runs/features/cost-<engine>.jsonl.
Spark: logical-plan tree with Statistics(sizeInBytes, rowCount) per node; scans carry the sizes the Flight server reports (after pushdown), so
selectivity = estimated scan rows / table rows (data-tables.jsonl). Join sides, outputs, aggregate input/output. StarRocks: cardinality per node; the
build side of a join is the node marked |---- under it."""
import json, math, pathlib, re

F = pathlib.Path(__file__).resolve().parent / "runs" / "features"
UNIT = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40, "PiB": 1 << 50, "EiB": 1 << 60}
STAT = re.compile(r"Statistics\(sizeInBytes=([\d.]+(?:E[+-]?\d+)?) (B|KiB|MiB|GiB|TiB|PiB|EiB)(?:, rowCount=([\d.]+(?:E[+-]?\d+)?))?")
tables = {}
for l in open(F / "data-tables.jsonl"):
    r = json.loads(l)
    tables[(r["schema"], r["table"])] = r
L = lambda x: math.log10(x + 1)  # noqa: E731


def spark_tree(text):
    sec = text.split("Optimized Logical Plan ==")[-1].split("== Physical Plan ==")[0]
    nodes, stack = [], []
    for line in sec.splitlines():
        m = re.match(r"^([ :|+\-]*)(\S.*)$", line)
        if not m or "Statistics(" not in line:
            continue
        col = len(m.group(1))
        s = STAT.search(line)
        n = {"col": col, "kind": m.group(2).split(" ")[0].split("[")[0], "bytes": float(s.group(1)) * UNIT[s.group(2)],
             "rows": float(s.group(3)) if s.group(3) else None, "kids": [], "text": m.group(2)}
        while stack and stack[-1]["col"] >= col:
            stack.pop()
        if stack:
            stack[-1]["kids"].append(n)
        stack.append(n)
        nodes.append(n)
    return nodes


def spark(schema, text):
    nodes = spark_tree(text)
    if not nodes:
        return None
    f = {}
    scans = [n for n in nodes if n["kind"].startswith("Relation")]
    sel = []
    for n in scans:
        t = re.search(r"\] (\S+?)\.(\S+?),", n["text"] + ",") or re.search(r" (\S+?)\.(\S+?),", n["text"])
        tb = tables.get((schema, t.group(2))) if t else None
        if tb and n["rows"] is not None and tb["rows"]:
            sel.append((min(n["rows"] / tb["rows"], 1.0), tb["rows"]))
    rows = [n["rows"] for n in scans if n["rows"] is not None] or [0]
    f.update({"c_scan_rows_sum": L(sum(rows)), "c_scan_rows_max": L(max(rows)), "c_scan_rows_min": L(min(rows)),
              "c_scan_bytes_sum": L(sum(n["bytes"] for n in scans)), "c_scan_bytes_max": L(max(n["bytes"] for n in scans)) if scans else 0,
              "c_sel_min": min((s for s, _ in sel), default=1.0), "c_sel_mean": sum(s for s, _ in sel) / len(sel) if sel else 1.0,
              "c_sel_big": max(sel, key=lambda x: x[1])[0] if sel else 1.0, "c_n_filtered_scans": sum(1 for s, _ in sel if s < 0.9)})
    joins = [n for n in nodes if n["kind"] == "Join" and len(n["kids"]) == 2]
    small, out_rows, expand, ratio = [], [], [], []
    for j in joins:
        a, b = j["kids"]
        small.append(min(a["bytes"], b["bytes"]))
        if j["rows"] is not None and a["rows"] and b["rows"]:
            out_rows.append(j["rows"]); expand.append(j["rows"] / max(a["rows"], b["rows"]))
            ratio.append(max(a["rows"], b["rows"]) / max(min(a["rows"], b["rows"]), 1))
    f.update({"c_joins": len(joins), "c_small_min": L(min(small)) if small else 0, "c_small_max": L(max(small)) if small else 0,
              "c_small_lt10m": sum(s < 10 << 20 for s in small), "c_small_lt64m": sum(s < 64 << 20 for s in small),
              "c_small_lt256m": sum(s < 256 << 20 for s in small), "c_small_lt1g": sum(s < 1 << 30 for s in small),
              "c_join_out_max": L(max(out_rows)) if out_rows else 0, "c_join_out_sum": L(sum(out_rows)),
              "c_join_expand_max": max(expand) if expand else 0, "c_join_ratio_max": L(max(ratio)) if ratio else 0})
    aggs = [n for n in nodes if n["kind"] == "Aggregate" and n["kids"] and n["rows"] and n["kids"][0]["rows"]]
    red = [n["kids"][0]["rows"] / max(n["rows"], 1) for n in aggs]
    f.update({"c_aggs": len(aggs), "c_agg_in_max": L(max(n["kids"][0]["rows"] for n in aggs)) if aggs else 0,
              "c_agg_out_max": L(max(n["rows"] for n in aggs)) if aggs else 0, "c_agg_reduction_max": L(max(red)) if red else 0,
              "c_root_rows": L(nodes[0]["rows"] or 0), "c_missing_rows": sum(n["rows"] is None for n in nodes)})
    return f


def starrocks(schema, text):
    cur, node = None, []
    for line in text.splitlines():
        m = re.match(r"^([\s|\-]*?)(\d+):([A-Z][A-Za-z\- ]+?)(?: \(.*\))?\s*$", line)
        if m:
            cur = {"build": "|----" in m.group(1), "kind": m.group(3).strip(), "card": None}
            node.append(cur)
            continue
        c = re.search(r"cardinality: (\d+)", line)
        if c and cur and cur["card"] is None:
            cur["card"] = int(c.group(1))
    if not node:
        return None
    rows = lambda kinds: [n["card"] for n in node if n["card"] is not None and n["kind"] in kinds]  # noqa: E731
    scans, joins, aggs = rows({"SCAN FLIGHT"}) or [0], rows({"HASH JOIN", "NESTLOOP JOIN", "MERGE JOIN"}), rows({"AGGREGATE"})
    builds = [n["card"] for n in node if n["build"] and n["card"] is not None]
    return {"c_scan_rows_sum": L(sum(scans)), "c_scan_rows_max": L(max(scans)), "c_scan_rows_min": L(min(scans)), "c_joins": len(joins),
            "c_join_out_max": L(max(joins)) if joins else 0, "c_join_out_sum": L(sum(joins)),
            "c_build_min": L(min(builds)) if builds else 0, "c_build_max": L(max(builds)) if builds else 0, "c_build_sum": L(sum(builds)),
            "c_build_lt1m": sum(b < 1_000_000 for b in builds), "c_build_lt10m": sum(b < 10_000_000 for b in builds),
            "c_build_lt100m": sum(b < 100_000_000 for b in builds),
            "c_join_expand_max": max([j / max(max(scans), 1) for j in joins], default=0),
            "c_aggs": len(aggs), "c_agg_out_max": L(max(aggs)) if aggs else 0,
            "c_agg_reduction_max": L(max(scans) / max(min(aggs), 1)) if aggs else 0, "c_root_rows": L(node[0]["card"] or 0)}


if __name__ == "__main__":
    for engine, fn in (("spark", spark), ("starrocks", starrocks)):
        p = F / ("%s-cost.jsonl" % engine)
        if not p.exists():
            continue
        out, bad = [], 0
        for l in p.read_text().splitlines():
            r = json.loads(l)
            f = None if r["error"] else fn(r["schema"], r["cost"])
            if f is None:
                bad += 1
                continue
            out.append({"engine": engine, "schema": r["schema"], "query": r["query"], "c": f})
        (F / ("cost-%s.jsonl" % engine)).write_text("".join(json.dumps(o) + "\n" for o in out))
        print(engine, len(out), "ok,", bad, "without features")
