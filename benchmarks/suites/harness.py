#!/usr/bin/env python3
"""Runs suite queries through the Spark driver (spark_server.py, POST /run) and appends one JSON record per run to a
JSONL file, in the same format for every suite.

  harness.py run --suite tpcds --schema tpcds_sf100_sorted --queries 1-99 --configs configs/phase1.json --reps 2 \\
                 --out runs/tpcds-sorted.jsonl [--kubectl PATH] [--modes direct,flight] [--url http://127.0.0.1:18000]
  harness.py check runs/tpcds-sorted.jsonl       # result mismatches between configurations, invalid runs, failures
  harness.py summary runs/tpcds-sorted.jsonl     # median wall per query, mode and configuration

Record types (field "type"):
  features  once per (suite, schema, query): plan operator counts from EXPLAIN FORMATTED and a plan hash
  run       one execution: suite, schema, query, config, mode, rep, warmup, wall, error, row_count, result_hash, rows
            (when small), valid/invalid_reason, cluster state (pod restarts, ready nodes, image digests), resources
Runs are dropped from training when valid is false: a pod restarted or a node was NotReady during the run, or the
error mentions memory or a lost connection (those say something about the cluster, not about the configuration).
"""
import argparse
import hashlib
import json
import pathlib
import random
import re
import statistics
import subprocess
import sys
import time
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import suites  # noqa: E402

OPERATORS = ["Scan", "BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin", "HashAggregate",
             "ObjectHashAggregate", "Sort", "Window", "Exchange", "Expand", "Union", "TakeOrderedAndProject", "Generate"]
BAD_ERROR = re.compile(r"memory|OutOfMemory|connection reset|UNAVAILABLE|Connection refused|NoRouteToHost", re.I)
MAX_ROWS_IN_RECORD = 50


def post(url, body, timeout):
    req = urllib.request.Request(url + "/run", json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


class Cluster:
    """Pod restarts, ready nodes and image digests via kubectl; a no-op without --kubectl."""

    def __init__(self, kubectl, namespace):
        self.kubectl, self.namespace = kubectl, namespace

    def snapshot(self):
        if not self.kubectl:
            return {}
        try:
            pods = json.loads(subprocess.run([self.kubectl, "-n", self.namespace, "get", "pods", "-o", "json"], check=True,
                                             capture_output=True, text=True, timeout=60).stdout)["items"]
            nodes = json.loads(subprocess.run([self.kubectl, "get", "nodes", "-o", "json"], check=True,
                                              capture_output=True, text=True, timeout=60).stdout)["items"]
        except Exception as e:  # kubectl hiccups must not stop a collection run
            return {"error": str(e)[:100]}
        restarts, images = {}, {}
        for p in pods:
            name = p["metadata"]["name"]
            statuses = p.get("status", {}).get("containerStatuses", [])
            restarts[name] = sum(c["restartCount"] for c in statuses)
            images.update({c["name"] + "@" + name.rsplit("-", 1)[0]: c.get("imageID", "")[-71:] for c in statuses})
        ready = sum(1 for n in nodes for c in n["status"]["conditions"] if c["type"] == "Ready" and c["status"] == "True")
        return {"restarts": restarts, "nodes_ready": ready, "nodes": len(nodes), "images": images}


def validity(before, after, error):
    if error and BAD_ERROR.search(error):
        return False, "error: " + error[:80]
    if before.get("restarts") is not None and after.get("restarts") is not None:
        if before["restarts"] != after["restarts"]:
            return False, "pod restarted"
    if after.get("nodes_ready") is not None and after["nodes_ready"] < after.get("nodes", 0):
        return False, "node not ready"
    return True, None


def features(explain_text):
    counts = {op: len(re.findall(r"\b%s\b" % op, explain_text)) for op in OPERATORS}
    counts["scans"] = len(re.findall(r"Scan ", explain_text))
    return {"operators": {k: v for k, v in counts.items() if v},
            "plan_hash": hashlib.sha256(re.sub(r"#\d+|\[\d+\]", "", explain_text).encode()).hexdigest()[:16],
            "plan_chars": len(explain_text)}


def load_configs(path):
    configs = {k: v for k, v in json.loads(pathlib.Path(path).read_text()).items() if not k.startswith("_")}
    if "default" not in configs:
        raise SystemExit("config set needs a 'default' entry")
    return configs


def plan_runs(configs, modes):
    """(config, mode) pairs worth running: the default, plus any configuration whose settings for that mode differ."""
    out = []
    for name, c in configs.items():
        for mode in modes:
            if name == "default" or c.get(mode, {}) != configs["default"].get(mode, {}):
                out.append((name, mode))
    return out


def run(args):
    suite = suites.SUITES[args.suite]
    schema = args.schema or suite["schema"]
    if not schema:
        raise SystemExit("--schema is required for suite " + args.suite)
    configs = load_configs(args.configs)
    modes = args.modes.split(",")
    rng = random.Random(args.seed)
    cluster = Cluster(args.kubectl, args.namespace)
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    def emit(rec):
        with out.open("a") as f:
            f.write(json.dumps(rec) + "\n")

    def execute(qid, sql, config_name, mode, rep, warmup):
        conf = dict(suite["conf"])
        conf.update(configs[config_name].get(mode, {}))
        before = cluster.snapshot()
        start = time.time()
        try:
            answer = post(args.url, {"sql": sql, "schema": schema, "mode": mode, "conf": conf, "max_rows": MAX_ROWS_IN_RECORD},
                          args.timeout)
        except Exception as e:  # the driver itself failed or timed out
            answer = {"wall": time.time() - start, "rows": None, "row_count": None, "hash": None, "error": str(e)[:300]}
        after = cluster.snapshot()
        valid, why = validity(before, after, answer["error"])
        rec = {"type": "run", "suite": args.suite, "schema": schema, "query": qid, "config": config_name, "mode": mode,
               "rep": rep, "warmup": warmup, "wall": round(answer["wall"], 3), "error": answer["error"],
               "row_count": answer["row_count"], "result_hash": answer["hash"], "rows": answer["rows"],
               "valid": valid, "invalid_reason": why, "conf": conf, "ts": round(start),
               "cluster": {k: v for k, v in after.items() if k != "restarts"} | {"restarts_total": sum(after.get("restarts", {}).values())}}
        emit(rec)
        print("%-12s %-8s %-18s %-6s rep%d wall=%7.1fs %s%s" % (args.suite, qid, config_name, mode, rep, rec["wall"],
              "" if valid else "INVALID(%s) " % why, rec["error"] or ""), flush=True)

    for qid in suites.parse_ids(args.suite, args.queries):
        sql = suites.query_sql(args.suite, qid)
        if not args.no_features:
            try:
                plan = post(args.url, {"sql": sql, "schema": schema, "mode": "flight", "conf": suite["conf"], "explain": True}, 300)
                if plan["error"] is None:
                    emit({"type": "features", "suite": args.suite, "schema": schema, "query": qid, **features(plan["rows"][0][0])})
            except Exception as e:
                print("features failed for %s: %s" % (qid, str(e)[:100]))
        if args.warmup:
            for mode in modes:
                execute(qid, sql, "default", mode, 0, True)
        jobs = [(c, m, rep) for (c, m) in plan_runs(configs, modes) for rep in range(1, args.reps + 1)]
        rng.shuffle(jobs)
        for c, m, rep in jobs:
            execute(qid, sql, c, m, rep, False)
    return 0


def validate(args):
    """Parses and analyzes every query of a suite with EXPLAIN (no execution): finds dialect problems before a long run."""
    suite = suites.SUITES[args.suite]
    schema = args.schema or suite["schema"]
    bad = 0
    for qid in suites.parse_ids(args.suite, args.queries):
        answer = post(args.url, {"sql": suites.query_sql(args.suite, qid), "schema": schema, "mode": args.mode,
                                 "conf": suite["conf"], "explain": True}, 300)
        if answer["error"]:
            bad += 1
            print("%s: %s" % (qid, answer["error"][:200].replace("\n", " ")))
    print("%d queries, %d fail to plan" % (len(suites.parse_ids(args.suite, args.queries)), bad))
    return 1 if bad else 0


def read(path):
    return [json.loads(l) for l in pathlib.Path(path).read_text().splitlines() if l.strip()]


def same_rows(a, b, rel=1e-6):
    """Results are equal when sorted rows match cell by cell, numbers within a relative tolerance (sums run in another order)."""
    def key(r):
        return tuple("%.6g" % c if isinstance(c, float) else str(c) for c in r)
    a, b = sorted(a, key=key), sorted(b, key=key)
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb):
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                if abs(x - y) > rel * max(abs(x), abs(y), 1.0):
                    return False
            elif x != y:
                return False
    return True


def check(args):
    recs = [r for r in read(args.file) if r["type"] == "run" and not r["warmup"]]
    problems = 0
    by_query = {}
    for r in recs:
        by_query.setdefault((r["schema"], r["query"]), []).append(r)
    for (schema, q), rs in sorted(by_query.items()):
        ok = [r for r in rs if r["error"] is None and r["valid"]]
        ref = next((r for r in ok if r["mode"] == "direct" and r["config"] == "default"), None) or \
            next((r for r in ok if r["config"] == "default"), None)
        for r in ok:
            if ref is None or r is ref:
                continue
            if r["row_count"] != ref["row_count"]:
                match = False
            elif r["result_hash"] == ref["result_hash"]:
                match = True
            elif r["rows"] is not None and ref["rows"] is not None:
                match = same_rows(r["rows"], ref["rows"])
            else:
                match = False
            if not match:
                problems += 1
                print("MISMATCH %s %s: %s/%s rows=%s differs from %s/%s rows=%s" % (
                    schema, q, r["mode"], r["config"], r["row_count"], ref["mode"], ref["config"], ref["row_count"]))
    failed = [r for r in recs if r["error"]]
    invalid = [r for r in recs if not r["valid"]]
    for r in failed:
        print("ERROR    %s %s %s/%s: %s" % (r["schema"], r["query"], r["mode"], r["config"], r["error"][:100]))
    print("%d runs, %d failed, %d invalid, %d result mismatches" % (len(recs), len(failed), len(invalid), problems))
    return 1 if problems else 0


def summary(args):
    cells = {}
    for r in read(args.file):
        if r["type"] == "run" and not r["warmup"] and r["error"] is None and r["valid"]:
            cells.setdefault((r["query"], r["config"], r["mode"]), []).append(r["wall"])
    print("%-8s %-18s %-7s %8s %4s" % ("query", "config", "mode", "median s", "n"))
    for (q, c, m), walls in sorted(cells.items()):
        print("%-8s %-18s %-7s %8.1f %4d" % (q, c, m, statistics.median(walls), len(walls)))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--suite", required=True, choices=sorted(suites.SUITES))
    r.add_argument("--schema")
    r.add_argument("--queries", default="all")
    r.add_argument("--configs", default=str(HERE / "configs" / "default.json"))
    r.add_argument("--modes", default="direct,flight")
    r.add_argument("--reps", type=int, default=2)
    r.add_argument("--warmup", type=int, default=1, help="discarded default-config runs per query and mode")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--timeout", type=int, default=600)
    r.add_argument("--url", default="http://127.0.0.1:18000")
    r.add_argument("--kubectl")
    r.add_argument("--namespace", default="tpch-bench")
    r.add_argument("--no-features", action="store_true")
    r.add_argument("--out", required=True)
    r.set_defaults(fn=run)
    v = sub.add_parser("validate")
    v.add_argument("--suite", required=True, choices=sorted(suites.SUITES))
    v.add_argument("--schema")
    v.add_argument("--queries", default="all")
    v.add_argument("--mode", default="direct")
    v.add_argument("--url", default="http://127.0.0.1:18000")
    v.set_defaults(fn=validate)
    for name, fn in (("check", check), ("summary", summary)):
        s = sub.add_parser(name)
        s.add_argument("file")
        s.set_defaults(fn=fn)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
