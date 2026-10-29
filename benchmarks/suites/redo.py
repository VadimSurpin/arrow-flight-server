#!/usr/bin/env python3
"""Drops every query that has an invalid run (or a driver connection error) from a JSONL file and prints the
comma-separated query numbers to rerun, so the collection can resume after a cluster disturbance."""
import json, pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import harness
path = sys.argv[1]
recs = [json.loads(l) for l in open(path) if l.strip()]
def dirty(r):
    """Invalid because of the cluster (restart, NotReady node, lost connection), judged by the current rules."""
    reason = r.get("invalid_reason") or ""
    return r["type"] == "run" and (bool(harness.BAD_ERROR.search(r["error"] or "")) or (not r["valid"] and not reason.startswith("error:")))


bad = {r["query"] for r in recs if dirty(r)}
open(path, "w").writelines(json.dumps(r) + "\n" for r in recs if r["query"] not in bad)
print(",".join(str(int(q[1:])) for q in sorted(bad)))
