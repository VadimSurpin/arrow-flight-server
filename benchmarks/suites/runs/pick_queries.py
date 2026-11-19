#!/usr/bin/env python3
"""pick_queries.py FILE [MAX_SECONDS]: comma separated queries whose default Flight run succeeded in under MAX_SECONDS (default 120).
Repetitions are only worth their time where a run finishes; failing or timing-out queries stay single labels."""
import json
import sys

limit = float(sys.argv[2]) if len(sys.argv) > 2 else 120.0
ok = {}
for line in open(sys.argv[1]):
    r = json.loads(line)
    if r["type"] == "run" and not r["warmup"] and r["valid"] and r["mode"] == "flight" and r["config"] == "default":
        ok[r["query"]] = ok.get(r["query"], True) and not r["error"] and r["wall"] < limit
print(",".join(sorted(q for q, v in ok.items() if v)))
