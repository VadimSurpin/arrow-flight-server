#!/usr/bin/env python3
"""analyze_plans.py ENGINE: how often each configuration changes the plan, and how wins/losses depend on it.
Win rate among cells whose plan equals the default plan is the false-positive rate of the labels (valid for plan-visible knobs only)."""
import collections, json, math, pathlib, sys
F = pathlib.Path(__file__).resolve().parent / "runs" / "features"
engine = sys.argv[1]
plans = {(r["schema"], r["query"], r["config"]): r for r in map(json.loads, open(F / ("config-plans-%s.jsonl" % engine))) if not r["error"]}
st = collections.defaultdict(lambda: {"same": [0, 0, 0, []], "chg": [0, 0, 0, []]})
for r in map(json.loads, open(F / ("labels-%s.jsonl" % engine))):
    d, p = plans.get((r["schema"], r["query"], "default")), plans.get((r["schema"], r["query"], r["config"]))
    if not d or not p:
        continue
    b = st[r["config"]]["same" if d["hash"] == p["hash"] else "chg"]
    b[0] += 1; b[1] += r["win"]; b[2] += r["loss"]; b[3].append(r["ratio"])
gm = lambda x: math.exp(sum(map(math.log, x)) / len(x)) if x else float("nan")  # noqa: E731
print("%-24s | %-34s | %-34s" % (engine, "plan SAME as default", "plan CHANGED"))
print("%-24s | %5s %6s %6s %8s | %5s %6s %6s %8s" % ("config", "n", "win%", "loss%", "geomean", "n", "win%", "loss%", "geomean"))
for c, v in sorted(st.items()):
    s, h = v["same"], v["chg"]
    pc = lambda b, i: "%5.0f%%" % (100 * b[i] / b[0]) if b[0] else "     -"  # noqa: E731
    print("%-24s | %5d %s %s %8.3f | %5d %s %s %8.3f" % (c, s[0], pc(s, 1), pc(s, 2), gm(s[3]), h[0], pc(h, 1), pc(h, 2), gm(h[3])))

# Null bias: runtime-neutral configs whose plan equals the default plan should be at ratio 1.0
null = [x for c in ("bloom_off", "reuse_bcast_only", "complete_agg_off", "partial_decimal_sum_on", "agg_and_join_off")
        for x in (st[c]["same"][3] if c in st else [])]
if null:
    print("\nnull check (runtime-neutral configs, plan identical): n=%d geomean ratio %.3f; share 'win' by the 5%% rule: see win%% columns above" % (len(null), gm(null)))
