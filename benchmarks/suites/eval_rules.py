#!/usr/bin/env python3
"""eval_rules.py ENGINE: policy 'apply a configuration only when its plan differs from the default plan', learned per configuration from the training suites
(leave-one-suite-out): for every config, margin = win rate - loss rate among its plan-changed training cells; a held-out query takes the changed config with the
largest margin if that margin exceeds MARGIN, else default. Realised result = the measured median ratio of the chosen cell (1.0 = default). Also reports the
always-apply-if-changed rule for single configs and the null-bias view: the same policy applied to cells whose plan did NOT change would be pure noise."""
import collections, json, math, pathlib, sys
F = pathlib.Path(__file__).resolve().parent / "runs" / "features"
engine = sys.argv[1]
MARGIN = float(sys.argv[2]) if len(sys.argv) > 2 else 0.10
rows = [json.loads(l) for l in open(F / ("dataset-%s.jsonl" % engine))]
gm = lambda x: math.exp(sum(map(math.log, x)) / len(x)) if x else float("nan")  # noqa: E731
cells = collections.defaultdict(dict)
for r in rows:
    cells[(r["suite"], r["schema"], r["query"])][r["config"]] = r
suites = sorted({r["suite"] for r in rows})
res = collections.defaultdict(list)
detail = collections.Counter()
for held in suites:
    margin = {}
    for c in {r["config"] for r in rows}:
        tr = [r for r in rows if r["suite"] != held and r["config"] == c and r["delta"]["changed"]]
        margin[c] = (sum(r["win"] for r in tr) - sum(r["loss"] for r in tr)) / len(tr) if len(tr) >= 10 else -1
    for (s, schema, q), cs in cells.items():
        if s != held:
            continue
        cand = {c: margin[c] for c, r in cs.items() if r["delta"]["changed"] and margin[c] > MARGIN}
        if cand:
            c = max(cand, key=cand.get)
            res["learned rule"].append(cs[c]["ratio"]); detail[(held, c)] += 1
        else:
            res["learned rule"].append(1.0)
        oracle = min([1.0] + [r["ratio"] for r in cs.values()])
        res["oracle (optimistic)"].append(oracle)
        for c in ("bcast_256m", "retain_off", "shuffle_48", "agg_pushdown_off", "join_key_reduction_off"):
            if c in cs:
                res["always %s" % c].append(cs[c]["ratio"])
                res["%s only if plan changed" % c].append(cs[c]["ratio"] if cs[c]["delta"]["changed"] else 1.0)
print("== %s  rule margin %.2f, leave-one-suite-out, %d query/layout cells" % (engine, MARGIN, len(cells)))
for k, v in sorted(res.items(), key=lambda kv: gm(kv[1])):
    print("  %-40s geomean %.3f  (n=%d)" % (k, gm(v), len(v)))
print("  learned-rule choices (held-out suite, config): %s" % dict(detail))
