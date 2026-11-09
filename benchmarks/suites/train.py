#!/usr/bin/env python3
"""train.py ENGINE: per-config win/loss classifiers on plan + data features, evaluated leave-one-suite-out.
Policy for a query: among the configs choose the one with the largest P(win) - P(loss); apply it when that margin exceeds MARGIN, else keep default.
Realised result of a choice = the measured median ratio (config / default) of that cell. Reported per held-out suite as the geomean ratio over its queries
(1.00 = default, lower is better), next to the best single static config picked on the training suites and the oracle (best measured config, optimistic:
a minimum over noisy 2-repetition medians)."""
import collections, json, math, pathlib, sys
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier

F = pathlib.Path(__file__).resolve().parent / "runs" / "features"
MARGIN = 0.15
engine = sys.argv[1]
SPLIT = sys.argv[2] if len(sys.argv) > 2 else "suite"  # suite: leave one suite out; query: leave one query (all its layouts) out, a looser test
rows = [json.loads(l) for l in open(F / ("dataset-%s.jsonl" % engine))]
if SPLIT == "query":
    for r in rows:
        r["suite"] = r["suite"] + "/" + r["query"]
USE = set(sys.argv[3].split(",")) if len(sys.argv) > 3 else {"plan", "data", "cost"}  # feature groups
groups = [g for g in ("plan", "data", "cost") if g in USE]
names = [g + "." + k for g in groups for k in sorted(rows[0][g])]
vec = lambda r: [r[n.split(".")[0]].get(n.split(".", 1)[1], 0) for n in names]  # noqa: E731
configs = sorted({r["config"] for r in rows})
suites = sorted({r["suite"] for r in rows})
cells = collections.defaultdict(dict)  # (schema, query) -> config -> row
for r in rows:
    cells[(r["schema"], r["query"])][r["config"]] = r
gm = lambda xs: math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float("nan")  # noqa: E731


def fit(model, X, y):
    if len(set(y)) < 2:
        const = float(y[0]) if y else 0.0
        return lambda Z: np.full(len(Z), const)
    m = model().fit(X, y)
    return lambda Z: m.predict_proba(Z)[:, 1]


def run(kind, make):
    res = {}
    aucs = collections.defaultdict(list)
    for held in suites:
        train = [r for r in rows if r["suite"] != held]
        test = [r for r in rows if r["suite"] == held]
        if not test or not train:
            continue
        pw, pl = {}, {}
        for c in configs:
            tr = [r for r in train if r["config"] == c]
            te = [r for r in test if r["config"] == c]
            if not tr or not te:
                continue
            X, Z = np.array([vec(r) for r in tr]), np.array([vec(r) for r in te])
            w = fit(make, X, [r["win"] for r in tr])(Z)
            l = fit(make, X, [r["loss"] for r in tr])(Z)
            for r, a, b in zip(te, w, l):
                pw[(r["schema"], r["query"], c)], pl[(r["schema"], r["query"], c)] = a, b
            if len({r["win"] for r in te}) == 2:
                aucs[c].append(roc_auc_score([r["win"] for r in te], w))
        learned, static, oracle, applied = [], [], [], 0
        best_static = min(configs, key=lambda c: gm([r["ratio"] for r in train if r["config"] == c] or [9]))
        for k in {(r["schema"], r["query"]) for r in test}:
            cs = cells[k]
            score = {c: pw.get((k[0], k[1], c), 0) - pl.get((k[0], k[1], c), 0) for c in cs}
            c = max(score, key=score.get)
            if score[c] > MARGIN:
                learned.append(cs[c]["ratio"]); applied += 1
            else:
                learned.append(1.0)
            static.append(cs[best_static]["ratio"] if best_static in cs else 1.0)
            oracle.append(min(1.0, min(r["ratio"] for r in cs.values())))
        res[held] = (len(learned), applied, gm(learned), gm(static), gm(oracle), best_static)
    print("== %s / %s  split=%s features=%s (margin %.2f)" % (engine, kind, SPLIT, "+".join(groups), MARGIN))
    print("%-11s %5s %7s | %7s %7s %7s | static config" % ("held-out", "cases", "applied", "learned", "static", "oracle"))
    for h, (n, a, l, s, o, b) in (res.items() if SPLIT == "suite" else []):
        print("%-11s %5d %7d | %7.3f %7.3f %7.3f | %s" % (h, n, a, l, s, o, b))
    tot = sum(v[0] for v in res.values())
    wgm = lambda i: gm([v[i] for v in res.values() for _ in range(v[0])])  # noqa: E731
    print("%-11s %5d %7d | %7.3f %7.3f %7.3f |" % ("all", tot, sum(v[1] for v in res.values()), wgm(2), wgm(3), wgm(4)))
    print("held-out AUC for 'win' per config (mean over suites with both classes):",
          {c: round(sum(v) / len(v), 2) for c, v in sorted(aucs.items())})


run("gradient boosting", lambda: GradientBoostingClassifier(n_estimators=60, max_depth=2, learning_rate=0.1, random_state=1))
run("decision tree depth 3", lambda: DecisionTreeClassifier(max_depth=3, min_samples_leaf=5, random_state=1))


def importances():
    imp = np.zeros(len(names))
    n = 0
    for c in configs:
        tr = [r for r in rows if r["config"] == c]
        y = [r["win"] for r in tr]
        if len(set(y)) < 2:
            continue
        imp += GradientBoostingClassifier(n_estimators=60, max_depth=2, learning_rate=0.1, random_state=1).fit(np.array([vec(r) for r in tr]), y).feature_importances_
        n += 1
    top = sorted(zip(imp / max(n, 1), names), reverse=True)[:10]
    print("top features for 'win' (gradient boosting, mean over configs):", ", ".join("%s %.2f" % (nm, v) for v, nm in top))


importances()
