#!/usr/bin/env python3
"""build_labels.py: per (engine, schema, query, config) label from the phase-1 sweeps -> runs/features/labels-<engine>.jsonl.
A config 'wins' when its median beats the default median by more than 5% AND both of its repetitions are faster than the slower-than-default-median
reference, i.e. max(rep walls of the config) < min(default walls) (so the gain is not one lucky run). Default runs under MIN_S seconds are not labelled.
Spark phase-1 files are runs/fresh-nocache/*-phase1.jsonl, StarRocks runs/fresh-sr/*-phase1.jsonl (+ the q38 rerun, which replaces the failed original; the shuffled rerun is already in its file)."""
import collections, glob, json, pathlib, statistics as st

HERE = pathlib.Path(__file__).resolve().parent
MIN_S, GAIN = 2.0, 1.05


def cells(pattern):
    d = collections.defaultdict(lambda: collections.defaultdict(list))
    for p in sorted(glob.glob(str(HERE / pattern))):
        for l in open(p):
            x = json.loads(l)
            if x.get("type") != "run" or x["warmup"] or x["error"] or x.get("valid") is False or x["mode"] != "flight":
                continue
            d[(x["schema"], x["query"])][x["config"]].append(x["wall"])
    return d


def label(engine, pattern, extra=()):
    d = cells(pattern)
    for e in extra:
        for k, v in cells(e).items():
            for c, w in v.items():
                d[k][c] = w  # the rerun replaces a failed original
    out = []
    for (schema, q), c in sorted(d.items()):
        base = c.get("default")
        if not base or len(base) < 2 or st.median(base) < MIN_S:
            continue
        bm = st.median(base)
        for cfg, w in c.items():
            if cfg == "default" or len(w) < 2:
                continue
            m = st.median(w)
            out.append({"engine": engine, "schema": schema, "query": q, "config": cfg, "default_s": round(bm, 3), "config_s": round(m, 3),
                        "ratio": round(m / bm, 3), "win": m * GAIN < bm and max(w) < min(base), "loss": m > bm * GAIN and min(w) > max(base)})
    path = HERE / "runs" / "features" / ("labels-%s.jsonl" % engine)
    path.write_text("".join(json.dumps(o) + "\n" for o in out))
    by = collections.defaultdict(lambda: [0, 0, 0])
    for o in out:
        b = by[o["config"]]; b[0] += 1; b[1] += o["win"]; b[2] += o["loss"]
    print(engine, len(out), "labelled cells")
    for cfg, (n, w, l) in sorted(by.items()):
        print("  %-24s n=%4d  win %3d (%2d%%)  loss %3d (%2d%%)" % (cfg, n, w, 100 * w // n, l, 100 * l // n))


label("spark", "runs/fresh-nocache/*-phase1.jsonl")
label("starrocks", "runs/fresh-sr/*-phase1.jsonl", extra=("runs/fresh-sr/tpcds-asis-phase1-rerun.jsonl",))
