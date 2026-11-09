"""Default-config medians: StarRocks flight vs Spark flight vs Spark direct (and StarRocks direct when present), per suite and layout.
Reads runs/fresh-nocache (Spark) and runs/fresh-sr (StarRocks); prints geometric-mean ratios over queries that are valid in all compared columns."""
import json, glob, os, statistics as st, math, collections, sys

def load(path, mode):
    d = collections.defaultdict(list); h = {}
    if not os.path.exists(path):
        return d, h
    for l in open(path):
        x = json.loads(l)
        if x.get("type") != "run" or x["mode"] != mode or x["warmup"] or x["config"] != "default":
            continue
        if x["error"]:
            d[x["query"]].append(None)
        else:
            d[x["query"]].append(x["wall"]); h[x["query"]] = x["result_hash"]
    return d, h

def med(v):
    v = [t for t in v if t is not None]
    return st.median(v) if v else None

def main():
    units = sorted(os.path.basename(p)[:-len("-default.jsonl")] for p in glob.glob("runs/fresh-nocache/*-default.jsonl"))
    print("%-18s %4s | %8s %8s %8s %8s | %s" % ("unit", "n", "SR-fl", "SP-fl", "SP-dir", "SR-dir", "geomean ratios (SR-fl/SP-fl, SR-fl/SP-dir, SR-dir/SP-dir) | SR-fl errors"))
    for u in units:
        sp = "runs/fresh-nocache/%s-default.jsonl" % u
        spf, _ = load(sp, "flight"); spd, _ = load(sp, "direct")
        srf, _ = load("runs/fresh-sr/%s-default.jsonl" % u, "flight")
        srd, _ = load("runs/fresh-sr/%s-direct.jsonl" % u, "direct")
        qs = [q for q in srf if med(srf[q]) and med(spf.get(q, [])) and med(spd.get(q, []))]
        if not qs:
            continue
        def g(a, b, qq):
            r = [med(a[q]) / med(b[q]) for q in qq if med(a.get(q, [])) and med(b.get(q, []))]
            return math.exp(sum(map(math.log, r)) / len(r)) if r else float("nan")
        tot = lambda d: sum(med(d[q]) for q in qs)
        qd = [q for q in qs if med(srd.get(q, []))]
        errs = sum(1 for q in srf if med(srf[q]) is None)
        print("%-18s %4d | %8.0f %8.0f %8.0f %8s | %.2f  %.2f  %s | %d" % (
            u, len(qs), tot(srf), tot(spf), tot(spd), ("%.0f" % sum(med(srd[q]) for q in qd)) if qd else "-",
            g(srf, spf, qs), g(srf, spd, qs), ("%.2f" % g(srd, spd, qd)) if qd else "-", errs))

main()
