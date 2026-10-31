#!/usr/bin/env python3
"""analyze_layouts.py ASIS SORTED SHUFFLED: does the best configuration per query change with the data layout?

Inputs are phase-1 run files (valid, measured Flight runs). A query's best configuration is the fastest one that beats the default by more than 10%, otherwise `default`.
Prints markdown: agreement of the best configuration across layouts, harm from applying one layout's best configuration in another, headroom of per-query choice against default and against the best single configuration."""
import collections
import itertools
import json
import sys

THRESHOLD = 0.10
LAYOUTS = ["asis", "sorted", "shuffled"]


def load(path):
    cells = collections.defaultdict(dict)
    for line in open(path):
        r = json.loads(line)
        if r["type"] == "run" and not r["warmup"] and r["valid"] and r["mode"] == "flight":
            cells[int(r["query"][1:])][r["config"]] = None if r["error"] else r["wall"]
    return cells


def best(cfgs):
    base = cfgs.get("default")
    ok = {k: v for k, v in cfgs.items() if v is not None}
    if base is None:
        fixed = [k for k in ok if k != "default"]
        return min(fixed, key=ok.get) if fixed else None
    top = min(ok, key=ok.get)
    return top if ok[top] < base * (1 - THRESHOLD) else "default"


def main(paths):
    data = dict(zip(LAYOUTS, (load(p) for p in paths)))
    queries = sorted(set.intersection(*(set(q for q, c in d.items() if "default" in c) for d in data.values())))
    bests = {l: {q: best(data[l][q]) for q in queries} for l in LAYOUTS}
    print("Queries with a default result in all three layouts: %d\n" % len(queries))
    same = [q for q in queries if len({bests[l][q] for l in LAYOUTS}) == 1]
    nondefault = [q for q in queries if any(bests[l][q] != "default" for l in LAYOUTS)]
    print("- best configuration identical in all three layouts: %d of %d (%.0f%%); of the %d queries where some layout wants a non-default configuration: %d identical"
          % (len(same), len(queries), 100 * len(same) / len(queries), len(nondefault), len([q for q in nondefault if q in same])))
    print("- default is best in all three layouts: %d\n" % len([q for q in queries if all(bests[l][q] == "default" for l in LAYOUTS)]))
    print("| from | applied in | queries where it is >10% slower than default | where it is >10% faster | no change |")
    print("|---|---|---|---|---|")
    for a, b in itertools.permutations(LAYOUTS, 2):
        worse = better = flat = 0
        for q in queries:
            cfg = bests[a][q]
            if cfg in (None, "default"):
                continue
            w, base = data[b][q].get(cfg), data[b][q].get("default")
            if base is None:
                better += w is not None
                flat += w is None
            elif w is None or w > base * (1 + THRESHOLD):
                worse += 1
            elif w < base * (1 - THRESHOLD):
                better += 1
            else:
                flat += 1
        print("| %s | %s | %d | %d | %d |" % (a, b, worse, better, flat))
    print("\nQueries whose best configuration differs between layouts (up to 40):\n")
    print("| query | " + " | ".join(LAYOUTS) + " |")
    print("|---|---|---|---|")
    for q in [q for q in queries if q not in same][:40]:
        print("| q%02d | " % q + " | ".join(str(bests[l][q]) for l in LAYOUTS) + " |")
    print("\nTotal wall time over these queries, seconds (failed or timed out default counted as 300 s):\n")
    print("| layout | default | best config per query | best single configuration | single configuration |")
    print("|---|---|---|---|---|")
    for l in LAYOUTS:
        d = data[l]
        def wall(q, c):
            v = d[q].get(c)
            return 300.0 if v is None else v
        tot_default = sum(wall(q, "default") for q in queries)
        tot_best = sum(min([wall(q, c) for c in d[q]]) for q in queries)
        cfgs = set.intersection(*(set(d[q]) for q in queries))
        singles = {c: sum(min(wall(q, c), 300.0) for q in queries) for c in cfgs}
        c1 = min(singles, key=singles.get)
        print("| %s | %.0f | %.0f | %.0f | %s |" % (l, tot_default, tot_best, singles[c1], c1))


if __name__ == "__main__":
    main(sys.argv[1:])
