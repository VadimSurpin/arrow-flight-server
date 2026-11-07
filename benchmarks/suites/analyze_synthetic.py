#!/usr/bin/env python3
"""analyze_synthetic.py DEFAULT.jsonl PHASE1.jsonl: Flight vs Direct and the knob sweep per synthetic schema, related to the dataset axes
(key cardinality, skew, clustering, file count, null fraction, key type; the schema name carries them). Prints markdown. Medians of valid measured runs."""
import collections
import json
import re
import statistics as st
import sys

BASE = {"k": "1000000", "s": "0p0", "c": "random", "f": "32", "n": "0p0", "t": "int"}
AXES = [("k", "key cardinality"), ("s", "skew"), ("c", "clustering"), ("f", "files"), ("n", "null fraction"), ("t", "key type")]


def axes(schema):
    m = re.fullmatch(r"syn_r\d+_k(\d+)_s(\w+)_(\w+)_f(\d+)_n(\w+)_(\w+)", schema)
    return dict(zip("kscfnt", m.groups()))


def load(path):
    cells = collections.defaultdict(list)
    for l in open(path):
        x = json.loads(l)
        if x["type"] == "run" and not x["warmup"] and not x["error"] and x["valid"]:
            cells[(x["schema"], x["query"], x["config"], x["mode"])].append(x["wall"])
    return {k: st.median(v) for k, v in cells.items()}


d, p = load(sys.argv[1]), load(sys.argv[2])
schemas = sorted({k[0] for k in d})
queries = sorted({k[1] for k in d})
ratio = {}
for s in schemas:
    for q in queries:
        f, x = d.get((s, q, "default", "flight")), d.get((s, q, "default", "direct"))
        if f and x:
            ratio[(s, q)] = f / x
print("## Flight/Direct (default configuration), median over schemas, per query\n")
print("| query | Flight (s) | Direct (s) | median ratio | schemas Flight faster |\n|---|---|---|---|---|")
for q in queries:
    rs = [ratio[(s, q)] for s in schemas if (s, q) in ratio]
    fl = [d[(s, q, "default", "flight")] for s in schemas if (s, q) in ratio]
    dr = [d[(s, q, "default", "direct")] for s in schemas if (s, q) in ratio]
    print("| %s | %.2f | %.2f | %.2f | %d of %d |" % (q, st.median(fl), st.median(dr), st.median(rs), sum(r < 1 for r in rs), len(rs)))
print("\n## Effect of each axis on Flight/Direct (total Flight time / total Direct time over the 8 queries)\n")
print("| axis | value | schemas | Flight total (s) | Direct total (s) | ratio |\n|---|---|---|---|---|---|")
for key, name in AXES:
    for v in sorted({axes(s)[key] for s in schemas}, key=lambda z: (len(z), z)):
        sel = [s for s in schemas if axes(s)[key] == v and all(axes(s)[o] == BASE[o] for o in BASE if o != key)]
        if not sel:
            continue
        f = sum(d[(s, q, "default", "flight")] for s in sel for q in queries if (s, q) in ratio)
        x = sum(d[(s, q, "default", "direct")] for s in sel for q in queries if (s, q) in ratio)
        print("| %s | %s%s | %d | %.1f | %.1f | %.2f |" % (name, v, " (base)" if v == BASE[key] else "", len(sel), f, x, f / x))
print("\nOne axis changed at a time from the base dataset (k=1000000, no skew, random, 32 files, no nulls, int key); schemas that change two axes at once are not in this table.\n")
print("## Knob sweep (Flight): cells (schema, query) where a configuration beats default by more than 15% and more than 0.2 s\n")
cfgs = sorted({k[2] for k in p if k[2] != "default"})
cells = sorted({(k[0], k[1]) for k in p})
tot = collections.Counter()
print("| configuration | faster | slower | median change |\n|---|---|---|---|")
for c in cfgs:
    fast = slow = 0
    ch = []
    for s, q in cells:
        a, b = p.get((s, q, "default", "flight")), p.get((s, q, c, "flight"))
        if a and b:
            ch.append(b / a)
            fast += (a - b > 0.2 and b < 0.85 * a)
            slow += (b - a > 0.2 and b > 1.15 * a)
    print("| %s | %d | %d | %+.0f%% |" % (c, fast, slow, (st.median(ch) - 1) * 100))
print("\n%d cells in total. Cells where the best configuration beats default by more than 15%% and 0.2 s:\n" % len(cells))
wins = []
for s, q in cells:
    a = p.get((s, q, "default", "flight"))
    best = min(((p[(s, q, c, "flight")], c) for c in cfgs if (s, q, c, "flight") in p), default=None)
    if a and best and a - best[0] > 0.2 and best[0] < 0.85 * a:
        wins.append((a / best[0], s, q, best[1], a, best[0]))
print("| query | schema | default (s) | best (s) | configuration |\n|---|---|---|---|---|")
for r, s, q, c, a, b in sorted(wins, reverse=True)[:25]:
    print("| %s | %s | %.2f | %.2f | %s |" % (q, s[len("syn_r50000000_"):], a, b, c))
print("\n%d such cells of %d." % (len(wins), len(cells)))
