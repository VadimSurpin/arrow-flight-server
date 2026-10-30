#!/usr/bin/env python3
"""analyze.py FILE [FILE...]: Flight vs Direct per layout and across layouts from merged run files (valid, measured runs only).

Prints markdown. A query counts in the totals only when both modes returned a result in that layout."""
import collections
import json
import statistics as st
import sys


def load(path):
    cells = collections.defaultdict(lambda: collections.defaultdict(list))
    errors = collections.defaultdict(dict)
    for line in open(path):
        r = json.loads(line)
        if r["type"] != "run" or r["warmup"] or not r["valid"]:
            continue
        q = int(r["query"][1:])
        if r["error"]:
            errors[q][r["mode"]] = r["error"].replace("\n", " ")
        else:
            cells[q][r["mode"]].append(r["wall"])
    return {q: {m: st.median(v) for m, v in modes.items()} for q, modes in cells.items()}, errors


def kind(error):
    if "timed out" in error:
        return "timeout"
    if "broadcast" in error:
        return "broadcast out of memory"
    return "other: " + error[:70]


data = {}
for path in sys.argv[1:]:
    layout = path.split("tpcds-")[1].split("-")[0]
    data[layout] = load(path)

print("| layout | queries with both | Direct total (s) | Flight total (s) | Flight/Direct | Flight faster | Flight slower | median ratio |")
print("|---|---|---|---|---|---|---|---|")
both = {}
for layout, (cells, errors) in data.items():
    pairs = {q: (m["direct"], m["flight"]) for q, m in cells.items() if "direct" in m and "flight" in m}
    both[layout] = pairs
    d, f = sum(x[0] for x in pairs.values()), sum(x[1] for x in pairs.values())
    ratios = [b / a for a, b in pairs.values()]
    print("| %s | %d | %.0f | %.0f | %.2f | %d | %d | %.2f |" % (
        layout, len(pairs), d, f, f / d, sum(r < 1 for r in ratios), sum(r >= 1 for r in ratios), st.median(ratios)))

common = set.intersection(*[set(p) for p in both.values()]) if both else set()
print("\nLayout effect on the %d queries that succeeded in both modes in every layout:\n" % len(common))
print("| layout | Direct total (s) | Flight total (s) |\n|---|---|---|")
for layout, pairs in both.items():
    print("| %s | %.0f | %.0f |" % (layout, sum(pairs[q][0] for q in common), sum(pairs[q][1] for q in common)))

print("\nFlight slower than Direct by more than 15% (ratio, per layout):\n")
slow = collections.defaultdict(dict)
for layout, pairs in both.items():
    for q, (a, b) in pairs.items():
        if b / a > 1.15:
            slow[q][layout] = b / a
for q in sorted(slow, key=lambda q: -max(slow[q].values())):
    print("- q%02d: %s" % (q, ", ".join("%s %.1fx" % (l, r) for l, r in slow[q].items())))

print("\nFailures (valid runs):\n")
for layout, (cells, errors) in data.items():
    groups = collections.defaultdict(list)
    for q, modes in sorted(errors.items()):
        for mode, e in modes.items():
            groups[(mode, kind(e))].append("q%02d" % q)
    for (mode, k), qs in groups.items():
        print("- %s, %s, %s: %s" % (layout, mode, k, ", ".join(qs)))
