#!/usr/bin/env python3
"""analyze_knobs.py FILE: per query, the best configuration against default from a phase-1 run file (valid, measured Flight runs).

Prints markdown: a per-query table, a per-knob summary, and the share of queries where some knob changes the result by more than 10%."""
import collections
import json
import sys

THRESHOLD = 0.10


def load(path):
    cells = collections.defaultdict(dict)
    for line in open(path):
        r = json.loads(line)
        if r["type"] != "run" or r["warmup"] or not r["valid"] or r["mode"] != "flight":
            continue
        cells[int(r["query"][1:])][r["config"]] = (r["wall"], r["error"], r["row_count"])
    return cells


def main(path):
    cells = load(path)
    knob_wins = collections.Counter()
    knob_losses = collections.Counter()
    knob_fixes = collections.Counter()
    rows = []
    sensitive = 0
    for q in sorted(cells):
        c = cells[q]
        base = c.get("default")
        if base is None:
            continue
        ok = {k: v[0] for k, v in c.items() if not v[1]}
        if base[1]:
            fixed = sorted(k for k in ok if k != "default")
            for k in fixed:
                knob_fixes[k] += 1
            best = min(fixed, key=lambda k: ok[k]) if fixed else None
            rows.append((q, "failed", "-", best or "-", f"{ok[best]:.1f}" if best else "-", "fixes" if best else "still fails"))
            sensitive += bool(best)
            continue
        for k, w in ok.items():
            if k == "default":
                continue
            if w < base[0] * (1 - THRESHOLD):
                knob_wins[k] += 1
            elif w > base[0] * (1 + THRESHOLD):
                knob_losses[k] += 1
        for k, v in c.items():
            if v[1] and k != "default":
                knob_losses[k] += 1
        best = min(ok, key=lambda k: ok[k])
        gain = 1 - ok[best] / base[0]
        sensitive += gain > THRESHOLD
        rows.append((q, f"{base[0]:.1f}", "-", best, f"{ok[best]:.1f}", f"{gain:+.0%}"))
    print("| query | default (s) | | best config | best (s) | gain |")
    print("|---|---|---|---|---|---|")
    for q, d, _, b, w, g in rows:
        print(f"| q{q:02d} | {d} | | {b} | {w} | {g} |")
    print(f"\nQueries where some knob beats default by more than {THRESHOLD:.0%} (or fixes a failure): {sensitive} of {len(rows)}\n")
    print("| knob | faster (>10%) | slower or failing (>10%) | fixes a default failure |")
    print("|---|---|---|---|")
    for k in sorted(set(knob_wins) | set(knob_losses) | set(knob_fixes)):
        print(f"| {k} | {knob_wins[k]} | {knob_losses[k]} | {knob_fixes[k]} |")


if __name__ == "__main__":
    main(sys.argv[1])
