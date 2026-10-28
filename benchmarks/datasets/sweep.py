#!/usr/bin/env python3
"""Print (or run) the gen.py invocations for one phase of the data plan.

  sweep.py --phase 1 --out s3://learn --sf 100            # TPC-DS x 3 layouts
  sweep.py --phase 2 --out s3://learn                      # synthetic axis grid (one-factor-at-a-time around a centre point)
  sweep.py --phase 2 --out s3://learn --run
"""
import argparse, itertools, shlex, subprocess, sys

CENTRE = dict(rows=50_000_000, key_card=1_000_000, skew=0.0, clustering="random", files=32, null_frac=0.0, key_type="int")
AXES = dict(
    key_card=[10, 1_000, 100_000, 10_000_000, 1_000_000_000],
    skew=[0.0, 0.8, 1.2],
    clustering=["sorted", "overlap", "random"],
    files=[4, 32, 256],
    null_frac=[0.0, 0.2, 0.9],
    key_type=["int", "string"],
)


def synthetic_points():
    seen, out = set(), []
    for axis, values in AXES.items():
        for v in values:
            p = dict(CENTRE, **{axis: v})
            k = tuple(sorted(p.items()))
            if k not in seen:
                seen.add(k)
                out.append(p)
    # the interactions that decide complete aggregation and DPP: clustering x cardinality x skew
    for c, k, s in itertools.product(AXES["clustering"], [1_000, 1_000_000], [0.0, 1.2]):
        p = dict(CENTRE, clustering=c, key_card=k, skew=s)
        key = tuple(sorted(p.items()))
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def commands(phase, out, sf):
    py = [sys.executable, "benchmarks/datasets/gen.py"]
    if phase == 1:
        return [py + ["--suite", "tpcds", "--sf", str(sf), "--layout", l, "--out", out] for l in ("asis", "sorted", "shuffled")]
    if phase == 2:
        cmds = []
        for p in synthetic_points():
            c = py + ["--suite", "synthetic", "--out", out]
            for k, v in p.items():
                c += ["--" + k.replace("_", "-"), str(v)]
            cmds.append(c)
        return cmds
    if phase == 3:
        return [py + ["--suite", "ssb", "--sf", str(sf), "--layout", l, "--out", out] for l in ("asis", "shuffled")]
    if phase == 4:
        return [py + ["--suite", "tpch", "--sf", str(s), "--layout", l, "--out", out] for s in (10, 300) for l in ("asis", "shuffled")]
    raise SystemExit("phase must be 1-4")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sf", type=int, default=100)
    ap.add_argument("--run", action="store_true")
    a = ap.parse_args()
    for c in commands(a.phase, a.out, a.sf):
        print(" ".join(shlex.quote(x) for x in c))
        if a.run:
            subprocess.run(c, check=True)
