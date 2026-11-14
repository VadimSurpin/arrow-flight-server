#!/usr/bin/env python3
"""merge_runs.py DIRECT.jsonl FLIGHT.jsonl OUT.jsonl: the dataset of one layout after a connector fix.

Takes the Direct runs from the first file and the Flight runs from the second (features come from the second when it
has them, else the first), so a fix that only touches Flight does not force a new Direct collection."""
import json
import sys

direct, flight, out = sys.argv[1:4]


def read(path):
    return [json.loads(l) for l in open(path) if l.strip()]


d, f = read(direct), read(flight)
records = [r for r in d if r["type"] == "run" and r["mode"] == "direct"]
records += [r for r in f if r["type"] == "run" and r["mode"] == "flight"]
flight_features = {(r["schema"], r["query"]) for r in f if r["type"] == "features"}
records += [r for r in f if r["type"] == "features"]
records += [r for r in d if r["type"] == "features" and (r["schema"], r["query"]) not in flight_features]
with open(out, "w") as fh:
    fh.writelines(json.dumps(r) + "\n" for r in records)
print("%s: %d direct runs, %d flight runs, %d features" % (
    out, sum(r["type"] == "run" and r["mode"] == "direct" for r in records),
    sum(r["type"] == "run" and r["mode"] == "flight" for r in records), sum(r["type"] == "features" for r in records)))
