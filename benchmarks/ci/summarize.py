#!/usr/bin/env python3
"""Convert compare-results.json to a GitHub Markdown summary."""
import json
import sys
import pathlib

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
sf   = data.get("scale_factor", "?")
reps = data.get("repetitions", "?")
host = data.get("flight_host", "?")

print("## Arrow Flight vs Direct HDFS Parquet\n")
print(
    f"**Scale factor:** {sf} &nbsp;|&nbsp; "
    f"**Repetitions:** {reps} &nbsp;|&nbsp; "
    f"**Flight host:** {host}\n"
)
print("| Query | Flight avg (ms) | Direct avg (ms) | Speedup |")
print("|:-----:|:--------------:|:--------------:|:-------:|")
for qname, d in data.get("queries", {}).items():
    f_avg = d["flight"]["avg_ms"]
    d_avg = d["direct"]["avg_ms"]
    speedup = round(d_avg / f_avg, 2) if f_avg else 0
    arrow = "\U0001f680" if speedup >= 1.0 else "\U0001f422"
    print(f"| {qname.upper()} | {f_avg} | {d_avg} | {arrow} {speedup}x |")

failed = data.get("failed", [])
if failed:
    print(f"\n**Failed queries:** {', '.join(q.upper() for q in failed)}")
