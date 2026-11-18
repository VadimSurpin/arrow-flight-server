#!/usr/bin/env python3
"""Merges the TPC-H result files into one markdown table.

Usage: report.py RESULTS_DIR > report.md
Per query and mode the first successful measurement wins, searched in this order: the base run (default settings),
then the re-runs with enable_spill=true. The Config column says which one each mode's numbers came from.
"""
import json
import pathlib
import sys


def load(path):
    return {int(k): v for k, v in json.loads(path.read_text()).items()}


def ok(entry):
    return bool(entry) and not entry.get("error") and bool(entry.get("times"))


def warm(entry):
    times = entry["times"]
    return min(times[1:]) if len(times) > 1 else times[0]


def main() -> int:
    d = pathlib.Path(sys.argv[1])
    sources = [("default", load(d / "sf100-direct-vs-flight.json"))]
    sources += [("spill", load(p)) for p in sorted(d.glob("sf100-spill-*.json"))]
    print("| Query | Direct cold / warm (s) | Flight cold / warm (s) | Flight vs direct (warm) | Results match | Config (direct/flight) |")
    print("|---|---|---|---|---|---|")
    for q in range(1, 23):
        picked = {}
        for mode in ("direct", "flight"):
            for name, data in sources:
                entry = data.get(q, {}).get(mode)
                if ok(entry):
                    picked[mode] = (entry, name, data.get(q, {}).get("flight", {}).get("matches_direct"))
                    break
        text, config, warms = {}, {}, {}
        for mode in ("direct", "flight"):
            if mode in picked:
                entry, name, _ = picked[mode]
                text[mode] = "%.1f / %.1f" % (entry["times"][0], warm(entry))
                warms[mode] = warm(entry)
                config[mode] = name
            else:
                text[mode], config[mode] = "OUT OF MEMORY", "-"
        ratio = "%.2fx" % (warms["flight"] / warms["direct"]) if len(warms) == 2 else "-"
        matches = [m for _, _, m in picked.values() if m is not None]
        match = "yes" if matches and all(matches) else ("NO" if matches else "n/a (no direct result)")
        print("| Q%d | %s | %s | %s | %s | %s/%s |" % (q, text["direct"], text["flight"], ratio, match,
                                                   config["direct"], config["flight"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
