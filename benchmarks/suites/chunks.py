#!/usr/bin/env python3
"""chunks.py SUITE SPEC N: prints the queries of SPEC (as in harness --queries) in comma-separated chunks of N, one chunk per line."""
import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import suites
ids = suites.parse_ids(sys.argv[1], sys.argv[2])
n = int(sys.argv[3])
for i in range(0, len(ids), n):
    print(",".join(ids[i:i + n]))
