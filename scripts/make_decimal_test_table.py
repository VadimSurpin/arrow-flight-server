#!/usr/bin/env python3
"""Adds test_schema.decimal_table (DECIMAL columns, NULLs, two files) to a copy of the Flight test data directory.

Usage: make_decimal_test_table.py DATA_DIR      Requires `duckdb`.
"""
import os
import sys

import duckdb

root = os.path.join(sys.argv[1], "test_schema", "decimal_table")
os.makedirs(root, exist_ok=True)
con = duckdb.connect()
for part, (lo, hi) in enumerate([(0, 500), (500, 1000)]):
    target = os.path.join(root, f"part-{part}.parquet")
    con.execute(
        "COPY (SELECT (i % 5)::INTEGER AS k, "
        "CASE WHEN i % 17 = 0 THEN NULL ELSE ((i * 37 % 10000) / 100.0)::DECIMAL(15,2) END AS price, "
        "((i % 11) / 100.0)::DECIMAL(15,2) AS disc, ((i % 9) / 100.0)::DECIMAL(15,2) AS tax, "
        "CASE WHEN i % 3 = 0 THEN 'MAIL' ELSE 'SHIP' END AS mode "
        f"FROM range({lo}, {hi}) t(i)) TO '{target}' (FORMAT PARQUET)")
print("wrote", root)
