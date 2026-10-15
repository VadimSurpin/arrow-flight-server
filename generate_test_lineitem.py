#!/usr/bin/env python3
"""
Generate small TPC-H lineitem Parquet file for local Flight server debugging.

Run:
    pip install pyarrow
    python generate_test_lineitem.py

Creates: flight-server/src/test/resources/lineitem_tiny/tpch/lineitem/lineitem_tiny.parquet
Query via Flight SQL: SELECT * FROM tpch.lineitem
"""
import os
import random
from datetime import date, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

NUM_ROWS = 100
BASE_DATE = date(1998, 1, 1)
OUTPUT = "flight-server/src/test/resources/lineitem_tiny/tpch/lineitem/lineitem_tiny.parquet"

random.seed(42)

schema = pa.schema([
    pa.field("l_orderkey", pa.int64()),
    pa.field("l_partkey", pa.int64()),
    pa.field("l_suppkey", pa.int64()),
    pa.field("l_linenumber", pa.int32()),
    pa.field("l_quantity", pa.decimal128(15, 2)),
    pa.field("l_extendedprice", pa.decimal128(15, 2)),
    pa.field("l_discount", pa.decimal128(15, 2)),
    pa.field("l_tax", pa.decimal128(15, 2)),
    pa.field("l_returnflag", pa.utf8()),
    pa.field("l_linestatus", pa.utf8()),
    pa.field("l_shipdate", pa.date32()),
    pa.field("l_commitdate", pa.date32()),
    pa.field("l_receiptdate", pa.date32()),
    pa.field("l_shipinstruct", pa.utf8()),
    pa.field("l_shipmode", pa.utf8()),
    pa.field("l_comment", pa.utf8()),
])

data = {
    "l_orderkey": [random.randint(1, 1000) for _ in range(NUM_ROWS)],
    "l_partkey": [random.randint(1, 2000) for _ in range(NUM_ROWS)],
    "l_suppkey": [random.randint(1, 100) for _ in range(NUM_ROWS)],
    "l_linenumber": [random.randint(1, 4) for _ in range(NUM_ROWS)],
    "l_quantity": [Decimal(random.randint(1, 50)) for _ in range(NUM_ROWS)],
    "l_extendedprice": [Decimal(str(round(random.uniform(10, 2000) * 100) / 100)) for _ in range(NUM_ROWS)],
    "l_discount": [Decimal(str(round(random.uniform(0, 0.10) * 100) / 100)) for _ in range(NUM_ROWS)],
    "l_tax": [Decimal(str(round(random.uniform(0, 0.08) * 100) / 100)) for _ in range(NUM_ROWS)],
    "l_returnflag": [random.choice(["A", "R", "N"]) for _ in range(NUM_ROWS)],
    "l_linestatus": [random.choice(["F", "O"]) for _ in range(NUM_ROWS)],
    "l_shipdate": [BASE_DATE + timedelta(days=random.randint(0, 365)) for _ in range(NUM_ROWS)],
    "l_commitdate": [BASE_DATE + timedelta(days=random.randint(0, 365)) for _ in range(NUM_ROWS)],
    "l_receiptdate": [BASE_DATE + timedelta(days=random.randint(0, 365)) for _ in range(NUM_ROWS)],
    "l_shipinstruct": [random.choice(["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]) for _ in range(NUM_ROWS)],
    "l_shipmode": [random.choice(["MAIL", "SHIP", "AIR", "TRUCK"]) for _ in range(NUM_ROWS)],
    "l_comment": [f"Debug row {i}" for i in range(NUM_ROWS)],
}

table = pa.table(data, schema=schema)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
pq.write_table(table, OUTPUT)
print(f"OK  {NUM_ROWS} rows -> {OUTPUT}")
print(f"Schema: {schema}")
print(f"\nQuery test:  python -c \"import pyarrow.flight; ...\"")
print(f"Or via tests: mvn test -Dtest=FlightQ1DebugTest -DexcludedGroups=\\\"\\\"")
