#!/usr/bin/env python3
"""Generate Parquet datasets for the optimizer-classifier learning set (docs/roadmap/optimizer-classifier-data-plan.md).

  gen.py --suite tpch|tpcds|ssb|synthetic --sf N --layout asis|sorted|shuffled --out DIR_OR_s3://bucket/prefix

Output: <out>/<suite>_sf<N>_<layout>/<table>/part-*.parquet plus manifest.json
(one directory level per schema, because the Flight server resolves schema.table as <data dir>/<schema>/<table>) (axes, row counts, file counts).
S3 targets read MINIO_ROOT_USER / MINIO_ROOT_PASSWORD and MINIO_ENDPOINT (default minio:9000).
Layouts: asis = generator order, sorted = by the table's main join key (SORT_KEYS), shuffled = random order.
"""
import argparse, json, os, sys, tempfile
import duckdb

# the join/filter key each large table is sorted by in the "sorted" layout
SORT_KEYS = {
    "lineitem": "l_orderkey", "orders": "o_orderkey", "partsupp": "ps_partkey",
    "store_sales": "ss_item_sk", "catalog_sales": "cs_item_sk", "web_sales": "ws_item_sk",
    "store_returns": "sr_item_sk", "catalog_returns": "cr_item_sk", "web_returns": "wr_item_sk",
    "inventory": "inv_item_sk", "lineorder": "lo_orderkey",
}
BIG_ROWS = 1_000_000  # tables smaller than this are written as a single file in every layout


def connect(spill, memory):
    con = duckdb.connect(os.path.join(spill, "gen.duckdb"))
    con.execute("SET memory_limit='%s'; SET temp_directory='%s'" % (memory, os.path.join(spill, "tmp")))
    return con


def load_s3(con):
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute("CREATE OR REPLACE SECRET (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', USE_SSL false, URL_STYLE 'path')"
                % (os.environ["MINIO_ROOT_USER"], os.environ["MINIO_ROOT_PASSWORD"],
                   os.environ.get("MINIO_ENDPOINT", "minio:9000")))


def build(con, suite, sf):
    if suite == "tpch":
        con.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s)" % sf)
    elif suite == "tpcds":
        con.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=%s)" % sf)
    elif suite == "ssb":
        con.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s)" % sf)
        ssb_from_tpch(con)
    else:
        raise SystemExit("unknown suite " + suite)


def ssb_from_tpch(con):
    """Star schema benchmark tables derived from TPC-H data (same row counts as the SSB spec, not its exact value domains)."""
    con.execute("""CREATE TABLE lineorder AS SELECT o_orderkey lo_orderkey, l_linenumber lo_linenumber, o_custkey lo_custkey,
        l_partkey lo_partkey, l_suppkey lo_suppkey, CAST(strftime(o_orderdate,'%Y%m%d') AS INT) lo_orderdate,
        o_orderpriority lo_orderpriority, l_quantity lo_quantity, l_extendedprice lo_extendedprice,
        l_discount lo_discount, l_extendedprice*(1-l_discount) lo_revenue, l_tax lo_tax, l_shipmode lo_shipmode
        FROM lineitem JOIN orders ON l_orderkey=o_orderkey""")
    con.execute("""CREATE TABLE customer_d AS SELECT c_custkey, c_name, c_city, c_nation, c_region, c_mktsegment FROM
        (SELECT c_custkey, c_name, c_address c_city, n_name c_nation, r_name c_region, c_mktsegment
         FROM customer JOIN nation ON c_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey)""")
    con.execute("""CREATE TABLE supplier_d AS SELECT s_suppkey, s_name, n_name s_nation, r_name s_region
        FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey""")
    con.execute("CREATE TABLE part_d AS SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container FROM part")
    con.execute("""CREATE TABLE date_d AS SELECT CAST(strftime(d,'%Y%m%d') AS INT) d_datekey, d d_date, year(d) d_year,
        month(d) d_month, strftime(d,'%Y%m') d_yearmonth, dayofweek(d) d_dayofweek, weekofyear(d) d_weeknuminyear
        FROM (SELECT unnest(generate_series(DATE '1992-01-01', DATE '1998-12-31', INTERVAL 1 DAY)) d)""")
    for old, new in (("customer_d", "customer"), ("supplier_d", "supplier"), ("part_d", "part")):
        con.execute("DROP TABLE %s" % new)
        con.execute("ALTER TABLE %s RENAME TO %s" % (old, new))
    con.execute("ALTER TABLE date_d RENAME TO dates")
    for t in ("lineitem", "orders", "partsupp", "nation", "region"):
        con.execute("DROP TABLE %s" % t)


def write_files(con, table, order, dest, rows, rows_per_file):
    """Materialize the table in the requested order, then write contiguous row ranges, one file each (so the
    layout survives: a parallel COPY would interleave the order across files)."""
    con.execute("DROP TABLE IF EXISTS ordered")
    con.execute("CREATE TABLE ordered AS SELECT * FROM %s%s" % (table, order))
    files = max(1, -(-rows // rows_per_file))
    for i in range(files):
        con.execute("COPY (SELECT * FROM ordered WHERE rowid >= %d AND rowid < %d) TO '%s/part-%03d.parquet' "
                    "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)" % (i * rows_per_file, (i + 1) * rows_per_file, dest, i))
    con.execute("DROP TABLE ordered")
    return files


def write_tables(con, base, layout, rows_per_file):
    out = {}
    tables = [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1").fetchall()]
    for t in tables:
        rows = con.execute("SELECT count(*) FROM %s" % t).fetchone()[0]
        big = rows >= BIG_ROWS
        order = ""
        if big and layout == "sorted" and t in SORT_KEYS:
            order = " ORDER BY " + SORT_KEYS[t]
        elif big and layout == "shuffled":
            order = " ORDER BY random()"
        dest = "%s/%s" % (base, t)
        if not base.startswith("s3://"):
            os.makedirs(dest, exist_ok=True)
        if big:
            files = write_files(con, t, order, dest, rows, rows_per_file)
        else:
            con.execute("COPY (SELECT * FROM %s) TO '%s/part-000.parquet' (FORMAT parquet, COMPRESSION zstd)" % (t, dest))
            files = 1
        out[t] = {"rows": rows, "files": files, "ordered_by": order.replace(" ORDER BY ", "") or None}
    return out


def synthetic(con, base, args):
    """One fact table with controllable axes and a dimension table; see manifest for the axis values."""
    n, card, skew = args.rows, args.key_card, args.skew
    # skew 0 = uniform keys; larger = more mass on small keys (power transform of a uniform draw)
    key = "CAST(floor(%d * pow(random(), %f)) AS BIGINT)" % (card, 1.0 + 3.0 * skew)
    if args.key_type == "string":
        key_sql = "'k' || lpad(CAST(k AS VARCHAR), 12, '0')"
    else:
        key_sql = "k"
    con.execute("CREATE TABLE raw AS SELECT %s AS k, random() AS v, (random()*1000000)::BIGINT AS m, "
                "CASE WHEN random() < %f THEN NULL ELSE (random()*100)::INT END AS g FROM range(%d)" % (key, args.null_frac, n))
    con.execute("CREATE TABLE fact AS SELECT %s AS k, v, m, g FROM raw" % key_sql)
    keys = "(SELECT DISTINCT k FROM raw)"
    con.execute("CREATE TABLE dim AS SELECT %s AS k, (hash(k) %% 1000)::INT AS cat, random() AS w FROM %s" % (key_sql, keys))
    manifest = {"fact": {}, "dim": {}}
    for t, col in (("fact", "k"), ("dim", "k")):
        rows = con.execute("SELECT count(*) FROM %s" % t).fetchone()[0]
        if t == "fact":
            if args.clustering == "sorted":
                order = " ORDER BY k"
            elif args.clustering == "overlap":  # sorted into coarse buckets, random inside: files overlap partially
                order = " ORDER BY k // %d, random()" % max(1, card // max(1, args.files * 2))
            else:
                order = " ORDER BY random()"
        else:
            order = ""
        dest = "%s/%s" % (base, t)
        if not base.startswith("s3://"):
            os.makedirs(dest, exist_ok=True)
        if t == "fact":
            write_files(con, "fact", order, dest, rows, max(1, -(-rows // args.files)))
        else:
            con.execute("COPY dim TO '%s/part-0.parquet' (FORMAT parquet, COMPRESSION zstd)" % dest)
        manifest[t] = {"rows": rows, "ordered_by": order.replace(" ORDER BY ", "") or None}
    manifest["axes"] = {k: getattr(args, k) for k in ("rows", "key_card", "skew", "clustering", "files", "null_frac", "key_type")}
    return manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", required=True, choices=["tpch", "tpcds", "ssb", "synthetic"])
    ap.add_argument("--sf", default="1")
    ap.add_argument("--layout", default="asis", choices=["asis", "sorted", "shuffled"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--memory", default="10GB")
    ap.add_argument("--rows-per-file", type=int, default=2_000_000, help="rows per Parquet file for tables over 1M rows")
    ap.add_argument("--spill", default=None, help="local scratch dir (default: a temp dir)")
    # synthetic axes
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--key-card", type=int, default=100_000)
    ap.add_argument("--skew", type=float, default=0.0)
    ap.add_argument("--clustering", default="random", choices=["sorted", "overlap", "random"])
    ap.add_argument("--files", type=int, default=8)
    ap.add_argument("--null-frac", type=float, default=0.0)
    ap.add_argument("--key-type", default="int", choices=["int", "string"])
    args = ap.parse_args()

    spill = args.spill or tempfile.mkdtemp(prefix="datagen-")
    os.makedirs(spill, exist_ok=True)
    con = connect(spill, args.memory)
    if args.out.startswith("s3://"):
        load_s3(con)
    if args.suite == "synthetic":
        name = "syn_r%d_k%d_s%s_%s_f%d_n%s_%s" % (args.rows, args.key_card, args.skew, args.clustering, args.files, args.null_frac, args.key_type)
        base = "%s/%s" % (args.out.rstrip("/"), name.replace(".", "p"))
        manifest = synthetic(con, base, args)
    else:
        base = "%s/%s_sf%s_%s" % (args.out.rstrip("/"), args.suite, args.sf.replace(".", "p"), args.layout)
        build(con, args.suite, args.sf)
        manifest = write_tables(con, base, args.layout, args.rows_per_file)
        manifest = {"tables": manifest, "suite": args.suite, "sf": args.sf, "layout": args.layout}
    mpath = os.path.join(spill, "manifest.json")
    json.dump(manifest, open(mpath, "w"), indent=1)
    if base.startswith("s3://"):
        # DuckDB cannot write arbitrary files; store the manifest as a one-row parquet next to the data
        con.execute("COPY (SELECT ? AS manifest) TO '%s/manifest.parquet' (FORMAT parquet)" % base, [json.dumps(manifest)])
    else:
        json.dump(manifest, open(os.path.join(base, "manifest.json"), "w"), indent=1)
    print(json.dumps(manifest)[:500])


if __name__ == "__main__":
    sys.exit(main())
