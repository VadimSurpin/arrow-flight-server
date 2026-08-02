#!/usr/bin/env python3
"""Arrow Flight vs direct HDFS Parquet timing comparison via Spark.

Registers both table sets in Spark, runs TPC-H queries against both engines
for the requested number of repetitions (first rep is warmup), then writes
results as JSON and prints a markdown summary.

Parameters come from CLI args first; environment variables are the defaults
so the Docker Compose ci-compare service can drive everything via -e flags.
"""
import argparse
import json
import math
import os
import pathlib
import time

from pyspark.sql import SparkSession

FLIGHT_SOURCE_PROVIDER = "pro.surpin.data.arrowflight.client.spark.FlightSource"

TPCH_TABLES = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem",
]

# All 22 TPC-H queries adapted for Spark SQL.
# {db} is substituted at runtime with the target database name.
TPCH_QUERIES = {
    "q1": """
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity)                                        AS sum_qty,
               SUM(l_extendedprice)                                   AS sum_base_price,
               SUM(l_extendedprice * (1 - l_discount))                AS sum_disc_price,
               SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
               AVG(l_quantity)      AS avg_qty,
               AVG(l_extendedprice) AS avg_price,
               AVG(l_discount)      AS avg_disc,
               COUNT(*)             AS count_order
        FROM {db}.lineitem
        WHERE l_shipdate <= date '1998-09-01'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "q2": """
        SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr,
               s_address, s_phone, s_comment
        FROM {db}.part, {db}.supplier, {db}.partsupp, {db}.nation, {db}.region
        WHERE p_partkey = ps_partkey
          AND s_suppkey = ps_suppkey
          AND p_size = 15
          AND p_type LIKE '%BRASS'
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'EUROPE'
          AND ps_supplycost = (
              SELECT MIN(ps_supplycost)
              FROM {db}.partsupp, {db}.supplier, {db}.nation, {db}.region
              WHERE p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
          )
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        LIMIT 100
    """,
    "q3": """
        SELECT l_orderkey,
               SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               o_orderdate, o_shippriority
        FROM {db}.customer, {db}.orders, {db}.lineitem
        WHERE c_mktsegment = 'BUILDING'
          AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate < date '1995-03-15'
          AND l_shipdate  > date '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
    """,
    "q4": """
        SELECT o_orderpriority, COUNT(*) AS order_count
        FROM {db}.orders
        WHERE o_orderdate >= date '1993-07-01'
          AND o_orderdate  < date '1993-10-01'
          AND EXISTS (
              SELECT * FROM {db}.lineitem
              WHERE l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate
          )
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
    """,
    "q5": """
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM {db}.customer, {db}.orders, {db}.lineitem,
             {db}.supplier, {db}.nation, {db}.region
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey  = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= date '1994-01-01'
          AND o_orderdate  < date '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    """,
    "q6": """
        SELECT SUM(l_extendedprice * l_discount) AS revenue
        FROM {db}.lineitem
        WHERE l_shipdate >= date '1994-01-01'
          AND l_shipdate  < date '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
    """,
    "q7": """
        SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
        FROM (
            SELECT n1.n_name AS supp_nation,
                   n2.n_name AS cust_nation,
                   YEAR(l_shipdate) AS l_year,
                   l_extendedprice * (1 - l_discount) AS volume
            FROM {db}.supplier, {db}.lineitem, {db}.orders, {db}.customer,
                 {db}.nation n1, {db}.nation n2
            WHERE s_suppkey  = l_suppkey
              AND o_orderkey = l_orderkey
              AND c_custkey  = o_custkey
              AND s_nationkey = n1.n_nationkey
              AND c_nationkey = n2.n_nationkey
              AND (
                (n1.n_name = 'FRANCE'  AND n2.n_name = 'GERMANY')
                OR
                (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
              )
              AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31'
        ) AS shipping
        GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year
    """,
    "q8": """
        SELECT o_year,
               SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)
               / SUM(volume) AS mkt_share
        FROM (
            SELECT YEAR(o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) AS volume,
                   n2.n_name AS nation
            FROM {db}.part, {db}.supplier, {db}.lineitem, {db}.orders,
                 {db}.customer, {db}.nation n1, {db}.nation n2, {db}.region
            WHERE p_partkey = l_partkey
              AND s_suppkey  = l_suppkey
              AND l_orderkey = o_orderkey
              AND o_custkey  = c_custkey
              AND c_nationkey = n1.n_nationkey
              AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA'
              AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN date '1995-01-01' AND date '1996-12-31'
              AND p_type = 'ECONOMY ANODIZED STEEL'
        ) AS all_nations
        GROUP BY o_year
        ORDER BY o_year
    """,
    "q9": """
        SELECT nation, o_year, SUM(amount) AS sum_profit
        FROM (
            SELECT n_name AS nation,
                   YEAR(o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount)
                   - ps_supplycost * l_quantity AS amount
            FROM {db}.part, {db}.supplier, {db}.lineitem,
                 {db}.partsupp, {db}.orders, {db}.nation
            WHERE s_suppkey  = l_suppkey
              AND ps_suppkey = l_suppkey
              AND ps_partkey = l_partkey
              AND p_partkey  = l_partkey
              AND o_orderkey = l_orderkey
              AND s_nationkey = n_nationkey
              AND p_name LIKE '%green%'
        ) AS profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
    """,
    "q10": """
        SELECT c_custkey, c_name,
               SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               c_acctbal, n_name, c_address, c_phone, c_comment
        FROM {db}.customer, {db}.orders, {db}.lineitem, {db}.nation
        WHERE c_custkey  = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate >= date '1993-10-01'
          AND o_orderdate  < date '1994-01-01'
          AND l_returnflag = 'R'
          AND c_nationkey  = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone,
                 n_name, c_address, c_comment
        ORDER BY revenue DESC
        LIMIT 20
    """,
    "q11": """
        SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
        FROM {db}.partsupp, {db}.supplier, {db}.nation
        WHERE ps_suppkey   = s_suppkey
          AND s_nationkey  = n_nationkey
          AND n_name = 'GERMANY'
        GROUP BY ps_partkey
        HAVING SUM(ps_supplycost * ps_availqty) > (
            SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
            FROM {db}.partsupp, {db}.supplier, {db}.nation
            WHERE ps_suppkey  = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
        )
        ORDER BY value DESC
    """,
    "q12": """
        SELECT l_shipmode,
               SUM(CASE WHEN o_orderpriority = '1-URGENT'
                          OR o_orderpriority = '2-HIGH'
                        THEN 1 ELSE 0 END) AS high_line_count,
               SUM(CASE WHEN o_orderpriority <> '1-URGENT'
                         AND o_orderpriority <> '2-HIGH'
                        THEN 1 ELSE 0 END) AS low_line_count
        FROM {db}.orders, {db}.lineitem
        WHERE o_orderkey   = l_orderkey
          AND l_shipmode IN ('MAIL', 'SHIP')
          AND l_commitdate < l_receiptdate
          AND l_shipdate   < l_commitdate
          AND l_receiptdate >= date '1994-01-01'
          AND l_receiptdate  < date '1995-01-01'
        GROUP BY l_shipmode
        ORDER BY l_shipmode
    """,
    "q13": """
        SELECT c_count, COUNT(*) AS custdist
        FROM (
            SELECT c_custkey, COUNT(o_orderkey) AS c_count
            FROM {db}.customer
            LEFT OUTER JOIN {db}.orders
              ON c_custkey = o_custkey
             AND o_comment NOT LIKE '%special%requests%'
            GROUP BY c_custkey
        ) AS c_orders
        GROUP BY c_count
        ORDER BY custdist DESC, c_count DESC
    """,
    "q14": """
        SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                 THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
               / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM {db}.lineitem
        JOIN {db}.part ON l_partkey = p_partkey
        WHERE l_shipdate >= date '1995-09-01'
          AND l_shipdate  < date '1995-10-01'
    """,
    "q15": """
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
        FROM {db}.supplier
        JOIN (
            SELECT l_suppkey AS supplier_no,
                   SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM {db}.lineitem
            WHERE l_shipdate >= date '1996-01-01'
              AND l_shipdate  < date '1996-04-01'
            GROUP BY l_suppkey
        ) AS revenue ON s_suppkey = supplier_no
        WHERE total_revenue = (
            SELECT MAX(total_revenue)
            FROM (
                SELECT SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM {db}.lineitem
                WHERE l_shipdate >= date '1996-01-01'
                  AND l_shipdate  < date '1996-04-01'
                GROUP BY l_suppkey
            ) AS revenue2
        )
        ORDER BY s_suppkey
    """,
    "q16": """
        SELECT p_brand, p_type, p_size,
               COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM {db}.partsupp, {db}.part
        WHERE p_partkey = ps_partkey
          AND p_brand <> 'Brand#45'
          AND p_type NOT LIKE 'MEDIUM POLISHED%'
          AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
          AND ps_suppkey NOT IN (
              SELECT s_suppkey FROM {db}.supplier
              WHERE s_comment LIKE '%Customer%Complaints%'
          )
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
    """,
    "q17": """
        SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
        FROM {db}.lineitem, {db}.part
        WHERE p_partkey = l_partkey
          AND p_brand     = 'Brand#23'
          AND p_container = 'MED BOX'
          AND l_quantity < (
              SELECT 0.2 * AVG(l_quantity)
              FROM {db}.lineitem
              WHERE l_partkey = p_partkey
          )
    """,
    "q18": """
        SELECT c_name, c_custkey, o_orderkey,
               o_orderdate, o_totalprice, SUM(l_quantity)
        FROM {db}.customer, {db}.orders, {db}.lineitem
        WHERE o_orderkey IN (
            SELECT l_orderkey
            FROM {db}.lineitem
            GROUP BY l_orderkey
            HAVING SUM(l_quantity) > 300
        )
        AND c_custkey  = o_custkey
        AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100
    """,
    "q19": """
        SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM {db}.lineitem, {db}.part
        WHERE (
            p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
            AND l_quantity >= 1 AND l_quantity <= 11
            AND p_size BETWEEN 1 AND 5
            AND l_shipmode IN ('AIR','AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        ) OR (
            p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 20
            AND p_size BETWEEN 1 AND 10
            AND l_shipmode IN ('AIR','AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        ) OR (
            p_partkey = l_partkey
            AND p_brand = 'Brand#34'
            AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 30
            AND p_size BETWEEN 1 AND 15
            AND l_shipmode IN ('AIR','AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        )
    """,
    "q20": """
        SELECT s_name, s_address
        FROM {db}.supplier, {db}.nation
        WHERE s_suppkey IN (
            SELECT ps_suppkey
            FROM {db}.partsupp
            WHERE ps_partkey IN (
                SELECT p_partkey FROM {db}.part
                WHERE p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT 0.5 * SUM(l_quantity)
                FROM {db}.lineitem
                WHERE l_partkey = ps_partkey
                  AND l_suppkey = ps_suppkey
                  AND l_shipdate >= date '1994-01-01'
                  AND l_shipdate  < date '1995-01-01'
            )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
        ORDER BY s_name
    """,
    "q21": """
        SELECT s_name, COUNT(*) AS numwait
        FROM {db}.supplier, {db}.lineitem l1, {db}.orders, {db}.nation
        WHERE s_suppkey  = l1.l_suppkey
          AND o_orderkey = l1.l_orderkey
          AND o_orderstatus = 'F'
          AND l1.l_receiptdate > l1.l_commitdate
          AND EXISTS (
              SELECT * FROM {db}.lineitem l2
              WHERE l2.l_orderkey = l1.l_orderkey
                AND l2.l_suppkey <> l1.l_suppkey
          )
          AND NOT EXISTS (
              SELECT * FROM {db}.lineitem l3
              WHERE l3.l_orderkey = l1.l_orderkey
                AND l3.l_suppkey <> l1.l_suppkey
                AND l3.l_receiptdate > l3.l_commitdate
          )
          AND s_nationkey = n_nationkey
          AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name
        ORDER BY numwait DESC, s_name
        LIMIT 100
    """,
    "q22": """
        SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
        FROM (
            SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal
            FROM {db}.customer
            WHERE SUBSTRING(c_phone, 1, 2) IN
                  ('13','31','23','29','30','18','17')
              AND c_acctbal > (
                  SELECT AVG(c_acctbal)
                  FROM {db}.customer
                  WHERE c_acctbal > 0.00
                    AND SUBSTRING(c_phone, 1, 2) IN
                        ('13','31','23','29','30','18','17')
              )
              AND NOT EXISTS (
                  SELECT * FROM {db}.orders
                  WHERE o_custkey = c_custkey
              )
        ) AS custsale
        GROUP BY cntrycode
        ORDER BY cntrycode
    """,
}


def sql_string(v):
    return "'" + str(v).replace("'", "''") + "'"


def quote_id(v):
    return "`" + v.replace("`", "``") + "`"


def parse_queries(value):
    if value.strip().lower() == "all":
        return list(TPCH_QUERIES.keys())
    names = []
    for token in value.lower().replace(" ", "").split(","):
        if not token:
            continue
        name = token if token.startswith("q") else f"q{token}"
        if name not in TPCH_QUERIES:
            raise ValueError(
                f"Unknown query: {token!r}. Available: {list(TPCH_QUERIES)}"
            )
        names.append(name)
    return names


def register_direct_db(spark, db, schema, hdfs_root):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_id(db)}")
    for table in TPCH_TABLES:
        t = f"{quote_id(db)}.{quote_id(table)}"
        location = f"{hdfs_root.rstrip('/')}/{schema}/{table}"
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        spark.sql(f"CREATE TABLE {t} USING parquet LOCATION {sql_string(location)}")
    print(f"Registered direct HDFS Parquet tables in database '{db}'")


def register_flight_db(spark, db, schema, host, port):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_id(db)}")
    for table in TPCH_TABLES:
        t = f"{quote_id(db)}.{quote_id(table)}"
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        spark.sql(f"""
            CREATE TABLE {t}
            USING {FLIGHT_SOURCE_PROVIDER}
            OPTIONS (
              host          {sql_string(host)},
              port          {sql_string(port)},
              user          'user',
              password      'password',
              `tls.enabled` 'false',
              table         {sql_string(f'{schema}.{table}')}
            )
        """)
    print(f"Registered Arrow Flight tables in database '{db}' (host={host}:{port})")


def run_query(spark, sql):
    t0 = time.perf_counter()
    count = spark.sql(sql).count()
    return time.perf_counter() - t0, count


def timing_stats(times_s):
    n = len(times_s)
    if n == 0:
        return {}
    avg = sum(times_s) / n
    variance = sum((t - avg) ** 2 for t in times_s) / max(n - 1, 1)
    return {
        "times_ms": [round(t * 1000, 1) for t in times_s],
        "avg_ms": round(avg * 1000, 1),
        "min_ms": round(min(times_s) * 1000, 1),
        "max_ms": round(max(times_s) * 1000, 1),
        "stddev_ms": round(math.sqrt(variance) * 1000, 1),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Arrow Flight vs Direct HDFS Parquet comparison"
    )
    parser.add_argument(
        "--flight-host",
        default=os.environ.get("FLIGHT_SOURCE_HOST", "flight-server-1"),
    )
    parser.add_argument(
        "--flight-port",
        default=os.environ.get("FLIGHT_SOURCE_PORT", "32010"),
    )
    parser.add_argument(
        "--hdfs-root",
        default=os.environ.get("HDFS_DATA_DIR", "hdfs://hdfs-namenode:8020/bench"),
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("BENCHMARK_SCHEMA", "tpch"),
    )
    parser.add_argument(
        "--queries",
        default=os.environ.get("COMPARE_QUERIES", "q1,q6,q14"),
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=int(os.environ.get("COMPARE_REPETITIONS", "3")),
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=float(os.environ.get("BENCHMARK_SCALE_FACTOR", "0.1")),
    )
    parser.add_argument(
        "--out",
        default=os.environ.get("COMPARE_OUT", "/results/compare-results.json"),
    )
    args = parser.parse_args()

    query_names = parse_queries(args.queries)
    reps = args.repetitions

    spark = (
        SparkSession.builder
        .appName("ArrowFlightCICompare")
        .enableHiveSupport()
        # Required for aggregate pushdown to the Flight connector: non-ANSI mode
        # wraps decimal arithmetic in CheckOverflow, which Spark's V2 expression
        # translator rejects, silently disabling SupportsPushDownAggregates.
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    flight_db = f"{args.schema}_flight"
    direct_db = f"{args.schema}_direct"

    register_direct_db(spark, direct_db, args.schema, args.hdfs_root)
    register_flight_db(spark, flight_db, args.schema, args.flight_host, args.flight_port)

    results = {
        "scale_factor": args.scale_factor,
        "schema": args.schema,
        "flight_host": args.flight_host,
        "flight_port": args.flight_port,
        "repetitions": reps,
        "queries": {},
    }

    failed = []
    for qname in query_names:
        template = TPCH_QUERIES[qname]
        flight_sql = template.format(db=flight_db)
        direct_sql = template.format(db=direct_db)

        if os.environ.get("COMPARE_EXPLAIN") == "1":
            print(f"\n=== {qname.upper()} — flight physical plan ===")
            try:
                spark.sql(flight_sql).explain(mode="formatted")
            except Exception as e:
                print(f"  explain FAILED: {e}")

        print(f"\n=== {qname.upper()} — warmup ===")
        try:
            run_query(spark, direct_sql)
            run_query(spark, flight_sql)
        except Exception as e:
            print(f"  warmup FAILED: {e}")
            failed.append(qname)
            continue

        flight_times: list[float] = []
        direct_times: list[float] = []
        ok = True
        for rep in range(1, reps + 1):
            try:
                t, c = run_query(spark, flight_sql)
                print(f"  [flight rep {rep}/{reps}] {t * 1000:.0f} ms  rows={c}")
                flight_times.append(t)

                t, c = run_query(spark, direct_sql)
                print(f"  [direct rep {rep}/{reps}] {t * 1000:.0f} ms  rows={c}")
                direct_times.append(t)
            except Exception as e:
                print(f"  rep {rep} FAILED: {e}")
                ok = False
                failed.append(qname)
                break

        if ok:
            results["queries"][qname] = {
                "flight": timing_stats(flight_times),
                "direct": timing_stats(direct_times),
            }

    if failed:
        results["failed"] = failed
        print(f"\nFailed queries: {failed}")

    print("\n## Arrow Flight vs Direct HDFS Parquet — Results\n")
    print(
        f"Scale factor: {args.scale_factor}  |  "
        f"Repetitions: {reps}  |  "
        f"Schema: {args.schema}\n"
    )
    print("| Query | Flight avg (ms) | Direct avg (ms) | Speedup |")
    print("|-------|:--------------:|:--------------:|:-------:|")
    for qname, data in results["queries"].items():
        f_avg = data["flight"]["avg_ms"]
        d_avg = data["direct"]["avg_ms"]
        speedup = round(d_avg / f_avg, 2) if f_avg else 0
        direction = "faster" if speedup >= 1.0 else "slower"
        print(f"| {qname.upper()} | {f_avg} | {d_avg} | {speedup}x ({direction}) |")

    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\nResults written to {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()
