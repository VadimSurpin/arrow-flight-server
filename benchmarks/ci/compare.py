#!/usr/bin/env python3
"""Arrow Flight vs direct HDFS Parquet timing comparison via Spark.

Registers both table sets in Spark, runs each TPC-H query against both
engines for the requested number of repetitions (first rep is warmup),
then writes results as JSON and prints a markdown summary.

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
    "q6": """
        SELECT SUM(l_extendedprice * l_discount) AS revenue
        FROM {db}.lineitem
        WHERE l_shipdate >= date '1994-01-01'
          AND l_shipdate  < date '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
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
}


def sql_string(v):
    return "'" + str(v).replace("'", "''") + "'"


def quote_id(v):
    return "`" + v.replace("`", "``") + "`"


def parse_queries(value):
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

    for qname in query_names:
        template = TPCH_QUERIES[qname]
        flight_sql = template.format(db=flight_db)
        direct_sql = template.format(db=direct_db)

        print(f"\n=== {qname.upper()} — warmup ===")
        run_query(spark, direct_sql)
        run_query(spark, flight_sql)

        flight_times: list[float] = []
        direct_times: list[float] = []
        for rep in range(1, reps + 1):
            t, c = run_query(spark, flight_sql)
            print(f"  [flight rep {rep}/{reps}] {t * 1000:.0f} ms  rows={c}")
            flight_times.append(t)

            t, c = run_query(spark, direct_sql)
            print(f"  [direct rep {rep}/{reps}] {t * 1000:.0f} ms  rows={c}")
            direct_times.append(t)

        results["queries"][qname] = {
            "flight": timing_stats(flight_times),
            "direct": timing_stats(direct_times),
        }

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
