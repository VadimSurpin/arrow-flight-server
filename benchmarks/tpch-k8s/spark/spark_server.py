"""Long-lived Spark driver for the TPC-H benchmark: one session, two table sets, queries over HTTP.

GET /run?q=6&mode=direct|flight runs /opt/queries/q06.sql against Parquet read straight from S3 ("direct") or through
the Arrow Flight connector ("flight") and answers JSON: {"wall": seconds, "rows": [...], "error": null}. The session
stays up between requests, so executors are warm like the StarRocks backends are.
"""
import decimal
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from pyspark.sql import SparkSession

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
CONNECTOR = "pro.surpin.data.arrowflight.client.spark.FlightSource"
LOCK = __import__("threading").Lock()


def build_session():
    builder = (SparkSession.builder.appName("tpch-bench")
               .config("spark.sql.catalog.spark_catalog", "pro.surpin.data.arrowflight.client.spark.FlightSessionCatalog")
               .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
               .config("spark.hadoop.fs.s3a.path.style.access", "true")
               .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
               .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
               .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
               .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    return builder.getOrCreate()


def register(spark):
    flight = os.environ.get("FLIGHT_HOST", "flight-0.flight")
    spark.sql("CREATE DATABASE IF NOT EXISTS direct")
    spark.sql("CREATE DATABASE IF NOT EXISTS flight")
    for t in TABLES:
        spark.sql("DROP TABLE IF EXISTS direct.%s" % t)
        spark.sql("CREATE TABLE direct.%s USING parquet LOCATION 's3a://tpch/sf100/%s'" % (t, t))
        spark.sql("DROP TABLE IF EXISTS flight.%s" % t)
        spark.sql("CREATE TABLE flight.%s USING %s OPTIONS (host '%s', port '32010', user 'bench', password 'bench', "
                  "table 'sf100.%s')" % (t, CONNECTOR, flight, t))


def cell(x):
    if x is None:
        return None
    if isinstance(x, (int, float, decimal.Decimal)) and not isinstance(x, bool):
        return float(x)
    return str(x)


def run_query(spark, q, mode):
    sql = open("/opt/queries/q%02d.sql" % q).read().strip().rstrip(";")
    with LOCK:
        spark.sql("USE " + mode)
        start = time.time()
        try:
            try:
                rows = [[cell(c) for c in r] for r in spark.sql(sql).collect()]
            except Exception as e:
                # Spark 3.5 bug: with ANSI on, the optimizer evaluates a pushed-down decimal avg() over zero rows
                # while decorrelating a scalar subquery (TPC-H Q17) and throws ARITHMETIC_OVERFLOW before any data
                # is read. Planning fails instantly, so run again with ANSI off; the aggregate is still pushed.
                if "ARITHMETIC_OVERFLOW" not in str(e) or time.time() - start > 5:
                    raise
                spark.conf.set("spark.sql.ansi.enabled", "false")
                try:
                    rows = [[cell(c) for c in r] for r in spark.sql(sql).collect()]
                finally:
                    spark.conf.set("spark.sql.ansi.enabled", "true")
            return {"wall": round(time.time() - start, 3), "rows": rows, "error": None}
        except Exception as e:  # reported to the harness, which records the failure and moves on
            return {"wall": round(time.time() - start, 3), "rows": None, "error": str(e)[:400]}


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    register(spark)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            args = parse_qs(url.query)
            if url.path == "/run":
                body = run_query(spark, int(args["q"][0]), args["mode"][0])
            else:
                body = {"ok": True}
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()


if __name__ == "__main__":
    main()
