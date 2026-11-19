"""Long-lived Spark driver for the TPC-H benchmark: one session, two table sets, queries over HTTP.

GET /run?q=6&mode=direct|flight runs /opt/queries/q06.sql against Parquet read straight from S3 ("direct") or through
the Arrow Flight connector ("flight") and answers JSON: {"wall": seconds, "rows": [...], "error": null}. The session
stays up between requests, so executors are warm like the StarRocks backends are.
"""
import decimal
import hashlib
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


BUCKET = os.environ.get("DATA_BUCKET", "learn")
REGISTERED = set()


def list_tables(spark, schema):
    """Table names of a schema: the sub-directories of s3a://<bucket>/<schema>."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path("s3a://%s/%s" % (BUCKET, schema))
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return sorted(st.getPath().getName() for st in fs.listStatus(path) if st.isDirectory())


def ensure_schema(spark, schema):
    """Registers <schema> tables once as direct_<schema> (Parquet on S3) and flight_<schema> (Flight connector)."""
    if schema in REGISTERED:
        return
    flight = os.environ.get("FLIGHT_HOST", "flight-0.flight")
    for t in list_tables(spark, schema):
        for mode in ("direct", "flight"):
            spark.sql("CREATE DATABASE IF NOT EXISTS %s_%s" % (mode, schema))
            spark.sql("DROP TABLE IF EXISTS %s_%s.%s" % (mode, schema, t))
        spark.sql("CREATE TABLE direct_%s.%s USING parquet LOCATION 's3a://%s/%s/%s'" % (schema, t, BUCKET, schema, t))
        spark.sql("CREATE TABLE flight_%s.%s USING %s OPTIONS (host '%s', port '32010', user 'bench', password 'bench', "
                  "table '%s.%s')" % (schema, t, CONNECTOR, flight, schema, t))
    REGISTERED.add(schema)


def digest(rows):
    """Order-independent fingerprint of a result: row count plus a hash of the rows with floats cut to 6 digits."""
    def norm(c):
        return "%.6g" % c if isinstance(c, float) else str(c)
    keys = sorted("|".join(norm(c) for c in r) for r in rows)
    return hashlib.sha256("\n".join(keys).encode()).hexdigest()[:16]


def run_sql(spark, sql, schema, mode, conf=(), explain=False, max_rows=1000):
    """The generic entry the harness uses: any SQL over the tables of one schema in one mode."""
    sql = sql.strip().rstrip(";")
    with LOCK:
        ensure_schema(spark, schema)
        spark.sql("USE %s_%s" % (mode, schema))
        previous = {k: spark.conf.get(k, None) for k, _ in conf}
        for k, v in conf:
            spark.conf.set(k, v)
        start = time.time()
        try:
            if explain:
                plan = spark.sql("EXPLAIN FORMATTED " + sql).collect()[0][0]
                return {"wall": 0, "rows": [[plan]], "row_count": 1, "hash": None, "error": None}
            ansi_retry = False
            try:
                rows = [[cell(c) for c in r] for r in spark.sql(sql).collect()]
            except Exception as e:
                # same Spark 3.5 bug as in run_query: a pushed-down decimal avg() overflows while the optimizer plans a
                # scalar subquery, before any data is read. Planning fails fast, so run again with ANSI off.
                if "ARITHMETIC_OVERFLOW" not in str(e) or time.time() - start > 5:
                    raise
                ansi_retry = True
                spark.conf.set("spark.sql.ansi.enabled", "false")
                try:
                    rows = [[cell(c) for c in r] for r in spark.sql(sql).collect()]
                finally:
                    spark.conf.set("spark.sql.ansi.enabled", "true")
            wall = round(time.time() - start, 3)
            return {"wall": wall, "rows": rows if len(rows) <= max_rows else None, "row_count": len(rows),
                    "hash": digest(rows), "error": None, "ansi_retry": ansi_retry}
        except Exception as e:
            return {"wall": round(time.time() - start, 3), "rows": None, "row_count": None, "hash": None,
                    "error": str(e)[:400]}
        finally:
            for k, v in previous.items():
                if v is None:
                    spark.conf.unset(k)
                else:
                    spark.conf.set(k, v)


def cell(x):
    if x is None:
        return None
    if isinstance(x, (int, float, decimal.Decimal)) and not isinstance(x, bool):
        return float(x)
    return str(x)


def run_query(spark, q, mode, conf=(), explain=False):
    """Runs a query; conf holds (key, value) Spark settings that apply to this query only; explain returns the plan."""
    sql = open("/opt/queries/q%02d.sql" % q).read().strip().rstrip(";")
    with LOCK:
        spark.sql("USE " + mode)
        previous = {k: spark.conf.get(k, None) for k, _ in conf}
        for k, v in conf:
            spark.conf.set(k, v)
        start = time.time()
        if explain:
            plan = spark.sql("EXPLAIN FORMATTED " + sql).collect()[0][0]
            return {"wall": 0, "rows": [[plan]], "error": None}
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
        finally:
            for k, v in previous.items():
                if v is None:
                    spark.conf.unset(k)
                else:
                    spark.conf.set(k, v)


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    register(spark)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            args = parse_qs(url.query)
            if url.path == "/run":
                conf = [tuple(c.split("=", 1)) for c in args.get("conf", [""])[0].split(",") if "=" in c]
                body = run_query(spark, int(args["q"][0]), args["mode"][0], conf, "explain" in args)
            else:
                body = {"ok": True}
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):  # noqa: N802
            """POST /run with {"sql", "schema", "mode", "conf": {k: v}, "explain", "max_rows"} (see run_sql);
            POST /cancel stops the running jobs (the harness calls it when a query times out)."""
            if self.path == "/cancel":
                spark.sparkContext.cancelAllJobs()
                data = b'{"cancelled": true}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            answer = run_sql(spark, body["sql"], body["schema"], body["mode"], list(body.get("conf", {}).items()),
                             bool(body.get("explain")), int(body.get("max_rows", 1000)))
            data = json.dumps(answer).encode()
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
