"""StarRocks backend for harness.py (--engine starrocks): runs a suite query through the FE's MySQL port against the native
`flight` catalog and answers in the same shape as the Spark driver's POST /run ({wall, rows, row_count, hash, error}), so
records and result hashes are comparable across engines.

Configuration: the "conf" of a run holds `flight.*` catalog properties (configs/starrocks.json). Those are catalog-level,
so when they differ from the properties the catalog was created with, the catalog is dropped and created again. Only
`flight` mode reads through the catalog; `direct` reads the same Parquet straight from S3 through FILES() views (database
direct_<schema>, one view per table of the schema, created on first use with the S3 settings from S3_ENDPOINT / S3_ACCESS_KEY /
S3_SECRET_KEY) with the BE data cache off for the session.
"""
import decimal
import hashlib
import os
import threading
import time

CATALOG = "flight"
BUCKET = "learn"
# The join filters of the dimension scans reach a fact scan 50-200 ms after it opens; at StarRocks' default wait of 20 ms the Flight
# scans start without them and move 5x the rows (q29: 9.7 s against 4.4 s with 500 ms). Direct mode keeps the StarRocks default.
FLIGHT_SCAN_WAIT_MS = 500
SINGLE_TABLE = {"clickbench": ["hits"], "taxi": ["yellow"]}  # the catalog lists no tables for clickbench (unsupported column type)


def cell(x):  # same normalisation as spark_server.cell
    if x is None:
        return None
    if isinstance(x, (int, float, decimal.Decimal)) and not isinstance(x, bool):
        return float(x)
    return str(x)


def digest(rows):  # same as spark_server.digest
    def norm(c):
        return "%.6g" % c if isinstance(c, float) else str(c)
    keys = sorted("|".join(norm(c) for c in r) for r in rows)
    return hashlib.sha256("\n".join(keys).encode()).hexdigest()[:16]


class StarRocks:
    def __init__(self, host, port, flight_uri, user="root", password="", token=None, connect=None):
        if connect is None:
            import pymysql
            connect = pymysql.connect
        self._connect = connect
        self.host, self.port, self.user, self.password = host, port, user, password
        self.base = {"type": "flight", "flight.uri": flight_uri}
        if token:
            self.base.update({"flight.auth.type": "token", "flight.auth.token": token})
        self.props = None  # catalog properties currently in force
        self.conn = None
        self.direct_ready = set()  # schemas whose direct_<schema> views exist

    def _open(self, timeout):
        self.conn = self._connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                  read_timeout=timeout, write_timeout=timeout, connect_timeout=30, autocommit=True)

    def _exec(self, sql):
        with self.conn.cursor() as c:
            c.execute(sql)
            return c.fetchall()

    def _ensure_catalog(self, extra):
        want = dict(self.base, **extra)
        if want == self.props:
            return
        props = ", ".join('"%s" = "%s"' % (k, str(v).replace('"', '\\"')) for k, v in want.items())
        self._exec("DROP CATALOG IF EXISTS %s" % CATALOG)
        self._exec("CREATE EXTERNAL CATALOG %s PROPERTIES (%s)" % (CATALOG, props))
        self.props = want

    def _files_props(self, schema, table):
        env = os.environ
        p = {"path": "s3://%s/%s/%s/*" % (BUCKET, schema, table), "format": "parquet",
             "aws.s3.endpoint": env.get("S3_ENDPOINT", "http://minio:9000"), "aws.s3.access_key": env["S3_ACCESS_KEY"],
             "aws.s3.secret_key": env["S3_SECRET_KEY"], "aws.s3.use_instance_profile": "false",
             "aws.s3.enable_path_style_access": "true", "aws.s3.region": "us-east-1"}
        return ", ".join('"%s" = "%s"' % kv for kv in p.items())

    def _ensure_direct(self, schema):
        """direct_<schema>: a FILES() view per table, the tables being those the flight catalog lists for the schema."""
        if schema in self.direct_ready:
            return
        self._exec("SET CATALOG %s" % CATALOG)
        tables = [r[0] for r in self._exec("SHOW TABLES FROM `%s`" % schema)] or SINGLE_TABLE.get(schema, [])
        self._exec("SET CATALOG default_catalog")
        self._exec("CREATE DATABASE IF NOT EXISTS `direct_%s`" % schema)
        for t in tables:
            self._exec("CREATE OR REPLACE VIEW `direct_%s`.`%s` AS SELECT * FROM FILES(%s)" % (schema, t, self._files_props(schema, t)))
        self.direct_ready.add(schema)

    def _kill(self, conn_id):
        try:
            other = self._connect(host=self.host, port=self.port, user=self.user, password=self.password, connect_timeout=30, autocommit=True)
            with other.cursor() as c:
                c.execute("KILL %d" % conn_id)
            other.close()
        except Exception:
            pass

    def run(self, body, timeout):
        """body: {"sql", "schema", "conf", "explain", "max_rows"} as for the Spark driver; `mode` is flight (default) or direct."""
        start = time.time()
        try:
            if self.conn is None:
                self._open(timeout)
            self.conn.ping(reconnect=True)
            direct = body.get("mode") == "direct"
            self._ensure_catalog({k: v for k, v in body["conf"].items() if k.startswith("flight.")})
            self._exec("SET query_timeout = %d" % timeout)
            self._exec("SET new_planner_optimize_timeout = 300000")  # the default 3 s fails big plans on a cold FE ("planner use long time")
            if direct:
                self._ensure_direct(body["schema"])
                self._exec("SET enable_scan_datacache = false")
                self._exec("SET enable_populate_datacache = false")
                self._exec("SET runtime_filter_scan_wait_time = %d" % body.get("scan_wait_ms", 20))
                self._exec("SET CATALOG default_catalog")
                self._exec("USE `direct_%s`" % body["schema"])
            else:
                self._exec("SET runtime_filter_scan_wait_time = %d" % body.get("scan_wait_ms", FLIGHT_SCAN_WAIT_MS))
                self._exec("SET CATALOG %s" % CATALOG)
                self._exec("USE `%s`" % body["schema"])
            if body.get("explain"):
                text = "\n".join(r[0] for r in self._exec("EXPLAIN " + body["sql"]))
                return {"wall": 0, "rows": [[text]], "row_count": 1, "hash": None, "error": None}
            conn_id = self.conn.thread_id()
            timer = threading.Timer(timeout, self._kill, (conn_id,))  # the server-side timeout does not always stop a scan
            timer.start()
            try:
                t0 = time.time()
                rows = [[cell(c) for c in r] for r in self._exec(body["sql"])]
                wall = round(time.time() - t0, 3)
            finally:
                timer.cancel()
            limit = body.get("max_rows", 1000)
            return {"wall": wall, "rows": rows if len(rows) <= limit else None, "row_count": len(rows), "hash": digest(rows), "error": None}
        except Exception as e:
            self.conn = None  # a broken or killed connection is replaced on the next run
            msg = str(e)[:400]
            timed_out = "timeout" in msg.lower() or "timed out" in msg.lower() or "Query was killed" in msg or "has gone away" in msg
            return {"wall": round(time.time() - start, 3), "rows": None, "row_count": None, "hash": None,
                    "error": ("client: " + msg) if timed_out else msg}
