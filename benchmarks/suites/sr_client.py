"""StarRocks backend for harness.py (--engine starrocks): runs a suite query through the FE's MySQL port against the native
`flight` catalog and answers in the same shape as the Spark driver's POST /run ({wall, rows, row_count, hash, error}), so
records and result hashes are comparable across engines.

Configuration: the "conf" of a run holds `flight.*` catalog properties (configs/starrocks.json). Those are catalog-level,
so when they differ from the properties the catalog was created with, the catalog is dropped and created again. Only
`flight` mode exists (there is no StarRocks counterpart of Spark's direct S3 read yet).
"""
import decimal
import hashlib
import threading
import time

CATALOG = "flight"


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

    def _kill(self, conn_id):
        try:
            other = self._connect(host=self.host, port=self.port, user=self.user, password=self.password, connect_timeout=30, autocommit=True)
            with other.cursor() as c:
                c.execute("KILL %d" % conn_id)
            other.close()
        except Exception:
            pass

    def run(self, body, timeout):
        """body: {"sql", "schema", "conf", "explain", "max_rows"} as for the Spark driver; `mode` is ignored."""
        start = time.time()
        try:
            if self.conn is None:
                self._open(timeout)
            self.conn.ping(reconnect=True)
            self._ensure_catalog({k: v for k, v in body["conf"].items() if k.startswith("flight.")})
            self._exec("SET query_timeout = %d" % timeout)
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
