import decimal
import os
import pathlib
import sys
import unittest
import unittest.mock

HERE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import sr_client  # noqa: E402


class FakeConn:
    log = []

    def __init__(self, **kw):
        self.kw = kw

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql):
        FakeConn.log.append(sql)
        self.sql = sql
        if "BOOM" in sql:
            raise RuntimeError("Query timeout")
        if "BAD" in sql:
            raise RuntimeError("Unknown table")

    def fetchall(self):
        if self.sql.startswith("EXPLAIN"):
            return [("PLAN 0",), ("  SCAN",)]
        if self.sql.startswith("SELECT"):
            return [(1, decimal.Decimal("2.5"), "a"), (2, None, "b")]
        return []

    def ping(self, reconnect=False):
        pass

    def thread_id(self):
        return 7

    def close(self):
        pass


def client():
    FakeConn.log = []
    return sr_client.StarRocks("h", 9030, "grpc://f:1", connect=FakeConn)


class SrClientTest(unittest.TestCase):
    def body(self, sql="SELECT 1", conf=None, **kw):
        return dict({"sql": sql, "schema": "tpch_sf100_asis", "conf": conf or {}, "max_rows": 50}, **kw)

    def test_result_shape_and_hash_like_spark(self):
        r = client().run(self.body(), 30)
        self.assertEqual((r["row_count"], r["error"]), (2, None))
        self.assertEqual(r["rows"], [[1.0, 2.5, "a"], [2.0, None, "b"]])
        self.assertEqual(r["hash"], sr_client.digest([[1.0, 2.5, "a"], [2.0, None, "b"]]))

    def test_catalog_recreated_only_when_properties_change(self):
        c = client()
        c.run(self.body(), 30)
        c.run(self.body(), 30)
        c.run(self.body(conf={"flight.aggregation_pushdown": "false", "spark.x": "1"}), 30)
        creates = [s for s in FakeConn.log if s.startswith("CREATE EXTERNAL CATALOG")]
        self.assertEqual(len(creates), 2)
        self.assertIn('"flight.aggregation_pushdown" = "false"', creates[1])
        self.assertNotIn("spark.x", creates[1])
        self.assertIn("USE `tpch_sf100_asis`", FakeConn.log)

    def test_flight_scans_wait_for_runtime_filters_longer_than_direct(self):
        os.environ.setdefault("S3_ACCESS_KEY", "k")
        os.environ.setdefault("S3_SECRET_KEY", "s")
        c = client()
        c.direct_ready.add("tpch_sf100_asis")
        c.run(self.body(), 30)
        c.run(self.body(mode="direct"), 30)
        c.run(self.body(scan_wait_ms=2000), 30)
        waits = [s for s in FakeConn.log if s.startswith("SET runtime_filter_scan_wait_time")]
        self.assertEqual(waits, ["SET runtime_filter_scan_wait_time = 500", "SET runtime_filter_scan_wait_time = 20",
                                 "SET runtime_filter_scan_wait_time = 2000"])

    def test_explain(self):
        r = client().run(self.body(explain=True), 30)
        self.assertEqual(r["rows"], [["PLAN 0\n  SCAN"]])

    def test_errors(self):
        c = client()
        self.assertTrue(c.run(self.body("SELECT BOOM"), 30)["error"].startswith("client: "))
        self.assertEqual(c.run(self.body("SELECT BAD"), 30)["error"], "Unknown table")

    def test_direct_builds_files_views_once_and_turns_cache_off(self):
        class Conn(FakeConn):
            def fetchall(self):
                if self.sql.startswith("SHOW TABLES"):
                    return [("orders",), ("lineitem",)]
                return super().fetchall()
        FakeConn.log = []
        c = sr_client.StarRocks("h", 9030, "grpc://f:1", connect=Conn)
        env = {"S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk"}
        with unittest.mock.patch.dict(os.environ, env):
            c.run(self.body(mode="direct"), 30)
            c.run(self.body(mode="direct"), 30)
        views = [s for s in FakeConn.log if s.startswith("CREATE OR REPLACE VIEW")]
        self.assertEqual(len(views), 2)  # second run reuses the views
        self.assertIn("`direct_tpch_sf100_asis`.`orders`", views[0])
        self.assertIn('"path" = "s3://learn/tpch_sf100_asis/orders/*"', views[0])
        self.assertIn("SET enable_scan_datacache = false", FakeConn.log)
        self.assertIn("SET enable_populate_datacache = false", FakeConn.log)
        self.assertIn("USE `direct_tpch_sf100_asis`", FakeConn.log)

    def test_direct_single_table_fallback_when_catalog_lists_none(self):
        FakeConn.log = []
        c = sr_client.StarRocks("h", 9030, "grpc://f:1", connect=FakeConn)
        with unittest.mock.patch.dict(os.environ, {"S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk"}):
            c.run(self.body(mode="direct", schema="clickbench"), 30)
        views = [s for s in FakeConn.log if s.startswith("CREATE OR REPLACE VIEW")]
        self.assertEqual(len(views), 1)
        self.assertIn("`direct_clickbench`.`hits`", views[0])

    def test_token(self):
        c = sr_client.StarRocks("h", 9030, "grpc://f:1", token="t", connect=FakeConn)
        self.assertEqual(c.base["flight.auth.type"], "token")


if __name__ == "__main__":
    unittest.main()
