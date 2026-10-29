import json
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

HERE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
import suites  # noqa: E402


class StubDriver(BaseHTTPRequestHandler):
    """Answers like spark_server.py: rows depend on the configuration so a 'bad' config produces a different result."""
    seen = []

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        StubDriver.seen.append(dict(body, path=self.path))
        if self.path == "/cancel":
            answer = {"cancelled": True}
        elif body.get("explain"):
            answer = {"wall": 0, "rows": [["== Physical Plan ==\n*(2) HashAggregate\n+- Exchange\n   +- *(1) Scan parquet"]], "row_count": 1, "hash": None, "error": None}
        elif body["conf"].get("slow"):
            time.sleep(3)
            answer = {"wall": 3.0, "rows": None, "row_count": None, "hash": None, "error": None}
        elif body["conf"].get("fail") == "oom":
            answer = {"wall": 1.5, "rows": None, "row_count": None, "hash": None, "error": "java.lang.OutOfMemoryError: heap"}
        else:
            value = 2.0 if body["conf"].get("wrong") else 1.0
            answer = {"wall": 2.0, "rows": [[1, value]], "row_count": 1, "hash": "h%s" % value, "error": None}
        data = json.dumps(answer).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


class HarnessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), StubDriver)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d" % cls.server.server_port

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def run_harness(self, configs, queries="1-2", **extra):
        tmp = pathlib.Path(tempfile.mkdtemp())
        cfg = tmp / "configs.json"
        cfg.write_text(json.dumps(configs))
        out = tmp / "runs.jsonl"
        args = [sys.executable, str(HERE / "harness.py"), "run", "--suite", "tpch", "--queries", queries, "--configs", str(cfg),
                "--reps", "2", "--url", self.url, "--out", str(out)]
        for k, v in extra.items():
            args += ["--" + k, v]
        subprocess.run(args, check=True, capture_output=True)
        return out, [json.loads(l) for l in out.read_text().splitlines()]

    def test_writes_features_warmups_and_only_distinct_configurations(self):
        configs = {"default": {"direct": {}, "flight": {}}, "knob": {"direct": {}, "flight": {"a": "1"}}}
        _, recs = self.run_harness(configs)
        self.assertEqual(2, sum(r["type"] == "features" for r in recs))
        runs = [r for r in recs if r["type"] == "run"]
        self.assertEqual(2 * 2, sum(r["warmup"] for r in runs))  # one per mode and query
        measured = [r for r in runs if not r["warmup"]]
        # per query: default direct x2, default flight x2, knob flight x2; the knob equals default for direct
        self.assertEqual(2 * 6, len(measured))
        self.assertEqual({("default", "direct"), ("default", "flight"), ("knob", "flight")}, {(r["config"], r["mode"]) for r in measured})
        self.assertEqual({"operators": {"Scan": 1, "HashAggregate": 1, "Exchange": 1}}, {"operators": {k: v for k, v in recs[0]["operators"].items() if k in ("Scan", "HashAggregate", "Exchange")}})

    def test_check_flags_a_configuration_that_changes_the_result(self):
        configs = {"default": {"direct": {}, "flight": {}}, "bad": {"direct": {}, "flight": {"wrong": "1"}}}
        out, _ = self.run_harness(configs, queries="1")
        done = subprocess.run([sys.executable, str(HERE / "harness.py"), "check", str(out)], capture_output=True, text=True)
        self.assertEqual(1, done.returncode)
        self.assertIn("MISMATCH", done.stdout)

    def test_check_passes_when_results_agree(self):
        out, _ = self.run_harness({"default": {"direct": {}, "flight": {}}}, queries="1")
        done = subprocess.run([sys.executable, str(HERE / "harness.py"), "check", str(out)], capture_output=True, text=True)
        self.assertEqual(0, done.returncode, done.stdout)

    def test_memory_errors_make_a_run_invalid(self):
        configs = {"default": {"direct": {}, "flight": {}}, "oom": {"direct": {}, "flight": {"fail": "oom"}}}
        _, recs = self.run_harness(configs, queries="1")
        oom = [r for r in recs if r["type"] == "run" and r["config"] == "oom"]
        self.assertTrue(oom and all(not r["valid"] for r in oom))
        self.assertTrue(all(r["valid"] for r in recs if r["type"] == "run" and r["config"] == "default"))

    def test_a_timed_out_configuration_is_cancelled_and_not_repeated(self):
        StubDriver.seen.clear()
        configs = {"default": {"direct": {}, "flight": {}}, "slow": {"direct": {}, "flight": {"slow": "1"}}}
        _, recs = self.run_harness(configs, queries="1", timeout="1", warmup="0")
        slow = [r for r in recs if r["type"] == "run" and r["config"] == "slow"]
        self.assertEqual(1, len(slow))  # the second repetition is skipped
        self.assertTrue(slow[0]["error"].startswith("client:"))
        self.assertTrue(any(s["path"] == "/cancel" for s in StubDriver.seen))

    def test_rows_equal_within_tolerance_in_any_order(self):
        self.assertTrue(harness.same_rows([[1, 1000000.0], [2, 3.0]], [[2, 3.0000000001], [1, 1000000.0004]]))
        self.assertFalse(harness.same_rows([[1, 1.0]], [[1, 1.01]]))
        self.assertFalse(harness.same_rows([[1, 1.0]], [[1, 1.0], [2, 2.0]]))

    def test_query_ids_and_ranges(self):
        self.assertEqual(["q01", "q02", "q03", "q09"], suites.parse_ids("tpch", "1-3,9"))
        self.assertEqual(99, len(suites.query_ids("tpcds")))
        self.assertEqual(43, len(suites.query_ids("clickbench")))
        with self.assertRaises(SystemExit):
            suites.parse_ids("tpch", "99")


if __name__ == "__main__":
    unittest.main()
