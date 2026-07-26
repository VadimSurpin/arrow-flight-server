#!/usr/bin/env python3
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("build-pages-site.py")
SPEC = importlib.util.spec_from_file_location("build_pages_site", MODULE_PATH)
BUILD_PAGES_SITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD_PAGES_SITE)


class AllQueryChartTest(unittest.TestCase):
    def test_latest_all_compare_is_rendered_on_index(self):
        query_rows = [{"query": "Q1", "avg": 12.0, "samples": 4}]
        run = {
            "kind": "compare",
            "id": "tpch-compare-all-test",
            "title": "tpch-compare-all-test",
            "benchmark": "tpch",
            "path": "flight vs direct",
            "query": "all",
            "scale": 1,
            "timestamp": "2026-07-23T10:00:00",
            "report": "benchmarks/test/compare.report.html",
            "files": "benchmarks/test",
            "flight": {
                "throughput": 1,
                "avgMs": 12,
                "report": "",
                "queryLatencies": query_rows,
            },
            "direct": {
                "throughput": 1,
                "avgMs": 10,
                "report": "",
                "queryLatencies": query_rows,
            },
            "flightNodes": 4,
        }

        page = BUILD_PAGES_SITE.build_index([run])

        self.assertIn("Latest TPC-H Q1-Q22 Average Query Execution Time", page)
        self.assertIn("average query execution time, ms", page)
        self.assertIn("q01", page)
        self.assertIn("q22", page)
        self.assertIn("Flight (ms)", page)
        self.assertIn("Direct (ms)", page)


class MachineResultTest(unittest.TestCase):
    def test_loads_paired_aggregate_instead_of_one_engine_run(self):
        machine_result = {
            "run": {
                "benchmark": "tpch",
                "finished_at": "2026-07-26T10:00:00Z",
                "workload": {"query_set": "q6", "scale_factor": 1.0},
                "topology": {
                    "cluster_nodes": 3,
                    "flight_hosts": ["flight-server-1"],
                },
            },
            "validation": {"valid": True},
            "comparison": {
                "publication": {"state": "publishable", "reasons": []}
            },
            "observations": [{"observation_index": 1}],
            "aggregate_summary": {
                "engines": {
                    "flight": {
                        "total_samples": 6,
                        "latency_microseconds": {
                            "median": 4000,
                            "p95": 6000,
                        },
                        "throughput_requests_per_second": {"median": 2.5},
                        "queries": {
                            "q6": {
                                "observation_median_latency_microseconds": {
                                    "median": 4000,
                                    "count": 3,
                                }
                            }
                        },
                    },
                    "direct": {
                        "total_samples": 6,
                        "latency_microseconds": {
                            "median": 8000,
                            "p95": 10000,
                        },
                        "throughput_requests_per_second": {"median": 1.5},
                        "queries": {},
                    },
                }
            },
        }

        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            run_dir = results / "tpch-compare-q6-test"
            run_dir.mkdir()
            (run_dir / "compare.report.html").write_text(
                "report", encoding="utf-8"
            )
            (run_dir / "benchmark-result.json").write_text(
                json.dumps(machine_result), encoding="utf-8"
            )

            run = BUILD_PAGES_SITE.load_compare_run(results, run_dir)

        self.assertEqual(4.0, run["flight"]["avgMs"])
        self.assertEqual(2.5, run["flight"]["throughput"])
        self.assertEqual("Q6", run["flight"]["queryLatencies"][0]["query"])
        self.assertEqual(3, run["flightNodes"])


if __name__ == "__main__":
    unittest.main()
