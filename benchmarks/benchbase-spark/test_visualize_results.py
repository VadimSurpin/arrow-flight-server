#!/usr/bin/env python3
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("visualize-results.py")
SPEC = importlib.util.spec_from_file_location("visualize_results", MODULE_PATH)
VISUALIZE_RESULTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VISUALIZE_RESULTS)


class ReadCsvTest(unittest.TestCase):
    def test_read_csv_removes_nul_bytes_from_beeline_output(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "query-q5.actual.csv"
            csv_path.write_text("name,value\ncustomer,ab\0cd\n", encoding="utf-8")

            rows = VISUALIZE_RESULTS.read_csv(csv_path)

        self.assertEqual([{"name": "customer", "value": "abcd"}], rows)


class PerQueryLatencyTest(unittest.TestCase):
    def test_aggregates_measured_raw_latency_by_query(self):
        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            raw_path = results / "tpch_test.raw.csv"
            raw_path.write_text(
                "Transaction Name,Latency (microseconds)\n"
                "Q2,3000\n"
                "Q1,1000\n"
                "Q1,5000\n",
                encoding="utf-8",
            )

            rows = VISUALIZE_RESULTS.per_query_latency_rows(results, "tpch_test")

        self.assertEqual(
            [
                {"query": "Q1", "avg": 3.0, "samples": 2},
                {"query": "Q2", "avg": 3.0, "samples": 1},
            ],
            rows,
        )

    def test_grouped_chart_contains_query_labels_and_both_series(self):
        chart = VISUALIZE_RESULTS.svg_query_latency_chart(
            [{"query": "Q1", "avg": 10.0, "samples": 2}],
            [{"query": "Q22", "avg": 20.0, "samples": 3}],
            range(1, 23),
        )

        self.assertIn("q01", chart)
        self.assertIn("q22", chart)
        self.assertIn("Flight (ms)", chart)
        self.assertIn("Direct (ms)", chart)
        self.assertIn("Average Query Execution Time", chart)
        self.assertIn("average query execution time, ms", chart)


class ReadConfigTest(unittest.TestCase):
    def test_collects_queries_from_all_timed_work_phases(self):
        config_text = """<?xml version="1.0"?>
<parameters>
  <scalefactor>1</scalefactor>
  <terminals>1</terminals>
  <works>
    <work>
      <time>180</time>
      <warmup>20</warmup>
      <rate>unlimited</rate>
      <weights>100,0,0</weights>
    </work>
    <work>
      <time>180</time>
      <warmup>0</warmup>
      <rate>unlimited</rate>
      <weights>0,0,100</weights>
    </work>
  </works>
</parameters>
"""
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "tpch.xml"
            config_path.write_text(config_text, encoding="utf-8")

            config = VISUALIZE_RESULTS.read_config(config_path)

        self.assertEqual("Q1, Q3", config["queries"])
        self.assertEqual({1, 3}, VISUALIZE_RESULTS.configured_query_ids(config))


class PairedReportTest(unittest.TestCase):
    def test_renders_each_observation_and_aggregate_spread(self):
        machine_result = {
            "run": {
                "started_at": "2026-07-26T00:00:00Z",
                "finished_at": "2026-07-26T00:10:00Z",
                "policy": {
                    "paired_observations": 3,
                    "engine_order_schedule": [
                        {
                            "observation_index": 1,
                            "engine_order": ["flight", "direct"],
                        },
                        {
                            "observation_index": 2,
                            "engine_order": ["direct", "flight"],
                        },
                    ],
                    "warmup_seconds": 30,
                    "repetitions": 2,
                    "cache_policy": "warm-cache",
                },
            },
            "comparison": {
                "publication": {"state": "publishable", "reasons": []}
            },
            "observations": [
                {
                    "observation_index": 1,
                    "engine_order": ["flight", "direct"],
                    "status": "completed",
                    "failures": [],
                    "metrics": {
                        "flight": {
                            "samples": 2,
                            "latency_microseconds": {
                                "median": 1000,
                                "p95": 1500,
                            },
                        },
                        "direct": {
                            "samples": 2,
                            "latency_microseconds": {
                                "median": 2000,
                                "p95": 2500,
                            },
                        },
                    },
                    "engines": [
                        {
                            "id": "flight",
                            "status": "completed",
                            "artifact_refs": {
                                "report": "observations/observation-001/"
                                "flight/report.html"
                            },
                            "execution_paths": {
                                "classification": "duckdb-scan",
                                "pushdown_evidence": True,
                                "nodes": [],
                            },
                        },
                        {
                            "id": "direct",
                            "status": "completed",
                            "artifact_refs": {},
                            "execution_paths": {
                                "classification": "unknown",
                                "pushdown_evidence": False,
                                "nodes": [],
                            },
                        },
                    ],
                }
            ],
            "aggregate_summary": {
                "engines": {
                    engine: {
                        "successful_observations": 3,
                        "scheduled_observations": 3,
                        "latency_microseconds": {
                            "median": 1000,
                            "p25": 900,
                            "p75": 1100,
                            "iqr": 200,
                            "p95": 1200,
                        },
                        "queries": {
                            "q6": {
                                "observation_median_latency_microseconds": {
                                    "median": 1000,
                                    "iqr": 200,
                                    "p95": 1200,
                                }
                            }
                        },
                    }
                    for engine in ("flight", "direct")
                },
                "paired": {
                    "complete_pairs": 3,
                    "scheduled_pairs": 3,
                    "flight_to_direct_median_latency_ratio": {
                        "median": 0.5,
                        "iqr": 0.1,
                        "p95": 0.6,
                    },
                    "queries": {
                        "q6": {
                            "flight_to_direct_median_latency_ratio": {
                                "median": 0.5,
                                "iqr": 0.1,
                            }
                        }
                    },
                },
            },
        }

        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            output = results / "compare.report.html"
            VISUALIZE_RESULTS.build_machine_compare_report(
                results, output, machine_result
            )
            report = output.read_text(encoding="utf-8")

        self.assertIn("Paired Observations", report)
        self.assertIn("flight → direct", report)
        self.assertIn("Aggregate Statistics", report)
        self.assertIn("ratio IQR", report)
        self.assertIn("Per-query Aggregate Latency", report)
        self.assertIn("Q6", report)
        self.assertIn("warm-cache", report)


if __name__ == "__main__":
    unittest.main()
