#!/usr/bin/env python3
import importlib.util
import sys
import tempfile
import types
import unittest
import xml.etree.ElementTree as element_tree
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CAPTURE_RESULTS = load_module("capture_query_results", "capture-query-results.py")
sys.modules.setdefault("duckdb", types.ModuleType("duckdb"))
GENERATE_DATA = load_module("generate_duckdb_data", "generate-duckdb-data.py")
REPEAT_WORK = load_module("repeat_work_phases", "repeat-work-phases.py")
SPLIT_TIMED_WORK = load_module(
    "split_timed_work_phases", "split-timed-work-phases.py"
)


class AllQuerySelectorTest(unittest.TestCase):
    def test_capture_selector_expands_all_queries(self):
        self.assertEqual(set(range(1, 23)), CAPTURE_RESULTS.parse_query_ids("all"))

    def test_generator_selector_expands_all_queries(self):
        self.assertEqual(list(range(1, 23)), GENERATE_DATA.parse_query_ids("ALL"))


class RepeatWorkPhasesTest(unittest.TestCase):
    def test_repeats_serial_phase_without_duplicating_transactions(self):
        config_text = """<?xml version="1.0"?>
<parameters>
  <works>
    <work>
      <serial>true</serial>
      <weights>50,50</weights>
    </work>
  </works>
  <transactiontypes>
    <transactiontype><name>Q1</name><id>1</id></transactiontype>
    <transactiontype><name>Q2</name><id>2</id></transactiontype>
  </transactiontypes>
</parameters>
"""
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "tpch.xml"
            config.write_text(config_text, encoding="utf-8")

            REPEAT_WORK.repeat_work_phases(config, 50)

            root = element_tree.parse(config).getroot()

        phases = root.findall("./works/work")
        self.assertEqual(50, len(phases))
        self.assertTrue(
            all(phase.findtext("weights") == "50,50" for phase in phases)
        )
        self.assertEqual(
            2, len(root.findall("./transactiontypes/transactiontype"))
        )

    def test_rejects_zero_repetitions(self):
        with self.assertRaisesRegex(ValueError, "positive integer"):
            REPEAT_WORK.repeat_work_phases(Path("unused.xml"), 0)


class SplitTimedWorkPhasesTest(unittest.TestCase):
    def test_creates_one_timed_phase_per_active_query(self):
        config_text = """<?xml version="1.0"?>
<parameters>
  <works>
    <work>
      <serial>false</serial>
      <rate>unlimited</rate>
      <time>180</time>
      <warmup>20</warmup>
      <weights>50,0,50</weights>
    </work>
  </works>
</parameters>
"""
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "tpch.xml"
            config.write_text(config_text, encoding="utf-8")

            count = SPLIT_TIMED_WORK.split_timed_work_phases(config)

            phases = element_tree.parse(config).getroot().findall(
                "./works/work"
            )

        self.assertEqual(2, count)
        self.assertEqual(
            ["100,0,0", "0,0,100"],
            [phase.findtext("weights") for phase in phases],
        )
        self.assertTrue(
            all(phase.findtext("time") == "180" for phase in phases)
        )
        self.assertEqual(
            ["20", "0"],
            [phase.findtext("warmup") for phase in phases],
        )

    def test_creates_22_independent_tpch_phases(self):
        weights = ",".join("1" for _ in range(22))
        config_text = f"""<?xml version="1.0"?>
<parameters>
  <works>
    <work>
      <time>180</time>
      <warmup>20</warmup>
      <weights>{weights}</weights>
    </work>
  </works>
</parameters>
"""
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "tpch.xml"
            config.write_text(config_text, encoding="utf-8")

            count = SPLIT_TIMED_WORK.split_timed_work_phases(config)

            phases = element_tree.parse(config).getroot().findall(
                "./works/work"
            )

        self.assertEqual(22, count)
        self.assertEqual(22, len(phases))
        for query_index, phase in enumerate(phases):
            phase_weights = phase.findtext("weights").split(",")
            self.assertEqual("100", phase_weights[query_index])
            self.assertEqual(1, phase_weights.count("100"))
            self.assertEqual("180", phase.findtext("time"))

    def test_rejects_timed_workload_without_active_queries(self):
        config_text = """<?xml version="1.0"?>
<parameters>
  <works>
    <work>
      <time>180</time>
      <weights>0,0</weights>
    </work>
  </works>
</parameters>
"""
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "tpch.xml"
            config.write_text(config_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "at least one query"):
                SPLIT_TIMED_WORK.split_timed_work_phases(config)


if __name__ == "__main__":
    unittest.main()
