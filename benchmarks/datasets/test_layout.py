import pathlib
import random
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import layout  # noqa: E402


def write_table(root, schema, table, files):
    d = pathlib.Path(root) / schema / table
    d.mkdir(parents=True)
    for i, cols in enumerate(files):
        pq.write_table(pa.table(cols), d / ("part-%03d.parquet" % i), row_group_size=25)


def keys(n_files, rows, order):
    ks = list(range(n_files * rows))
    if order == "shuffled":
        random.Random(1).shuffle(ks)
    return [ks[i * rows:(i + 1) * rows] for i in range(n_files)]


class LayoutTest(unittest.TestCase):
    def features(self, order):
        with tempfile.TemporaryDirectory() as root:
            chunks = keys(6, 100, order)
            write_table(root, "s", "t", [{"k": c, "const": [7] * 100, "name": ["n%04d" % x for x in c]} for c in chunks])
            return layout.table_features(root, "s", "t")

    def test_sorted_files_do_not_overlap(self):
        f = self.features("sorted")
        self.assertEqual(f["files"], 6)
        self.assertEqual(f["columns"]["k"]["file_overlap"], 0.0)
        self.assertAlmostEqual(f["columns"]["k"]["range_frac"], 1 / 6, places=2)
        self.assertEqual(f["sort_key"], "k")
        self.assertEqual(f["sortedness"], 1.0)

    def test_shuffled_files_overlap_everywhere(self):
        f = self.features("shuffled")
        self.assertGreater(f["columns"]["k"]["file_overlap"], 0.99)
        self.assertGreater(f["columns"]["k"]["range_frac"], 0.8)
        self.assertLess(f["sortedness"], 0.05)

    def test_constant_columns_are_not_informative(self):
        f = self.features("sorted")
        self.assertIsNone(f["columns"]["const"]["file_overlap"])
        self.assertNotEqual(f["sort_key"], "const")

    def test_strings_report_overlap_without_range_fraction(self):
        f = self.features("sorted")
        self.assertEqual(f["columns"]["name"]["file_overlap"], 0.0)
        self.assertIsNone(f["columns"]["name"]["range_frac"])

    def test_row_group_overlap_inside_files(self):
        sorted_f = self.features("sorted")
        shuffled_f = self.features("shuffled")
        self.assertEqual(sorted_f["columns"]["k"]["rg_overlap"], 0.0)
        self.assertGreater(shuffled_f["columns"]["k"]["rg_overlap"], 0.9)

    def test_files_sorted_inside_but_overlapping_across(self):
        with tempfile.TemporaryDirectory() as root:
            write_table(root, "s", "t", [{"k": list(range(100))} for _ in range(4)])
            f = layout.table_features(root, "s", "t")
        self.assertEqual(f["columns"]["k"]["rg_overlap"], 0.0)
        self.assertEqual(f["columns"]["k"]["file_overlap"], 1.0)

    def test_schema_listing(self):
        with tempfile.TemporaryDirectory() as root:
            write_table(root, "s", "a", [{"k": [1, 2]}])
            write_table(root, "s", "b", [{"k": [3, 4]}])
            recs = layout.schema_features(root, "s")
        self.assertEqual([r["table"] for r in recs], ["a", "b"])
        self.assertTrue(all(r["type"] == "layout" and r["schema"] == "s" for r in recs))


if __name__ == "__main__":
    unittest.main()
