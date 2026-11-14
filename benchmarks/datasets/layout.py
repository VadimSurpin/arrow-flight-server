#!/usr/bin/env python3
"""Data layout features from Parquet footers (no data is read).

  layout.py --root s3://learn --schemas tpcds_sf100_asis,tpcds_sf100_sorted,tpcds_sf100_shuffled --out runs/layout.jsonl
  layout.py --root /some/dir --schemas tpch_sf1_asis

Expects root/<schema>/<table>/*.parquet. One JSONL record per table:
  {type: "layout", schema, table, files, row_groups, rows, columns: {name: {...}}, sort_key, sortedness}
Per column (None when it cannot be computed or the column is constant):
  file_overlap  share of file pairs whose [min, max] ranges intersect: 0 = files hold disjoint ranges (sorted), 1 = every file spans the same range (shuffled)
  range_frac    median file range width / whole column range width (numeric, date and timestamp columns); about 1/files when sorted, about 1 when shuffled
  rg_overlap    the same pair share over the row groups inside one file, averaged over files with more than one row group
Table level: sort_key is the column with the lowest file_overlap (ties: lowest range_frac), sortedness = 1 - file_overlap of it.

Credentials for s3:// roots: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD / MINIO_ENDPOINT (default minio:9000).
"""
import argparse
import datetime
import decimal
import json
import os
import statistics
import sys

import pyarrow.fs as pafs
import pyarrow.parquet as pq


def open_fs(root):
    if root.startswith("s3://"):
        fs = pafs.S3FileSystem(endpoint_override=os.environ.get("MINIO_ENDPOINT", "minio:9000"), scheme="http",
                               access_key=os.environ["MINIO_ROOT_USER"], secret_key=os.environ["MINIO_ROOT_PASSWORD"])
        return fs, root[len("s3://"):].rstrip("/")
    return pafs.LocalFileSystem(), os.path.abspath(root)


def files_of(fs, directory):
    infos = fs.get_file_info(pafs.FileSelector(directory, recursive=True))
    return sorted(i.path for i in infos if i.is_file and i.path.endswith(".parquet"))


def number(v):
    """Float for ordering arithmetic, None for values that have no meaningful width (strings, bytes)."""
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float, decimal.Decimal)):
        return float(v)
    if isinstance(v, datetime.datetime):
        return v.timestamp() if v.tzinfo else v.replace(tzinfo=datetime.timezone.utc).timestamp()
    if isinstance(v, datetime.date):
        return float(v.toordinal())
    return None


def overlap_share(ranges):
    n = len(ranges)
    if n < 2:
        return None
    hits = sum(1 for i in range(n) for j in range(i + 1, n) if ranges[i][0] <= ranges[j][1] and ranges[j][0] <= ranges[i][1])
    return hits / (n * (n - 1) / 2)


def column_ranges(fs, paths):
    """name -> (per file (min, max), per file list of row group (min, max)); a column is dropped when any row group lacks statistics."""
    per_file = {}
    broken = set()
    rows = 0
    for p in paths:
        with fs.open_input_file(p) as f:
            md = pq.ParquetFile(f).metadata
        rows += md.num_rows
        local = {}
        for rg in range(md.num_row_groups):
            for c in range(md.num_columns):
                col = md.row_group(rg).column(c)
                name = col.path_in_schema
                st = col.statistics
                if st is None or not st.has_min_max:
                    broken.add(name)
                    continue
                local.setdefault(name, []).append((st.min, st.max))
        for name, rgs in local.items():
            per_file.setdefault(name, []).append(rgs)
    out = {}
    for name, files in per_file.items():
        if name in broken or len(files) != len(paths):
            continue
        out[name] = files
    return out, rows


def column_features(files):
    spans = [(min(r[0] for r in rgs), max(r[1] for r in rgs)) for rgs in files]
    try:
        lo, hi = min(s[0] for s in spans), max(s[1] for s in spans)
        constant = lo == hi
    except TypeError:
        return {"file_overlap": None, "range_frac": None, "rg_overlap": None}
    if constant:
        return {"file_overlap": None, "range_frac": None, "rg_overlap": None}
    widths = None
    nlo, nhi = number(lo), number(hi)
    if nlo is not None and nhi is not None and nhi > nlo:
        widths = [(number(b) - number(a)) / (nhi - nlo) for a, b in spans]
    inside = [s for s in (overlap_share(rgs) for rgs in files) if s is not None]
    return {"file_overlap": round(overlap_share(spans), 4) if len(spans) > 1 else None,
            "range_frac": round(statistics.median(widths), 4) if widths else None,
            "rg_overlap": round(sum(inside) / len(inside), 4) if inside else None}


def table_features(root, schema, table):
    fs, base = open_fs(root)
    paths = files_of(fs, "%s/%s/%s" % (base, schema, table))
    ranges, rows = column_ranges(fs, paths)
    cols = {name: column_features(files) for name, files in ranges.items()}
    row_groups = sum(len(rgs) for rgs in next(iter(ranges.values()))) if ranges else 0
    informative = [(c["file_overlap"], c["range_frac"] if c["range_frac"] is not None else 1.0, n)
                   for n, c in cols.items() if c["file_overlap"] is not None]
    best = min(informative) if informative else None
    return {"type": "layout", "schema": schema, "table": table, "files": len(paths), "row_groups": row_groups, "rows": rows,
            "columns": cols, "sort_key": best[2] if best else None,
            "sortedness": round(1 - best[0], 4) if best else None}


def schema_features(root, schema):
    fs, base = open_fs(root)
    tables = sorted(i.base_name for i in fs.get_file_info(pafs.FileSelector("%s/%s" % (base, schema))) if i.type == pafs.FileType.Directory)
    return [table_features(root, schema, t) for t in tables]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True)
    ap.add_argument("--schemas", required=True, help="comma separated")
    ap.add_argument("--out", help="JSONL file (default stdout)")
    args = ap.parse_args()
    out = open(args.out, "w") if args.out else sys.stdout
    for schema in args.schemas.split(","):
        for rec in schema_features(args.root, schema):
            out.write(json.dumps(rec) + "\n")
            out.flush()
            print("%s.%s files=%d sort_key=%s sortedness=%s" % (schema, rec["table"], rec["files"], rec["sort_key"], rec["sortedness"]),
                  file=sys.stderr)


if __name__ == "__main__":
    main()
