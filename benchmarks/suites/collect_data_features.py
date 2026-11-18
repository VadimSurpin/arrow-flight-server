#!/usr/bin/env python3
"""collect_data_features.py OUT: per (schema, table) data facts for the classifier -> OUT (jsonl, resumable).
From MinIO (mc inside the minio pod): file count, bytes, file-size spread. From the Spark driver in direct mode (reads Parquet footers/columns, no Flight):
row count; for tables with several files and >= KEY_ROWS rows, per-file min/max of the first numeric column (the clustering key):
overlap_factor = sum of per-file ranges / global range (1 = files cover disjoint ranges, ~files = every file spans everything), and its approximate NDV."""
import collections, json, os, pathlib, statistics as st, subprocess, sys
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness, suites

KEY_ROWS = 5_000_000
NUMERIC = ("bigint", "int", "smallint", "tinyint", "double", "float", "decimal", "date", "timestamp")
URL = "http://127.0.0.1:18000"
KUBECTL = str(HERE / ".." / ".." / "tools" / ".local" / "bin" / "kubectl")


def schemas():
    seen = {}
    for e in ("spark",):
        for l in open(HERE / "runs" / "features" / ("features-%s.jsonl" % e)):
            r = json.loads(l)
            seen[r["schema"]] = r["suite"]
    return seen


def listing():
    sh = ('mc alias set l http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; '
          'mc ls --recursive --json l/learn/')
    out = subprocess.run([KUBECTL, "-n", "tpch-bench", "exec", "deploy/minio", "--", "sh", "-c", sh], capture_output=True, text=True, timeout=600).stdout
    files = collections.defaultdict(list)
    for l in out.splitlines():
        try:
            x = json.loads(l)
        except ValueError:
            continue
        if x.get("type") == "file" and x["key"].endswith(".parquet"):
            parts = x["key"].split("/")
            if len(parts) >= 3:
                files[(parts[0], parts[1])].append(x["size"])
            else:
                files[(parts[0], None)].append(x["size"])
    return files


def sql(schema, conf, text, timeout=1500):
    a = harness.post(URL, {"sql": text, "schema": schema, "mode": "direct", "conf": conf, "max_rows": 100000}, timeout)
    if a["error"]:
        raise RuntimeError(a["error"][:200])
    return a["rows"]


def main():
    out = pathlib.Path(sys.argv[1])
    out.parent.mkdir(parents=True, exist_ok=True)
    done = {(json.loads(l)["schema"], json.loads(l)["table"]) for l in out.read_text().splitlines()} if out.exists() else set()
    files = listing()
    for schema, suite in schemas().items():
        conf = suites.SUITES[suite]["conf"]
        try:
            tables = [r[1] for r in sql(schema, conf, "SHOW TABLES")]
        except Exception as e:
            print(schema, "SHOW TABLES failed", e, flush=True)
            continue
        for t in tables:
            if (schema, t) in done:
                continue
            fs = files.get((schema, t)) or files.get((schema, None)) or []
            rec = {"schema": schema, "suite": suite, "table": t, "files": len(fs), "bytes": sum(fs),
                   "file_cv": round(st.pstdev(fs) / st.mean(fs), 3) if len(fs) > 1 else 0.0}
            try:
                rec["rows"] = int(sql(schema, conf, "SELECT count(*) FROM `%s`" % t)[0][0])
                cols = [(r[0], r[1]) for r in sql(schema, conf, "DESCRIBE `%s`" % t)]
                rec["columns"] = len(cols)
                key = next((c for c, ty in cols if ty.split("(")[0] in NUMERIC), None)
                rec["key"] = key
                if key and rec["rows"] >= KEY_ROWS and rec["files"] > 1:
                    per = sql(schema, conf, "SELECT input_file_name(), min(`%s`), max(`%s`) FROM `%s` GROUP BY 1" % (key, key, t))
                    per = [(float(lo), float(hi)) for _, lo, hi in per if lo is not None]
                    lo, hi = min(p[0] for p in per), max(p[1] for p in per)
                    span = max(hi - lo, 1.0)
                    rec["key_overlap"] = round(sum(max(h - l, 0.0) for l, h in per) / span, 3)
                    rec["key_ndv"] = int(sql(schema, conf, "SELECT approx_count_distinct(`%s`) FROM `%s`" % (key, t))[0][0])
            except Exception as e:
                rec["error"] = str(e)[:200]
            with out.open("a") as f:
                f.write(json.dumps(rec) + "\n")
            print(schema, t, rec.get("rows"), rec.get("key_overlap"), rec.get("error") or "", flush=True)


main()
