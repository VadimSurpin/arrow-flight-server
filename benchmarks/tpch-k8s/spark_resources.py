#!/usr/bin/env python3
"""Runs TPC-H queries on the Spark cluster in direct (Parquet on S3) and flight (Arrow Flight connector) mode and
measures the CPU, memory and I/O of the pods while they run, like resources.py does for StarRocks.

Usage: spark_resources.py --kubectl PATH [--namespace tpch-bench] [--queries 1,3] [--runs 3] [--out r.json]
       spark_resources.py --summary FILE

The Spark driver pod (benchmarks/tpch-k8s/spark/spark_server.py) answers GET /run?q=N&mode=M with the wall time and rows;
this script reaches it through a kubectl port-forward. Pod groups: spark (workers), driver, flight servers, MinIO.
"""
import argparse
import json
import pathlib
import statistics
import subprocess
import sys
import time
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import resources  # noqa: E402  (PodSampler and its window arithmetic)
import run as bench  # noqa: E402  (normalise() and same())

GROUPS = {
    "spark": ["spark-worker-0", "spark-worker-1", "spark-worker-2"],
    "driver": [],  # generated pod name
    "flight": ["flight-0", "flight-1", "flight-2"],
    "minio": [],  # generated pod name
}
PORT = 18000


def call(q, mode, timeout):
    with urllib.request.urlopen("http://127.0.0.1:%d/run?q=%d&mode=%s" % (PORT, q, mode), timeout=timeout) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kubectl", required=True)
    ap.add_argument("--namespace", default="tpch-bench")
    ap.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--out", default="spark-resources.json")
    args = ap.parse_args()

    pods = [p.split("/")[1] for p in subprocess.run(
        [args.kubectl, "-n", args.namespace, "get", "pods", "-o", "name"], check=True,
        capture_output=True, text=True).stdout.split()]
    GROUPS["driver"] = [p for p in pods if p.startswith("spark-driver-")]
    GROUPS["minio"] = [p for p in pods if p.startswith("minio-")]
    forward = subprocess.Popen([args.kubectl, "-n", args.namespace, "port-forward", "svc/spark-driver",
                                "%d:8000" % PORT], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    samplers = {group: [resources.PodSampler(args.kubectl, args.namespace, pod) for pod in names]
                for group, names in GROUPS.items()}
    time.sleep(5)
    results = {}
    for q in [int(x) for x in args.queries.split(",")]:
        entry = {"direct": [], "flight": []}
        first_rows = {}
        for run in range(args.runs):
            for mode in (("direct", "flight") if run % 2 == 0 else ("flight", "direct")):
                start = time.time()
                try:
                    answer = call(q, mode, args.timeout)
                except Exception as e:  # the driver itself failed or timed out
                    answer = {"wall": time.time() - start, "rows": None, "error": str(e)[:300]}
                end = time.time()
                time.sleep(2.5)
                record = {"wall": round(answer["wall"], 2), "error": answer["error"]}
                for group, group_samplers in samplers.items():
                    windows = [w for w in (s.window(start, end) for s in group_samplers) if w]
                    if windows:
                        total = {k: sum(w[k] for w in windows) for k in windows[0]}
                        record[group] = {k: round(v, 2) for k, v in total.items()}
                        record[group]["cores"] = round(total["cpu_s"] / max(end - start, 0.1), 2)
                if answer["rows"] is not None and run == 0:
                    first_rows[mode] = bench.normalise([tuple(r) for r in answer["rows"]])
                entry[mode].append(record)
                print("q%02d %-6s run%d wall=%6.1fs %s" % (q, mode, run + 1, record["wall"], record["error"] or ""),
                      flush=True)
        if len(first_rows) == 2:
            entry["matches"] = bench.same(first_rows["direct"], first_rows["flight"])
        results[q] = entry
        pathlib.Path(args.out).write_text(json.dumps(results, indent=1))
    for group_samplers in samplers.values():
        for sampler in group_samplers:
            sampler.stop()
    forward.terminate()
    return 0


def summarise(path):
    """Prints the median per query and mode: time, CPU, memory over time and I/O of compute and storage."""
    data = json.loads(pathlib.Path(path).read_text())
    print("compute = spark workers + driver + flight pods; storage = MinIO. core-s = CPU seconds, GBs = GB x seconds "
          "of working-set memory, net = GB received by workers, conn = peak connections to MinIO")
    head = "%3s %-6s %6s | %8s %8s | %7s %6s | %7s %7s %7s | %6s %6s | %6s %6s %5s"
    print(head % ("Q", "mode", "wall", "compute", "storage", "compute", "extra", "worker", "spill", "MinIO",
                  "MinIO", "MinIO", "conn", "conn", "conn"))
    print(head % ("", "", "s", "core-s", "core-s", "GBs", "GBs", "net rx", "GB", "disk r", "net tx", "cores",
                  "flight", "spark", "all"))
    for q, entry in sorted(data.items(), key=lambda kv: int(kv[0])):
        for mode in ("direct", "flight"):
            ok = [r for r in entry[mode] if not r["error"] and "minio" in r]
            if not ok:
                print("%3s %-6s FAIL" % (q, mode))
                continue

            def med(fn):
                return statistics.median(fn(r) for r in ok)

            def grp(r, names, key):
                return sum(r[g][key] for g in names if g in r)
            compute = ("spark", "driver", "flight")
            print("%3s %-6s %6.1f | %8.0f %8.0f | %7.0f %6.0f | %7.1f %7.1f %7.1f | %6.1f %6.1f | %6.0f %6.0f %5.0f" % (
                q, mode, med(lambda r: r["wall"]),
                med(lambda r: grp(r, compute, "cpu_s")), med(lambda r: grp(r, ("minio",), "cpu_s")),
                med(lambda r: grp(r, compute, "mem_gbs")), med(lambda r: grp(r, compute, "mem_extra_gbs")),
                med(lambda r: grp(r, ("spark",), "rx_gb")),
                med(lambda r: grp(r, compute, "disk_w_gb")), med(lambda r: grp(r, ("minio",), "disk_r_gb")),
                med(lambda r: grp(r, ("minio",), "tx_gb")), med(lambda r: grp(r, ("minio",), "cores")),
                med(lambda r: grp(r, ("flight",), "conn_out_peak")), med(lambda r: grp(r, ("spark",), "conn_out_peak")),
                med(lambda r: grp(r, ("minio",), "conn_in_peak"))))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--summary":
        summarise(sys.argv[2])
        raise SystemExit(0)
    raise SystemExit(main())
