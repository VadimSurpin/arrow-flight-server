#!/usr/bin/env python3
"""Runs TPC-H queries in direct and flight mode and measures the CPU and memory of the pods while they run.

Usage: resources.py FE_HOST FE_PORT --kubectl PATH [--namespace tpch-bench] [--queries 1,3] [--runs 3] [--out r.json]

A shell loop in every pod prints the cgroup counters once per second: CPU time (cpu.stat usage_usec), memory.current and
inactive_file. Working-set memory is memory.current minus inactive_file, the figure `kubectl top` reports. Per
query run it reports wall time, CPU seconds used by each pod group (flight servers, StarRocks BEs, FE, MinIO),
the average cores that means, and the peak working set. Modes alternate per run so neither always runs on the other's
warm caches. Each CPU figure is the counter difference between the samples around the query, so a query shorter than
a couple of seconds is only accurate to about one second of CPU time.
"""
import argparse
import json
import pathlib
import statistics
import subprocess
import sys
import threading
import time

import pymysql

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run as bench  # noqa: E402  (normalise() and same() compare result sets)

GROUPS = {
    "flight": ["flight-0", "flight-1", "flight-2"],
    "be": ["starrocks-be-0", "starrocks-be-1", "starrocks-be-2"],
    "fe": ["starrocks-fe-0"],
    "minio": [],  # filled with the generated pod name
}
LOOP = ('while :; do u=$(grep usage_usec /sys/fs/cgroup/cpu.stat); i=$(grep ^inactive_file /sys/fs/cgroup/memory.stat); '
        'echo "${u#usage_usec } $(cat /sys/fs/cgroup/memory.current) ${i#inactive_file }"; sleep 1; done')


class PodSampler:
    """Streams one sample per second from a pod: (wall time, cpu seconds, working set bytes)."""

    def __init__(self, kubectl, namespace, pod):
        self.pod = pod
        self.samples = []
        self._lock = threading.Lock()
        self._proc = subprocess.Popen([kubectl, "-n", namespace, "exec", pod, "--", "sh", "-c", LOOP],
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self):
        for line in self._proc.stdout:
            parts = line.split()
            if len(parts) != 3:
                continue
            try:
                cpu, current, inactive = int(parts[0]) / 1e6, int(parts[1]), int(parts[2])
            except ValueError:
                continue
            with self._lock:
                self.samples.append((time.time(), cpu, current - inactive))

    def window(self, start, end):
        """CPU seconds used and peak working set between two wall-clock times, or None without samples."""
        with self._lock:
            samples = list(self.samples)
        before = [s for s in samples if s[0] <= start]
        after = [s for s in samples if s[0] >= end]
        inside = [s for s in samples if start <= s[0] <= end]
        if not before or not after:
            return None
        first, last = before[-1], after[0]
        peak = max([s[2] for s in inside] + [first[2], last[2]])
        return last[1] - first[1], peak

    def stop(self):
        self._proc.terminate()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("port", type=int)
    ap.add_argument("--kubectl", required=True)
    ap.add_argument("--namespace", default="tpch-bench")
    ap.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--out", default="resources.json")
    args = ap.parse_args()

    pods = subprocess.run([args.kubectl, "-n", args.namespace, "get", "pods", "-o", "name"], check=True,
                          capture_output=True, text=True).stdout.split()
    GROUPS["minio"] = [p.split("/")[1] for p in pods if p.split("/")[1].startswith("minio-")]
    samplers = {group: [PodSampler(args.kubectl, args.namespace, pod) for pod in names]
                for group, names in GROUPS.items()}
    time.sleep(3)

    def connect():
        connection = pymysql.connect(host=args.host, port=args.port, user="root", autocommit=True,
                                     read_timeout=args.timeout + 120, connect_timeout=10)
        cursor = connection.cursor()
        cursor.execute("SET query_timeout = %d" % args.timeout)
        return cursor

    cur = connect()
    dbs = {"direct": "default_catalog.direct", "flight": "flight_c.sf100"}
    results = {}
    for q in [int(x) for x in args.queries.split(",")]:
        sql = (HERE / "queries" / ("q%02d.sql" % q)).read_text()
        entry = {"direct": [], "flight": []}
        first_rows = {}
        for run in range(args.runs):
            for mode in (("direct", "flight") if run % 2 == 0 else ("flight", "direct")):
                cur.execute("USE " + dbs[mode])
                start = time.time()
                error = None
                try:
                    cur.execute(sql)
                    rows = cur.fetchall()
                except pymysql.MySQLError as e:
                    error, rows = str(e)[:200], None
                    if isinstance(e, pymysql.err.OperationalError):
                        cur = connect()
                end = time.time()
                time.sleep(2.5)  # let the sampler print a sample after the query
                record = {"wall": round(end - start, 2), "error": error}
                for group, group_samplers in samplers.items():
                    windows = [w for w in (s.window(start, end) for s in group_samplers) if w]
                    if windows:
                        cpu = sum(w[0] for w in windows)
                        record[group] = {"cpu_s": round(cpu, 1), "cores": round(cpu / record["wall"], 2),
                                         "peak_gb": round(sum(w[1] for w in windows) / 2**30, 2)}
                if rows is not None and run == 0:
                    first_rows[mode] = bench.normalise(rows)
                entry[mode].append(record)
                print("q%02d %-6s run%d wall=%6.1fs %s" % (q, mode, run + 1, record["wall"], error or ""), flush=True)
        if len(first_rows) == 2:
            entry["matches"] = bench.same(first_rows["direct"], first_rows["flight"])
        results[q] = entry
        pathlib.Path(args.out).write_text(json.dumps(results, indent=1))
    for group_samplers in samplers.values():
        for sampler in group_samplers:
            sampler.stop()
    return 0


def summarise(path):
    """Prints the median per query and mode of wall time, CPU seconds and peak memory."""
    data = json.loads(pathlib.Path(path).read_text())
    print("%3s %-6s %6s | %-24s | %-24s | %-14s | %s" % (
        "Q", "mode", "wall", "flight pods cpu s / cores / GB", "BE pods cpu s / cores / GB", "FE cpu s/GB", "MinIO cpu s"))
    for q, entry in sorted(data.items(), key=lambda kv: int(kv[0])):
        for mode in ("direct", "flight"):
            ok = [r for r in entry[mode] if not r["error"]]
            if not ok:
                print("%3s %-6s FAIL" % (q, mode))
                continue

            def med(group, key):
                values = [r[group][key] for r in ok if group in r]
                return statistics.median(values) if values else float("nan")
            print("%3s %-6s %6.1f | %7.0f %6.1f %9.1f | %7.0f %6.1f %9.1f | %6.0f %6.1f | %6.0f" % (
                q, mode, statistics.median(r["wall"] for r in ok),
                med("flight", "cpu_s"), med("flight", "cores"), med("flight", "peak_gb"),
                med("be", "cpu_s"), med("be", "cores"), med("be", "peak_gb"),
                med("fe", "cpu_s"), med("fe", "peak_gb"), med("minio", "cpu_s")))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--summary":
        summarise(sys.argv[2])
        raise SystemExit(0)
    raise SystemExit(main())
