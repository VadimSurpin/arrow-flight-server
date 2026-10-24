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
LOOP = r"""while :; do
u=$(grep usage_usec /sys/fs/cgroup/cpu.stat); i=$(grep ^inactive_file /sys/fs/cgroup/memory.stat)
set -- $(grep eth0 /proc/net/dev | tr ':' ' '); rx=$2; tx=${10}
io=$(awk '{r=0;w=0;for(i=2;i<=NF;i++){split($i,a,"=");if(a[1]=="rbytes")r=a[2];if(a[1]=="wbytes")w=a[2]}
 if(r>R)R=r;if(w>W)W=w}END{printf "%.0f %.0f\n",R,W}' /sys/fs/cgroup/io.stat)
c=$(cat /proc/net/tcp /proc/net/tcp6 | awk '$4=="01"{n=split($3,a,":");if(a[n]=="2328")r++;n=split($2,b,":");if(b[n]=="2328")l++}END{print r+0,l+0}')
echo "${u#usage_usec } $(cat /sys/fs/cgroup/memory.current) ${i#inactive_file } $rx $tx $io $c"
sleep 1; done"""
FIELDS = ["cpu", "mem", "rx", "tx", "disk_r", "disk_w", "conn_out", "conn_in"]


class PodSampler:
    """Streams one sample per second from a pod: wall time and the counters in FIELDS (mem is the working set)."""

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
            if len(parts) != 9:
                continue
            try:
                v = [int(p) for p in parts]
            except ValueError:
                continue
            sample = {"t": time.time(), "cpu": v[0] / 1e6, "mem": v[1] - v[2], "rx": v[3], "tx": v[4],
                      "disk_r": v[5], "disk_w": v[6], "conn_out": v[7], "conn_in": v[8]}
            with self._lock:
                self.samples.append(sample)

    def window(self, start, end):
        """Resources used between two wall-clock times, or None without samples around them.

        cpu_s, rx/tx and disk figures are counter differences; mem_gbs is the integral of the working set over
        time (GB x seconds) and mem_extra_gbs the same above the working set at the start; conn_* are the average and
        peak number of established connections to port 9000 (conn_out) and from clients to this pod (conn_in).
        """
        with self._lock:
            samples = list(self.samples)
        before = [s for s in samples if s["t"] <= start]
        after = [s for s in samples if s["t"] >= end]
        if not before or not after:
            return None
        first, last = before[-1], after[0]
        span = [first] + [s for s in samples if start < s["t"] < end] + [last]
        gbs = extra = 0.0
        for x, y in zip(span, span[1:]):
            dt = y["t"] - x["t"]
            gbs += (x["mem"] + y["mem"]) / 2 * dt / 2**30
            extra += max(0.0, (x["mem"] + y["mem"]) / 2 - first["mem"]) * dt / 2**30
        out = {"cpu_s": last["cpu"] - first["cpu"], "mem_gbs": gbs, "mem_extra_gbs": extra,
               "peak_gb": max(s["mem"] for s in span) / 2**30,
               "rx_gb": (last["rx"] - first["rx"]) / 2**30, "tx_gb": (last["tx"] - first["tx"]) / 2**30,
               "disk_r_gb": (last["disk_r"] - first["disk_r"]) / 2**30,
               "disk_w_gb": (last["disk_w"] - first["disk_w"]) / 2**30}
        for key in ("conn_out", "conn_in"):
            values = [s[key] for s in span]
            out[key + "_avg"] = sum(values) / len(values)
            out[key + "_peak"] = max(values)
        return out

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
                        total = {k: sum(w[k] for w in windows) for k in windows[0]
                                 if not k.startswith("conn_") and k != "peak_gb"}
                        total["peak_gb"] = sum(w["peak_gb"] for w in windows)
                        for k in ("conn_out_avg", "conn_in_avg"):
                            total[k] = sum(w[k] for w in windows)
                        for k in ("conn_out_peak", "conn_in_peak"):
                            total[k] = sum(w[k] for w in windows)
                        record[group] = {k: round(v, 2) for k, v in total.items()}
                        record[group]["cores"] = round(total["cpu_s"] / record["wall"], 2)
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
    """Prints the median per query and mode: time, CPU, memory over time and I/O of the compute and storage side."""
    data = json.loads(pathlib.Path(path).read_text())
    print("compute = flight pods + StarRocks BEs + FE; storage = MinIO. core-s = CPU seconds used, GBs = GB x seconds of "
          "working-set memory, net = GB received by compute, conn = established connections to MinIO (peak)")
    print("%3s %-6s %6s | %8s %8s | %7s %6s | %7s %7s %7s | %6s %6s | %6s %6s %5s" % (
        "Q", "mode", "wall", "compute", "storage", "compute", "extra", "net rx", "spill", "MinIO", "MinIO", "MinIO",
        "conn", "conn", "conn"))
    print("%3s %-6s %6s | %8s %8s | %7s %6s | %7s %7s %7s | %6s %6s | %6s %6s %5s" % (
        "", "", "s", "core-s", "core-s", "GBs", "GBs", "GB", "GB", "disk r", "net tx", "cores", "flight", "BE", "all"))
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
            compute = ("flight", "be", "fe")
            print("%3s %-6s %6.1f | %8.0f %8.0f | %7.0f %6.0f | %7.1f %7.1f %7.1f | %6.1f %6.1f | %6.0f %6.0f %5.0f" % (
                q, mode, med(lambda r: r["wall"]),
                med(lambda r: grp(r, compute, "cpu_s")), med(lambda r: grp(r, ("minio",), "cpu_s")),
                med(lambda r: grp(r, compute, "mem_gbs")), med(lambda r: grp(r, compute, "mem_extra_gbs")),
                med(lambda r: grp(r, ("flight", "be"), "rx_gb")),
                med(lambda r: grp(r, compute, "disk_w_gb")), med(lambda r: grp(r, ("minio",), "disk_r_gb")),
                med(lambda r: grp(r, ("minio",), "tx_gb")), med(lambda r: grp(r, ("minio",), "cores")),
                med(lambda r: grp(r, ("flight",), "conn_out_peak")), med(lambda r: grp(r, ("be",), "conn_out_peak")),
                med(lambda r: grp(r, ("minio",), "conn_in_peak"))))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--summary":
        summarise(sys.argv[2])
        raise SystemExit(0)
    raise SystemExit(main())
