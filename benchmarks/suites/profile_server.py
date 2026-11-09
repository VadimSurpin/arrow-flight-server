#!/usr/bin/env python3
"""profile_server.py OUT Q...: server-side view of StarRocks-over-Flight TPC-DS queries (tpcds_sf100_asis, cache off).
Scrapes arrowflight_stream_* and the query-duration sum from every Flight pod (port 9404) before and after each repetition and stores the deltas next to the
client wall time: streams (endpoints), rows read/sent, wire bytes, time waiting for DuckDB, time handing batches to Flight, back-pressure, first-batch time,
server execution seconds. One warmup repetition per query, then REPS measured ones."""
import json, os, re, subprocess, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sr_client, suites

REPS = 3
PODS = ["flight-0", "flight-1", "flight-2"]
KUBECTL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools", ".local", "bin", "kubectl")
FIELDS = {"streams": "arrowflight_stream_streams_total", "rows_read": "arrowflight_stream_rows_read_total",
          "rows_sent": "arrowflight_stream_rows_sent_total", "batches": "arrowflight_stream_batches_total",
          "wire_bytes": "arrowflight_stream_wire_bytes_total", "duckdb_s": "arrowflight_stream_duckdb_seconds_total",
          "send_s": "arrowflight_stream_send_seconds_total", "backpressure_s": "arrowflight_stream_backpressure_seconds_total",
          "first_batch_s": "arrowflight_stream_first_batch_seconds_total"}


def scrape(port):
    text = urllib.request.urlopen("http://127.0.0.1:%d/metrics" % port, timeout=10).read().decode()
    out = {k: float(re.search(r"^%s (\S+)" % m, text, re.M).group(1)) for k, m in FIELDS.items()}
    out["exec_s"] = sum(float(v) for v in re.findall(r"^arrowflight_parquet_query_duration_seconds_sum\{[^}]*\} (\S+)", text, re.M))
    return out


def total(ports):
    acc = {}
    for p in ports:
        for k, v in scrape(p).items():
            acc[k] = acc.get(k, 0.0) + v
    return acc


def main():
    out = open(sys.argv[1], "a")
    env = dict(os.environ, KUBECONFIG=os.environ.get("KUBECONFIG", "/home/vadim/cloudru-k8s/.kube/kubeconfig-surpin.yaml"))
    ports = [19404 + i for i in range(len(PODS))]
    fwd = [subprocess.Popen([KUBECTL, "-n", "tpch-bench", "port-forward", pod, "%d:9404" % port], env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL) for pod, port in zip(PODS, ports)]
    try:
        for _ in range(30):
            try:
                total(ports)
                break
            except Exception:
                time.sleep(1)
        sr = sr_client.StarRocks("127.0.0.1", 19030, "grpc://flight-0.flight:32010")
        for q in sys.argv[2:]:
            sql = suites.query_sql("tpcds", q, "starrocks")
            body = {"sql": sql, "schema": "tpcds_sf100_asis", "conf": {}, "mode": "flight", "max_rows": 10}
            sr.run(body, 300)  # warmup
            for rep in range(1, REPS + 1):
                before = total(ports)
                ans = sr.run(body, 300)
                time.sleep(2)
                after = total(ports)
                rec = {"query": q, "rep": rep, "wall": ans["wall"], "error": ans["error"]}
                rec.update({k: round(after[k] - before[k], 3) for k in after})
                out.write(json.dumps(rec) + "\n"); out.flush()
                print(rec, flush=True)
    finally:
        for p in fwd:
            p.terminate()


main()
