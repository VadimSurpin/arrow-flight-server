#!/usr/bin/env python3
"""Bootstrap a suite: list suites, (re)generate query files, and build or run the data generation.

  bootstrap.py list
  bootstrap.py queries --suite tpcds|clickbench            # rewrites <suite>/queries/*.sql (needs network)
  bootstrap.py data --suite tpcds --sf 100 --layout sorted --out s3://learn            # prints the local command
  bootstrap.py data --suite tpcds --sf 100 --layout sorted --kubectl PATH [--node compute4]   # creates a Job in the cluster
  bootstrap.py data --suite synthetic --phase 2 ...                                       # the synthetic axis grid
"""
import argparse
import pathlib
import re
import shlex
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
DATASETS = ROOT / "benchmarks" / "datasets"
sys.path.insert(0, str(HERE))
import suites  # noqa: E402

CLICKBENCH_QUERIES = "https://raw.githubusercontent.com/ClickHouse/ClickBench/main/spark/queries.sql"


def cmd_list(_):
    for name, s in sorted(suites.SUITES.items()):
        print("%-11s %3d queries  default schema %-18s data: %s" % (name, len(suites.query_ids(name)), s["schema"], s["data"]))


def cmd_queries(args):
    out = HERE / args.suite / "queries"
    out.mkdir(parents=True, exist_ok=True)
    if args.suite == "tpcds":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL tpcds; LOAD tpcds")
        for n, q in con.execute("SELECT query_nr, query FROM tpcds_queries() ORDER BY 1").fetchall():
            # Spark treats "x" as a string literal, so quoted identifiers become backticks
            q = re.sub(r'"([^"]+)"', r"`\1`", q.strip().rstrip(";"))
            (out / ("q%02d.sql" % n)).write_text("-- TPC-DS query %d, default parameters (DuckDB tpcds extension), quoted identifiers rewritten for Spark\n%s\n" % (n, q))
    elif args.suite == "clickbench":
        import urllib.request
        text = urllib.request.urlopen(urllib.request.Request(CLICKBENCH_QUERIES, headers={"User-Agent": "curl/8.5"})).read().decode()
        for i, line in enumerate(text.strip().splitlines(), 1):
            (out / ("q%02d.sql" % i)).write_text("-- ClickBench query %d (Spark dialect, ClickHouse/ClickBench spark/queries.sql)\n%s\n" % (i, line.strip().rstrip(";")))
    else:
        raise SystemExit("%s queries are checked in by hand (tpch: standard text; synthetic and taxi: written for this repo)" % args.suite)
    print("wrote", len(list(out.glob("*.sql"))), "queries to", out)


def data_command(args):
    py = "python"
    if args.suite in ("tpch", "tpcds"):
        return "%s gen.py --suite %s --sf %s --layout %s --out %s --memory %s --spill %s" % (
            py, args.suite, args.sf, args.layout, args.out, args.memory, args.spill)
    if args.suite == "synthetic":
        return "%s sweep.py --phase 2 --out %s --run" % (py, args.out)
    if args.suite in ("clickbench", "taxi"):
        return "%s fetch.py --dataset %s --out %s" % (py, args.suite, args.out)
    raise SystemExit("no data command for " + args.suite)


def cmd_data(args):
    command = data_command(args)
    if not args.kubectl:
        print("run from %s:\n  %s" % (DATASETS, command))
        return
    # one Job from the template; the scripts reach the pod through the gen-datasets ConfigMap
    cm = subprocess.run([args.kubectl, "-n", args.namespace, "create", "configmap", "gen-datasets", "--dry-run=client", "-o", "yaml",
                         "--from-file=" + str(DATASETS / "gen.py"), "--from-file=" + str(DATASETS / "sweep.py"),
                         "--from-file=" + str(DATASETS / "fetch.py")], check=True, capture_output=True, text=True).stdout
    subprocess.run([args.kubectl, "-n", args.namespace, "apply", "-f", "-"], input=cm, text=True, check=True)
    job = (ROOT / "deploy" / "k8s-tpch" / "21-gen-datasets.yaml").read_text()
    job = re.sub(r"compute3", args.node, job)
    job = re.sub(r'\{name: ARGS, value: ".*"\}', '{name: ARGS, value: "%s"}' % command.split(" ", 2)[2], job)
    script = command.split(" ")[1]
    job = job.replace("python /src/gen.py $ARGS", "python /src/%s $ARGS" % script)
    subprocess.run([args.kubectl, "-n", args.namespace, "create", "-f", "-"], input=job, text=True, check=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    q = sub.add_parser("queries")
    q.add_argument("--suite", required=True)
    q.set_defaults(fn=cmd_queries)
    d = sub.add_parser("data")
    d.add_argument("--suite", required=True, choices=sorted(suites.SUITES))
    d.add_argument("--sf", default="100")
    d.add_argument("--layout", default="asis", choices=["asis", "sorted", "shuffled"])
    d.add_argument("--out", default="s3://learn")
    d.add_argument("--memory", default="5GB")
    d.add_argument("--spill", default="/scratch")
    d.add_argument("--kubectl")
    d.add_argument("--namespace", default="tpch-bench")
    d.add_argument("--node", default="compute4")
    d.set_defaults(fn=cmd_data)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
