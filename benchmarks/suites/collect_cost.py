#!/usr/bin/env python3
"""collect_cost.py ENGINE OUT: the optimizer's own size estimates for every (schema, query): Spark `EXPLAIN COST` (logical plan with Statistics per node, scans
carry the post-pushdown sizes the Flight server reports) and StarRocks `EXPLAIN COSTS` (cardinality per node). Nothing is executed. Resumable."""
import argparse, json, os, pathlib, sys
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness, suites
from collect_features import units  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("engine", choices=["spark", "starrocks"])
    ap.add_argument("out")
    a = ap.parse_args()
    if a.engine == "starrocks":
        import sr_client
        sr = sr_client.StarRocks("127.0.0.1", 19030, "grpc://flight-0.flight:32010", token=os.environ.get("FLIGHT_TOKEN"))
    out = pathlib.Path(a.out)
    done = {(json.loads(l)["schema"], json.loads(l)["query"]) for l in out.read_text().splitlines()} if out.exists() else set()
    for suite, schema in units():
        for q in suites.parse_ids(suite, "all"):
            if (schema, q) in done:
                continue
            sql = suites.query_sql(suite, q, a.engine)
            try:
                if a.engine == "starrocks":
                    ans = sr.run({"sql": "EXPLAIN COSTS " + sql, "schema": schema, "conf": {}, "mode": "flight", "max_rows": 1000000}, 300)
                    text = "\n".join(r[0] for r in ans["rows"] or [])
                else:
                    ans = harness.post("http://127.0.0.1:18000", {"sql": "EXPLAIN COST " + sql, "schema": schema, "mode": "flight",
                                                                  "conf": suites.SUITES[suite]["conf"], "max_rows": 10}, 300)
                    text = (ans["rows"] or [[""]])[0][0]
            except Exception as e:
                ans, text = {"error": "client: " + str(e)[:200]}, ""
            rec = {"engine": a.engine, "suite": suite, "schema": schema, "query": q, "error": ans["error"], "cost": text}
            with out.open("a") as f:
                f.write(json.dumps(rec) + "\n")
            print(schema, q, ans["error"] or "ok", flush=True)


if __name__ == "__main__":
    main()
