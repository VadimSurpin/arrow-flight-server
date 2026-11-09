#!/usr/bin/env python3
"""collect_features.py ENGINE OUT: one EXPLAIN per (suite, schema, query) through the Flight path, no query is executed.
Writes {"type":"plan","engine","suite","schema","query","plan": full text, **harness.features(plan)} to OUT, skipping cells already there.
Schemas: the three layouts of tpch/ssb/tpcds, clickbench, taxi, and the synthetic schemas of runs/syn_schemas.txt."""
import argparse, json, os, pathlib, sys
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness, suites


def units():
    for s in ("tpch", "ssb", "tpcds"):
        for l in ("asis", "sorted", "shuffled"):
            yield s, "%s_sf100_%s" % (s, l)
    yield "clickbench", "clickbench"
    yield "taxi", "taxi"
    for line in (HERE / "runs" / "syn_schemas.txt").read_text().split():
        yield "synthetic", line


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("engine", choices=["spark", "starrocks"])
    ap.add_argument("out")
    ap.add_argument("--url", default="http://127.0.0.1:18000")
    a = ap.parse_args()
    if a.engine == "starrocks":
        import sr_client
        sr = sr_client.StarRocks("127.0.0.1", 19030, "grpc://flight-0.flight:32010", token=os.environ.get("FLIGHT_TOKEN"))
        call = sr.run
    else:
        call = lambda body, t: harness.post(a.url, body, t)  # noqa: E731
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if out.exists():
        for l in out.read_text().splitlines():
            r = json.loads(l)
            done.add((r["schema"], r["query"]))
    for suite, schema in units():
        for q in suites.parse_ids(suite, "all"):
            if (schema, q) in done:
                continue
            try:
                ans = call({"sql": suites.query_sql(suite, q, a.engine), "schema": schema, "mode": "flight",
                            "conf": suites.SUITES[suite]["conf"], "explain": True}, 300)
            except Exception as e:
                ans = {"error": "client: " + str(e)[:200]}
            rec = {"type": "plan", "engine": a.engine, "suite": suite, "schema": schema, "query": q, "error": ans["error"]}
            if not ans["error"]:
                text = ans["rows"][0][0]
                rec.update(harness.features(text), plan=text)
            with out.open("a") as f:
                f.write(json.dumps(rec) + "\n")
            print(schema, q, ans["error"] or "ok", flush=True)


main()
