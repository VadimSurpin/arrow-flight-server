#!/usr/bin/env python3
"""collect_config_plans.py ENGINE OUT: the plan each configuration of the sweep produces, for the labelled cells (default >= 2 s) of runs/features/labels-<engine>.jsonl.
One EXPLAIN per (schema, query, config), nothing executed. Record: stable plan hash (ids, plan_id and codegen numbers removed), operator counts. Resumable.
Static plans do not show runtime effects (AQE, runtime filters, bloom, complete aggregation), so 'same plan' is evidence only for plan-visible knobs."""
import hashlib, json, os, pathlib, re, sys
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import build_features, harness, suites

norm = lambda t: hashlib.sha256(re.sub(r"#\d+L?|plan_id=\d+|\[\d+\]|\(\d+\)|codegen id : \d+|EXCHANGE ID: \d+|\bslot \d+|\b\d+: ", "", t).encode()).hexdigest()[:12]  # noqa: E731


def main():
    engine, out = sys.argv[1], pathlib.Path(sys.argv[2])
    cfgs = json.load(open(HERE / "configs" / ("phase1.json" if engine == "spark" else "starrocks.json")))
    cfgs = {k: v["flight"] for k, v in cfgs.items() if not k.startswith("_")}
    cells = sorted({(r["schema"], r["query"]) for r in map(json.loads, open(HERE / "runs" / "features" / ("labels-%s.jsonl" % engine)))})
    suite_of = {r["schema"]: r["suite"] for r in map(json.loads, open(HERE / "runs" / "features" / ("features-%s.jsonl" % engine)))}
    done = {(r["schema"], r["query"], r["config"]) for r in map(json.loads, out.read_text().splitlines())} if out.exists() else set()
    fn = build_features.spark if engine == "spark" else build_features.starrocks
    if engine == "starrocks":
        import sr_client
        sr = sr_client.StarRocks("127.0.0.1", 19030, "grpc://flight-0.flight:32010", token=os.environ.get("FLIGHT_TOKEN"))
    order = [(c, s, q) for c in cfgs for (s, q) in cells] if engine == "starrocks" else [(c, s, q) for (s, q) in cells for c in cfgs]  # SR: the catalog is rebuilt per config
    for c, schema, q in order:
        if (schema, q, c) in done:
            continue
        suite = suite_of[schema]
        conf = dict(suites.SUITES[suite]["conf"], **cfgs[c])
        sql = suites.query_sql(suite, q, engine)
        try:
            body = {"sql": sql, "schema": schema, "mode": "flight", "conf": conf, "explain": True}
            if engine == "starrocks":
                ans = sr.run(body, 300)
            else:
                ans = harness.post("http://127.0.0.1:18000", body, 300)
        except Exception as e:
            ans = {"error": "client: " + str(e)[:200], "rows": None}
        rec = {"engine": engine, "schema": schema, "query": q, "config": c, "error": ans["error"]}
        if not ans["error"]:
            text = ans["rows"][0][0]
            rec.update(hash=norm(text), f=fn(text))
        with out.open("a") as f:
            f.write(json.dumps(rec) + "\n")
        print(schema, q, c, ans["error"] or "ok", flush=True)


if __name__ == "__main__":
    main()
