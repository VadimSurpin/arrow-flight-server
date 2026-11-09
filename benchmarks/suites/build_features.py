#!/usr/bin/env python3
"""build_features.py: numeric plan features per (engine, schema, query) from runs/features/<engine>-plans.jsonl -> runs/features/features-<engine>.jsonl.
Counts come from the plan under the default configuration."""
import json, pathlib, re

HERE = pathlib.Path(__file__).resolve().parent / "runs" / "features"


def spark(t):
    f = {"scans": len(re.findall(r"^\(\d+\) \w*Scan", t, re.M)),
         "joins": len(re.findall(r"^\(\d+\) \w*Join", t, re.M)),
         "bcast_joins": len(re.findall(r"^\(\d+\) BroadcastHashJoin", t, re.M)),
         "smj": len(re.findall(r"^\(\d+\) SortMergeJoin", t, re.M)),
         "shj": len(re.findall(r"^\(\d+\) ShuffledHashJoin", t, re.M)),
         "nested_loop": len(re.findall(r"^\(\d+\) BroadcastNestedLoopJoin", t, re.M)),
         "aggs": len(re.findall(r"^\(\d+\) \w*Aggregate", t, re.M)),
         "exchanges": len(re.findall(r"^\(\d+\) Exchange", t, re.M)),
         "bcast_exchanges": len(re.findall(r"^\(\d+\) BroadcastExchange", t, re.M)),
         "sorts": len(re.findall(r"^\(\d+\) Sort\b", t, re.M)),
         "windows": len(re.findall(r"^\(\d+\) Window", t, re.M)),
         "filters": len(re.findall(r"^\(\d+\) Filter", t, re.M)),
         "limits": len(re.findall(r"^\(\d+\) (TakeOrdered|\w*Limit)", t, re.M)),
         "distinct_aggs": len(re.findall(r"count\(distinct|\(distinct ", t, re.I)),
         "left_outer": len(re.findall(r"Join type: (LeftOuter|RightOuter|FullOuter)", t)),
         "semi_anti": len(re.findall(r"Join type: (LeftSemi|LeftAnti)", t)),
         "unions": len(re.findall(r"^\(\d+\) Union", t, re.M)),
         "scan_cols": sum(int(n) for n in re.findall(r"^\(\d+\) \w*Scan[^\n]*\nOutput \[(\d+)\]", t, re.M)),
         "pushed_predicates": len(re.findall(r"\bwhere\b", t))}
    return f


def starrocks(t):
    return {"scans": len(re.findall(r"\d+:SCAN", t)),
            "joins": len(re.findall(r"\d+:(HASH|NESTLOOP|MERGE) JOIN", t)),
            "bcast_joins": len(re.findall(r"join op: [A-Z ]+\(BROADCAST\)", t)),
            "partitioned_joins": len(re.findall(r"join op: [A-Z ]+\((PARTITIONED|BUCKET_SHUFFLE|COLOCATE)", t)),
            "aggs": len(re.findall(r"\d+:AGGREGATE", t)),
            "merge_aggs": len(re.findall(r"merge finalize|merge serialize", t)),
            "distinct_aggs": len(re.findall(r"multi_distinct|distinct", t, re.I)),
            "exchanges": len(re.findall(r"\d+:EXCHANGE|\d+:MERGING-EXCHANGE", t)),
            "sorts": len(re.findall(r"\d+:SORT", t)),
            "topn": len(re.findall(r"\d+:TOP-N", t)),
            "windows": len(re.findall(r"\d+:ANALYTIC", t)),
            "left_outer": len(re.findall(r"(LEFT|RIGHT|FULL) OUTER JOIN", t)),
            "semi_anti": len(re.findall(r"(LEFT|RIGHT) (SEMI|ANTI) JOIN", t)),
            "unions": len(re.findall(r"\d+:UNION", t)),
            "fragments": len(re.findall(r"PLAN FRAGMENT", t))}


def main():
    for engine, fn in (("spark", spark), ("starrocks", starrocks)):
        out = []
        for l in (HERE / ("%s-plans.jsonl" % engine)).read_text().splitlines():
            r = json.loads(l)
            if r["error"]:
                continue
            f = fn(r["plan"])
            f["plan_chars"] = r["plan_chars"]
            out.append({"engine": engine, "suite": r["suite"], "schema": r["schema"], "query": r["query"], "plan_hash": r["plan_hash"], "f": f})
        (HERE / ("features-%s.jsonl" % engine)).write_text("".join(json.dumps(o) + "\n" for o in out))
        print(engine, len(out), "plans;", len({o["plan_hash"] for o in out}), "distinct plan hashes")



if __name__ == "__main__":
    main()
