"""Registry of benchmark suites: where the queries are, which schema (S3 directory) they run on by default, and the
Spark settings the suite needs. The data side (bootstrap) lives in bootstrap.py, the measuring side in harness.py."""
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent

SUITES = {
    "tpch": {"schema": "tpch_sf100_asis", "conf": {}, "data": "gen"},
    "tpcds": {"schema": "tpcds_sf100_asis", "conf": {}, "data": "gen"},
    # the Parquet files store text as binary
    "clickbench": {"schema": "clickbench", "conf": {"spark.sql.parquet.binaryAsString": "true"}, "data": "fetch"},
    # column types drift between years
    "taxi": {"schema": "taxi", "conf": {"spark.sql.parquet.mergeSchema": "true"}, "data": "fetch"},
    "synthetic": {"schema": None, "conf": {}, "data": "gen"},  # the schema is one of the syn_* datasets
}


def query_ids(suite):
    return sorted(p.stem for p in (HERE / suite / "queries").glob("*.sql"))


def parse_ids(suite, spec):
    """'1-5,9' -> q01..q05, q09 for numbered suites; names pass through (synthetic: 'topn,join_selective_dim'); '' or 'all' -> all."""
    known = query_ids(suite)
    if spec in ("", "all"):
        return known
    out = []
    for part in spec.split(","):
        m = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
        if m:
            lo, hi = int(m.group(1)), int(m.group(2) or m.group(1))
            out += ["q%02d" % i for i in range(lo, hi + 1)]
        else:
            out.append(part)
    missing = [q for q in out if q not in known]
    if missing:
        raise SystemExit("unknown queries for %s: %s" % (suite, ", ".join(missing)))
    return out


def query_sql(suite, qid):
    """The query text without the leading comment lines."""
    text = (HERE / suite / "queries" / (qid + ".sql")).read_text()
    return "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--")).strip().rstrip(";")
