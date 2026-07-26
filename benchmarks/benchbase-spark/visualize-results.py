#!/usr/bin/env python3
import argparse
import csv
import html
import json
import xml.etree.ElementTree as ET
from pathlib import Path


LAT_AVG = "Average Latency (millisecond)"
LAT_P95 = "95th Percentile Latency (millisecond)"
LAT_MAX = "Maximum Latency (millisecond)"
THROUGHPUT = "Throughput (requests/second)"
TIME = "Time (seconds)"


STYLE = """
  <style>
    body { margin: 0; font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; background: #f8fafc; color: #111827; }
    main { max-width: 1180px; margin: 0 auto; padding: 28px; }
    h1 { margin: 0 0 8px; font-size: 28px; }
    h2 { margin: 0 0 14px; font-size: 19px; }
    h3 { margin: 16px 0 8px; font-size: 15px; }
    section { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 18px; margin: 16px 0; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; margin-top: 18px; }
    .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 14px; }
    .label { color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 25px; font-weight: 700; margin-top: 4px; }
    .subtle { color: #6b7280; margin: 4px 0 0; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: 8px 10px; text-align: right; vertical-align: top; }
    th:first-child, td:first-child { text-align: left; }
    .kv th { color: #6b7280; width: 220px; font-weight: 600; }
    .kv td, .kv th { text-align: left; }
    tr.muted { color: #9ca3af; }
    tr.active { background: #eff6ff; }
    pre { overflow: auto; background: #0f172a; color: #e5e7eb; padding: 14px; border-radius: 8px; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    svg { width: 100%; height: auto; }
    svg text { fill: #4b5563; font-size: 12px; }
    .legend { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 8px; color: #4b5563; }
    .legend i { display: inline-block; width: 11px; height: 11px; border-radius: 2px; margin-right: 6px; vertical-align: -1px; }
    .grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
    .ok { color: #15803d; font-weight: 700; }
    .bad { color: #b91c1c; font-weight: 700; }
    .warn { color: #a16207; font-weight: 700; }
    a { color: #2563eb; }
  </style>
"""


def parse_args():
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Build HTML report for BenchBase CSV results.")
    parser.add_argument("--results", type=Path, default=script_dir / "results")
    parser.add_argument("--base", help="Result prefix, for example tpch_2026-07-09_07-49-48.")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--compare", action="store_true", help="Build a flight-vs-direct comparison report.")
    return parser.parse_args()


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as file:
        # Beeline can emit NUL padding that Python's csv parser rejects.
        sanitized_lines = (line.replace("\0", "") for line in file)
        return list(csv.DictReader(sanitized_lines))


def read_json(path):
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fmt(value):
    value = number(value)
    if abs(value) >= 100:
        return f"{value:.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.3f}"


def human_bytes(value):
    value = number(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if abs(value) < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} TiB"


def latest_summary(results_dir):
    summaries = sorted(results_dir.rglob("*.summary.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not summaries:
        raise SystemExit(f"No *.summary.json files in {results_dir}")
    return summaries[0]


def find_summary(results_dir, base=None):
    if base:
        direct = results_dir / f"{base}.summary.json"
        if direct.exists():
            return direct
        matches = sorted(results_dir.rglob(f"{base}.summary.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not matches:
            raise SystemExit(f"No {base}.summary.json under {results_dir}")
        return matches[0]
    return latest_summary(results_dir)


def metadata_for(run_dir):
    for path in [run_dir / "benchmark-metadata.json", run_dir.parent / "benchmark-metadata.json"]:
        if path.exists():
            return read_json(path)
    return {}


def metric_from_last(rows, column):
    if not rows:
        return "n/a"
    return fmt(rows[-1].get(column))


def summary_latency_value(summary, name):
    latency = summary.get("Latency Distribution", {})
    return number(latency.get(f"{name} Latency (microseconds)")) / 1000


def summary_latency_ms(summary, name):
    return fmt(summary_latency_value(summary, name))


def summary_metric(summary, name):
    if not summary:
        return "n/a"
    return fmt(summary.get(name))


def read_config(path):
    if not path.exists():
        return {}
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return {}

    def text(expr, default="n/a"):
        value = root.findtext(expr)
        return value.strip() if value and value.strip() else default

    active = set()
    for work in root.findall("./works/work"):
        weights = work.findtext("weights", "")
        for idx, raw in enumerate(weights.split(","), start=1):
            try:
                if float(raw) > 0:
                    active.add(f"Q{idx}")
            except ValueError:
                pass

    return {
        "scale": text("./scalefactor"),
        "terminals": text("./terminals"),
        "time": text("./works/work/time", "serial"),
        "warmup": text("./works/work/warmup", "0"),
        "rate": text("./works/work/rate"),
        "queries": ", ".join(
            sorted(active, key=lambda query: int(query[1:]))
        ) if active else "all configured",
    }


def configured_query_ids(config):
    query_ids = set()
    for token in config.get("queries", "").split(","):
        token = token.strip().lower().removeprefix("q")
        if token.isdigit():
            query_ids.add(int(token))
    return query_ids or None


def query_number(path):
    suffix = path.name.split(".results.Q", 1)[-1].split(".csv", 1)[0]
    try:
        return int(suffix)
    except ValueError:
        return 999


def per_query_rows(results_dir, base):
    rows = []
    for path in sorted(results_dir.glob(f"{base}.results.Q*.csv"), key=query_number):
        data = read_csv(path)
        if not data:
            continue
        last = data[-1]
        rows.append(
            {
                "query": f"Q{query_number(path)}",
                "throughput": last.get(THROUGHPUT, "0"),
                "avg": last.get(LAT_AVG, "0"),
                "p95": last.get(LAT_P95, "0"),
                "max": last.get(LAT_MAX, "0"),
            }
        )
    return rows


def per_query_latency_rows(results_dir, base):
    grouped = {}
    for row in read_csv(results_dir / f"{base}.raw.csv"):
        name = str(row.get("Transaction Name", "")).strip().upper()
        if not name.startswith("Q") or not name[1:].isdigit():
            continue
        try:
            latency_ms = float(row.get("Latency (microseconds)", "")) / 1000
        except (TypeError, ValueError):
            continue
        grouped.setdefault(int(name[1:]), []).append(latency_ms)

    return [
        {
            "query": f"Q{query_id}",
            "avg": sum(values) / len(values),
            "samples": len(values),
        }
        for query_id, values in sorted(grouped.items())
    ]


def load_run(run_dir, base=None):
    summary_path = find_summary(run_dir, base)
    base = summary_path.name.removesuffix(".summary.json")
    return {
        "dir": summary_path.parent,
        "base": base,
        "rows": read_csv(summary_path.parent / f"{base}.results.csv"),
        "summary": read_json(summary_path),
        "config": read_config(summary_path.parent / f"{base}.config.xml"),
        "query_rows": per_query_rows(summary_path.parent, base),
        "query_latency_rows": per_query_latency_rows(summary_path.parent, base),
        "report": summary_path.parent / f"{base}.report.html",
    }


def rows_equal(expected_rows, actual_rows):
    def normalize(rows):
        return [
            {str(k).strip().lower(): str(v).strip() for k, v in row.items()}
            for row in rows
        ]

    return normalize(expected_rows) == normalize(actual_rows)


def table_from_rows(rows, max_rows=20):
    if not rows:
        return "<p class=\"subtle\">No rows captured.</p>"
    columns = list(rows[0].keys())
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body = []
    for row in rows[:max_rows]:
        body.append("".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns))
    hidden = ""
    if len(rows) > max_rows:
        hidden = f"<p class=\"subtle\">Showing {max_rows} of {len(rows)} row(s).</p>"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(f'<tr>{row}</tr>' for row in body)}</tbody></table>{hidden}"


def run_context(summary, config, rows, mode=None):
    elapsed = number(summary.get("Elapsed Time (nanoseconds)")) / 1_000_000_000
    items = [
        ("Path", mode or "n/a"),
        ("Benchmark", summary.get("Benchmark Type", "n/a")),
        ("Scale factor", config.get("scale", summary.get("scalefactor", "n/a"))),
        ("Queries", config.get("queries", "n/a")),
        ("BenchBase workers", config.get("terminals", summary.get("terminals", "n/a"))),
        ("Duration", f"{fmt(elapsed)} s" if elapsed else config.get("time", "n/a")),
        ("Measured requests", str(summary.get("Measured Requests", "n/a"))),
        ("Last window", f"{rows[-1].get(TIME, 'n/a')} s" if rows else "n/a"),
    ]
    body = "".join(f"<tr><th>{html.escape(label)}</th><td>{html.escape(str(value))}</td></tr>" for label, value in items)
    return f"<section><h2>Run Context</h2><table class=\"kv\"><tbody>{body}</tbody></table></section>"


def svg_line_chart(rows, columns, title, unit):
    series = []
    colors = ["#2563eb", "#dc2626", "#16a34a"]
    for idx, column in enumerate(columns):
        series.append({"label": column, "rows": rows, "column": column, "color": colors[idx % len(colors)]})
    return svg_compare_chart(series, title, unit)


def svg_compare_chart(series, title, unit):
    width = 920
    height = 320
    pad_left = 62
    pad_right = 24
    pad_top = 44
    pad_bottom = 46
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom

    points_by_label = {}
    values = []
    for item in series:
        points = []
        for index, row in enumerate(item["rows"]):
            x_value = number(row.get(TIME), index)
            y_value = number(row.get(item["column"]))
            points.append((x_value, y_value))
            values.append(y_value)
        points_by_label[item["label"]] = points

    if not values:
        return f"<section><h2>{html.escape(title)}</h2><p>No data.</p></section>"

    all_points = [point for points in points_by_label.values() for point in points]
    min_x = min(point[0] for point in all_points)
    max_x = max(point[0] for point in all_points)
    min_y = 0
    max_y = max(values) or 1

    def sx(value):
        if max_x == min_x:
            return pad_left + plot_w / 2
        return pad_left + ((value - min_x) / (max_x - min_x)) * plot_w

    def sy(value):
        return pad_top + plot_h - ((value - min_y) / (max_y - min_y)) * plot_h

    lines = []
    legends = []
    for item in series:
        color = item["color"]
        points = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in points_by_label[item["label"]])
        lines.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>')
        legends.append(f'<span><i style="background:{color}"></i>{html.escape(item["label"])} ({unit})</span>')

    y_labels = []
    for step in range(5):
        value = min_y + (max_y - min_y) * step / 4
        y = sy(value)
        y_labels.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" y2="{y:.1f}" stroke="#e5e7eb"/>'
            f'<text x="{pad_left-10}" y="{y+4:.1f}" text-anchor="end">{fmt(value)}</text>'
        )

    x_labels = []
    for step in range(4):
        value = min_x + (max_x - min_x) * step / 3
        x = sx(value)
        x_labels.append(
            f'<line x1="{x:.1f}" y1="{pad_top}" x2="{x:.1f}" y2="{height-pad_bottom}" stroke="#f3f4f6"/>'
            f'<text x="{x:.1f}" y="{height-pad_bottom+20}" text-anchor="middle">{fmt(value)}</text>'
        )

    return f"""
<section>
  <h2>{html.escape(title)}</h2>
  <div class="legend">{''.join(legends)}</div>
  <svg viewBox="0 0 {width} {height}" role="img">
    <rect x="0" y="0" width="{width}" height="{height}" fill="#fff"/>
    {''.join(y_labels)}
    {''.join(x_labels)}
    <line x1="{pad_left}" y1="{height-pad_bottom}" x2="{width-pad_right}" y2="{height-pad_bottom}" stroke="#9ca3af"/>
    <line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{height-pad_bottom}" stroke="#9ca3af"/>
    {''.join(lines)}
    <text x="{width/2:.1f}" y="{height-10}" text-anchor="middle">time, seconds</text>
  </svg>
</section>
"""


def svg_query_latency_chart(flight_rows, direct_rows, expected_query_ids=None):
    flight = {int(row["query"][1:]): row for row in flight_rows}
    direct = {int(row["query"][1:]): row for row in direct_rows}
    query_ids = sorted(expected_query_ids or (set(flight) | set(direct)))
    if not query_ids:
        return """
<section>
  <h2>TPC-H Per-Query Average Query Execution Time</h2>
  <p>No measured per-query samples.</p>
</section>
"""

    width = 1120
    height = 430
    pad_left = 72
    pad_right = 24
    pad_top = 46
    pad_bottom = 62
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    group_w = plot_w / len(query_ids)
    bar_w = min(17, group_w * 0.34)
    values = [
        number(row.get("avg"))
        for row in list(flight.values()) + list(direct.values())
    ]
    max_y = max(values) if values else 1
    max_y = max_y or 1

    def sy(value):
        return pad_top + plot_h - (value / max_y) * plot_h

    y_labels = []
    for step in range(5):
        value = max_y * step / 4
        y = sy(value)
        y_labels.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" '
            f'y2="{y:.1f}" stroke="#e5e7eb"/>'
            f'<text x="{pad_left-10}" y="{y+4:.1f}" '
            f'text-anchor="end">{fmt(value)}</text>'
        )

    bars = []
    labels = []
    for index, query_id in enumerate(query_ids):
        center = pad_left + group_w * (index + 0.5)
        labels.append(
            f'<text x="{center:.1f}" y="{height-pad_bottom+22}" '
            f'text-anchor="middle">q{query_id:02d}</text>'
        )
        for offset, rows, color, label in [
            (-bar_w, flight, "#2563eb", "Flight"),
            (0, direct, "#f97316", "Direct"),
        ]:
            row = rows.get(query_id)
            if row is None:
                continue
            value = number(row.get("avg"))
            y = sy(value)
            bar_height = pad_top + plot_h - y
            x = center + offset
            tooltip = (
                f"{label} Q{query_id}: {fmt(value)} ms, "
                f"{int(number(row.get('samples')))} samples"
            )
            bars.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
                f'height="{bar_height:.1f}" fill="{color}">'
                f'<title>{html.escape(tooltip)}</title></rect>'
            )

    return f"""
<section>
  <h2>TPC-H Q1-Q22 Average Query Execution Time: Flight vs Direct</h2>
  <p class="subtle">Average execution time of measured BenchBase samples from
  each query type. <strong>Lower is faster.</strong></p>
  <div class="legend">
    <span><i style="background:#2563eb"></i>Flight (ms)</span>
    <span><i style="background:#f97316"></i>Direct (ms)</span>
  </div>
  <svg viewBox="0 0 {width} {height}" role="img"
       aria-label="TPC-H per-query average execution time comparison">
    <rect x="0" y="0" width="{width}" height="{height}" fill="#fff"/>
    {''.join(y_labels)}
    <line x1="{pad_left}" y1="{height-pad_bottom}" x2="{width-pad_right}"
          y2="{height-pad_bottom}" stroke="#9ca3af"/>
    <line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}"
          y2="{height-pad_bottom}" stroke="#9ca3af"/>
    {''.join(bars)}
    {''.join(labels)}
    <text x="18" y="{height/2:.1f}" text-anchor="middle"
          transform="rotate(-90 18 {height/2:.1f})">average query execution time, ms</text>
  </svg>
</section>
"""


def query_table(rows):
    if not rows:
        return "<section><h2>Per Query</h2><p>No per-query CSV files.</p></section>"

    body = []
    for row in rows:
        active = number(row["throughput"]) > 0 or number(row["avg"]) > 0
        cls = "active" if active else "muted"
        body.append(
            "<tr class=\"%s\"><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (cls, html.escape(row["query"]), fmt(row["throughput"]), fmt(row["avg"]), fmt(row["p95"]), fmt(row["max"]))
        )

    return f"""
<section>
  <h2>Per Query</h2>
  <table>
    <thead><tr><th>Query</th><th>Throughput req/s</th><th>Avg ms</th><th>P95 ms</th><th>Max ms</th></tr></thead>
    <tbody>{''.join(body)}</tbody>
  </table>
</section>
"""


def settings_section(metadata):
    if not metadata:
        return ""

    items = [
        ("Dataset", metadata.get("dataset", "n/a")),
        ("Schema", metadata.get("schema", "n/a")),
        ("Scale factor", metadata.get("scale_factor", "n/a")),
        ("Storage", metadata.get("storage", "n/a")),
        ("HDFS data", metadata.get("hdfs_data_dir", "n/a")),
        ("HDFS replication", metadata.get("hdfs_replication", "n/a")),
        ("HDFS block size", human_bytes(metadata.get("hdfs_block_size_bytes", 0))),
        ("Shared Direct/Flight dataset", metadata.get("shared_parquet_dataset", "n/a")),
        ("Flight nodes", metadata.get("cluster_nodes", "n/a")),
        ("Flight hosts", metadata.get("flight_hosts", "n/a")),
        ("Flight source", f"{metadata.get('flight_source_host', 'n/a')}:{metadata.get('flight_source_port', 'n/a')}"),
        ("Direct parquet partitions", metadata.get("direct_parquet_partitions", "n/a")),
    ]
    kv = "".join(f"<tr><th>{html.escape(str(k))}</th><td>{html.escape(str(v))}</td></tr>" for k, v in items)

    flight_rows = []
    for server in metadata.get("flight_data", []):
        flight_rows.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td><code>%s</code></td></tr>"
            % (
                html.escape(str(server.get("server_index", "n/a"))),
                html.escape(str(server.get("files", "0"))),
                human_bytes(server.get("bytes", 0)),
                html.escape(str(server.get("schema_path", ""))),
            )
        )

    direct = metadata.get("direct_data", {})
    direct_rows = (
        "<tr><td>direct</td><td>%s</td><td>%s</td><td><code>%s</code></td></tr>"
        % (
            html.escape(str(direct.get("files", "0"))),
            human_bytes(direct.get("bytes", 0)),
            html.escape(str(direct.get("schema_path", ""))),
        )
    )

    file_rows = []
    for server in metadata.get("flight_data", []):
        for table in server.get("tables", []):
            for file in table.get("file_details", []):
                file_rows.append(
                    "<tr><td>flight-%s</td><td>%s</td><td><code>%s</code></td><td>%s</td></tr>"
                    % (
                        html.escape(str(server.get("server_index", ""))),
                        html.escape(str(table.get("table", ""))),
                        html.escape(str(file.get("relative_path", ""))),
                        human_bytes(file.get("bytes", 0)),
                    )
                )
    for table in direct.get("tables", []):
        for file in table.get("file_details", []):
            file_rows.append(
                "<tr><td>direct</td><td>%s</td><td><code>%s</code></td><td>%s</td></tr>"
                % (
                    html.escape(str(table.get("table", ""))),
                    html.escape(str(file.get("relative_path", ""))),
                    human_bytes(file.get("bytes", 0)),
                )
            )

    file_table = ""
    if file_rows:
        file_table = f"""
  <h3>Parquet Files</h3>
  <table>
    <thead><tr><th>Path</th><th>Table</th><th>File</th><th>Size</th></tr></thead>
    <tbody>{''.join(file_rows)}</tbody>
  </table>
"""

    return f"""
<section>
  <h2>Benchmark Data And Flight Settings</h2>
  <table class="kv"><tbody>{kv}</tbody></table>
  <h3>Storage Summary</h3>
  <table>
    <thead><tr><th>Path</th><th>Parquet files</th><th>Total size</th><th>Schema path</th></tr></thead>
    <tbody>{''.join(flight_rows)}{direct_rows}</tbody>
  </table>
  {file_table}
</section>
"""


def query_reference_section(run_dir, metadata, title="Query Results", active_query_ids=None):
    references = metadata.get("reference_queries", []) if metadata else []
    if active_query_ids:
        references = [
            query for query in references
            if int(query.get("query_id", 0)) in active_query_ids
        ]
    if not references:
        return ""

    blocks = []
    for query in references:
        query_id = int(query.get("query_id", 0))
        actual_path = run_dir / f"query-q{query_id}.actual.csv"
        actual_rows = read_csv(actual_path)
        expected_rows = query.get("expected_rows", [])
        if actual_path.exists():
            status = "<span class=\"ok\">MATCH</span>" if rows_equal(expected_rows, actual_rows) else "<span class=\"bad\">DIFF</span>"
        else:
            status = "<span class=\"warn\">not captured</span>"
        blocks.append(
            f"""
  <h3>{html.escape(query.get('name', f'Q{query_id}'))}: {status}</h3>
  <p class="subtle">Expected answer is computed by DuckDB on the generated TPC-H data. Actual answer is captured through Spark Thrift for this run path.</p>
  <pre>{html.escape(query.get('sql', ''))}</pre>
  <div class="grid2">
    <div><h3>Expected</h3>{table_from_rows(expected_rows)}</div>
    <div><h3>Actual</h3>{table_from_rows(actual_rows)}</div>
  </div>
"""
        )

    return f"<section><h2>{html.escape(title)}</h2>{''.join(blocks)}</section>"


def summary_block(summary):
    if not summary:
        return ""
    pretty = html.escape(json.dumps(summary, indent=2, ensure_ascii=False))
    return f"<section><h2>Summary JSON</h2><pre>{pretty}</pre></section>"


def execution_path_section(results_dir):
    machine_result = read_json(results_dir / "benchmark-result.json")
    if not machine_result:
        return ""

    rows = []
    for observation in machine_result.get("observations", []):
        for engine in observation.get("engines", []):
            paths = engine.get("execution_paths", {})
            nodes = paths.get("nodes", [])
            node_text = ", ".join(
                f"{node.get('node', 'unknown')}: "
                + "/".join(node.get("paths", {}).keys())
                for node in nodes
            ) or "n/a"
            evidence = (
                '<span class="ok">yes</span>'
                if paths.get("pushdown_evidence")
                else '<span class="warn">no</span>'
            )
            classification = paths.get("classification", "unknown")
            uniform = paths.get("uniform_path")
            if uniform:
                classification += f" ({uniform})"
            rows.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    observation.get("observation_index", ""),
                    html.escape(str(engine.get("id", ""))),
                    html.escape(str(classification)),
                    evidence,
                    html.escape(node_text),
                )
            )
    return f"""
<section>
  <h2>Runtime Execution Paths</h2>
  <p class="subtle">Paths are emitted by the server branch that executed each
  query. Fallback and unknown events are not pushdown evidence.</p>
  <table>
    <thead><tr><th>Observation</th><th>Engine</th><th>Classification</th><th>Pushdown evidence</th><th>Per-node paths</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</section>
"""


def cards(run):
    summary = run["summary"]
    rows = run["rows"]
    return f"""
  <div class="cards">
    <div class="card"><div class="label">Overall Throughput</div><div class="value">{summary_metric(summary, THROUGHPUT)} req/s</div><p class="subtle">last window: {metric_from_last(rows, THROUGHPUT)} req/s</p></div>
    <div class="card"><div class="label">Overall Avg Latency</div><div class="value">{summary_latency_ms(summary, "Average")} ms</div><p class="subtle">last window: {metric_from_last(rows, LAT_AVG)} ms</p></div>
    <div class="card"><div class="label">Overall P95 Latency</div><div class="value">{summary_latency_ms(summary, "95th Percentile")} ms</div><p class="subtle">last window: {metric_from_last(rows, LAT_P95)} ms</p></div>
    <div class="card"><div class="label">Measured Requests</div><div class="value">{html.escape(str(summary.get("Measured Requests", "n/a")))}</div><p class="subtle">completed SQL transactions</p></div>
  </div>
"""


def build_report(results_dir, base, output):
    run = load_run(results_dir, base)
    metadata = metadata_for(run["dir"])

    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BenchBase Spark Report - {html.escape(run["base"])}</title>
  {STYLE}
</head>
<body>
<main>
  <h1>BenchBase Spark Report</h1>
  <p>{html.escape(run["base"])}</p>
  {cards(run)}
  {run_context(run["summary"], run["config"], run["rows"], run["dir"].name)}
  {settings_section(metadata)}
  {svg_line_chart(run["rows"], [LAT_AVG, LAT_P95, LAT_MAX], "Latency", "ms")}
  {svg_line_chart(run["rows"], [THROUGHPUT], "Throughput", "req/s")}
  {query_table(run["query_rows"])}
  {query_reference_section(run["dir"], metadata, active_query_ids=configured_query_ids(run["config"]))}
  {summary_block(run["summary"])}
</main>
</body>
</html>
"""
    output.write_text(html_text, encoding="utf-8")


def compare_cards(flight, direct):
    rows = []
    metrics = [
        ("Throughput req/s", number(flight["summary"].get(THROUGHPUT)), number(direct["summary"].get(THROUGHPUT)), True),
        ("Avg latency ms", summary_latency_value(flight["summary"], "Average"), summary_latency_value(direct["summary"], "Average"), False),
        ("P95 latency ms", summary_latency_value(flight["summary"], "95th Percentile"), summary_latency_value(direct["summary"], "95th Percentile"), False),
        ("Max latency ms", summary_latency_value(flight["summary"], "Maximum"), summary_latency_value(direct["summary"], "Maximum"), False),
    ]
    for label, flight_value, direct_value, higher_better in metrics:
        delta = flight_value - direct_value
        if direct_value:
            pct = (delta / direct_value) * 100
            delta_text = f"{delta:+.3f} ({pct:+.1f}%)"
        else:
            delta_text = "n/a"
        winner = "Flight" if (flight_value > direct_value if higher_better else flight_value < direct_value) else "Direct"
        rows.append(
            f"<tr><td>{html.escape(label)}</td><td>{fmt(flight_value)}</td><td>{fmt(direct_value)}</td><td>{html.escape(delta_text)}</td><td>{winner}</td></tr>"
        )
    return f"""
<section>
  <h2>Flight vs Direct Summary</h2>
  <table>
    <thead><tr><th>Metric</th><th>Flight</th><th>Direct</th><th>Flight - Direct</th><th>Better</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</section>
"""


def compare_reference_section(parent_dir, metadata, flight, direct):
    if not metadata or not metadata.get("reference_queries"):
        return ""
    return f"""
<section>
  <h2>Query Result Correctness</h2>
  <div class="grid2">
    <div>{query_reference_section(parent_dir / "flight", metadata, "Flight Actual vs Expected", configured_query_ids(flight["config"]))}</div>
    <div>{query_reference_section(parent_dir / "direct", metadata, "Direct Actual vs Expected", configured_query_ids(direct["config"]))}</div>
  </div>
</section>
"""


def optional_number(value, divisor=1):
    if value is None:
        return "n/a"
    return fmt(number(value) / divisor)


def observation_statistics_section(machine_result):
    rows = []
    for observation in machine_result.get("observations", []):
        metrics = observation.get("metrics", {})
        engines = {
            engine.get("id"): engine for engine in observation.get("engines", [])
        }
        cells = []
        for engine_id in ("flight", "direct"):
            engine_metrics = metrics.get(engine_id, {})
            latency = engine_metrics.get("latency_microseconds", {})
            engine = engines.get(engine_id, {})
            report = engine.get("artifact_refs", {}).get("report")
            label = html.escape(str(engine.get("status", "missing")))
            if report:
                label = f'<a href="{html.escape(report)}">{label}</a>'
            cells.extend(
                [
                    label,
                    optional_number(latency.get("median"), 1000),
                    optional_number(latency.get("p95"), 1000),
                    html.escape(str(engine_metrics.get("samples", 0))),
                ]
            )
        failures = "; ".join(
            f"{failure.get('engine')}: {failure.get('reason')}"
            for failure in observation.get("failures", [])
        ) or "none"
        rows.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                observation.get("observation_index", ""),
                html.escape(" → ".join(observation.get("engine_order", []))),
                html.escape(str(observation.get("status", ""))),
                *cells,
                html.escape(failures),
            )
        )
    return f"""
<section>
  <h2>Paired Observations</h2>
  <p class="subtle">Latency values are calculated from each observation's raw
  measured transactions. Serial work phases retain their repetition indices.</p>
  <table>
    <thead>
      <tr>
        <th>Pair</th><th>Order</th><th>Status</th>
        <th>Flight</th><th>Flight median ms</th><th>Flight p95 ms</th><th>Flight samples</th>
        <th>Direct</th><th>Direct median ms</th><th>Direct p95 ms</th><th>Direct samples</th>
        <th>Failures</th>
      </tr>
    </thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</section>
"""


def aggregate_statistics_section(machine_result):
    aggregate = machine_result.get("aggregate_summary", {})
    rows = []
    for engine_id in ("flight", "direct"):
        engine = aggregate.get("engines", {}).get(engine_id, {})
        latency = engine.get("latency_microseconds", {})
        rows.append(
            "<tr><td>%s</td><td>%s/%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td></tr>"
            % (
                html.escape(engine_id),
                engine.get("successful_observations", 0),
                engine.get("scheduled_observations", 0),
                optional_number(latency.get("median"), 1000),
                optional_number(latency.get("minimum"), 1000),
                optional_number(latency.get("maximum"), 1000),
                optional_number(latency.get("spread"), 1000),
                optional_number(latency.get("p25"), 1000),
                optional_number(latency.get("p75"), 1000),
                optional_number(latency.get("iqr"), 1000),
                optional_number(latency.get("p95"), 1000),
            )
        )
    paired = aggregate.get("paired", {})
    ratios = paired.get("flight_to_direct_median_latency_ratio", {})
    return f"""
<section>
  <h2>Aggregate Statistics</h2>
  <p class="subtle">Each successful observation contributes one median, so
  observations are equally weighted. Quartiles and p95 use linear interpolation.</p>
  <table>
    <thead><tr><th>Engine</th><th>Successful pairs</th><th>Median ms</th><th>Min ms</th><th>Max ms</th><th>Spread ms</th><th>p25 ms</th><th>p75 ms</th><th>IQR ms</th><th>p95 ms</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <p>Complete pairs: {paired.get('complete_pairs', 0)} /
  {paired.get('scheduled_pairs', 0)}. Median Flight/Direct latency ratio:
  <strong>{optional_number(ratios.get('median'))}</strong>;
  ratio spread: {optional_number(ratios.get('spread'))};
  ratio IQR: {optional_number(ratios.get('iqr'))};
  ratio p95: {optional_number(ratios.get('p95'))}.</p>
</section>
"""


def query_aggregate_section(machine_result):
    aggregate = machine_result.get("aggregate_summary", {})
    engines = aggregate.get("engines", {})
    paired_queries = aggregate.get("paired", {}).get("queries", {})
    query_ids = sorted(
        {
            query_id
            for engine in engines.values()
            for query_id in engine.get("queries", {})
        }
        | set(paired_queries),
        key=lambda query_id: int(query_id.removeprefix("q")),
    )
    if not query_ids:
        return ""
    rows = []
    for query_id in query_ids:
        cells = []
        for engine_id in ("flight", "direct"):
            summary = (
                engines.get(engine_id, {})
                .get("queries", {})
                .get(query_id, {})
                .get("observation_median_latency_microseconds", {})
            )
            cells.extend(
                [
                    optional_number(summary.get("median"), 1000),
                    optional_number(summary.get("iqr"), 1000),
                    optional_number(summary.get("p95"), 1000),
                ]
            )
        ratios = paired_queries.get(query_id, {}).get(
            "flight_to_direct_median_latency_ratio", {}
        )
        cells.extend(
            [
                optional_number(ratios.get("median")),
                optional_number(ratios.get("iqr")),
            ]
        )
        rows.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (html.escape(query_id.upper()), *cells)
        )
    return f"""
<section>
  <h2>Per-query Aggregate Latency</h2>
  <table>
    <thead><tr><th>Query</th><th>Flight median ms</th><th>Flight IQR ms</th><th>Flight p95 ms</th><th>Direct median ms</th><th>Direct IQR ms</th><th>Direct p95 ms</th><th>F/D median ratio</th><th>Ratio IQR</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</section>
"""


def machine_policy_section(machine_result):
    run = machine_result.get("run", {})
    policy = run.get("policy", {})
    publication = machine_result.get("comparison", {}).get("publication", {})
    state = publication.get("state", "not-publishable")
    css_class = "ok" if state == "publishable" else "bad"
    reasons = "; ".join(publication.get("reasons", [])) or "none"
    return f"""
<section>
  <h2>Measurement Policy</h2>
  <p>Publication: <span class="{css_class}">{html.escape(state)}</span></p>
  <table class="kv"><tbody>
    <tr><th>Paired observations</th><td>{policy.get('paired_observations', 'n/a')}</td></tr>
    <tr><th>Order schedule</th><td>{html.escape(str(policy.get('engine_order_schedule', [])))}</td></tr>
    <tr><th>Warmup</th><td>{policy.get('warmup_seconds', 'n/a')} seconds per engine run</td></tr>
    <tr><th>Query repetitions</th><td>{policy.get('repetitions', 'n/a')}</td></tr>
    <tr><th>Cache policy</th><td>{html.escape(str(policy.get('cache_policy', 'n/a')))}</td></tr>
    <tr><th>Started</th><td>{html.escape(str(run.get('started_at', 'n/a')))}</td></tr>
    <tr><th>Finished</th><td>{html.escape(str(run.get('finished_at', 'n/a')))}</td></tr>
    <tr><th>Publication reasons</th><td>{html.escape(reasons)}</td></tr>
  </tbody></table>
</section>
"""


def build_machine_compare_report(results_dir, output, machine_result):
    metadata = metadata_for(results_dir)
    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BenchBase Spark Paired Report - {html.escape(results_dir.name)}</title>
  {STYLE}
</head>
<body>
<main>
  <h1>BenchBase Spark Paired Report</h1>
  <p>{html.escape(results_dir.name)}</p>
  {machine_policy_section(machine_result)}
  {observation_statistics_section(machine_result)}
  {aggregate_statistics_section(machine_result)}
  {query_aggregate_section(machine_result)}
  {execution_path_section(results_dir)}
  {settings_section(metadata)}
</main>
</body>
</html>
"""
    output.write_text(html_text, encoding="utf-8")


def build_compare_report(results_dir, output):
    machine_result = read_json(results_dir / "benchmark-result.json")
    if machine_result.get("observations"):
        build_machine_compare_report(results_dir, output, machine_result)
        return
    flight = load_run(results_dir / "flight")
    direct = load_run(results_dir / "direct")
    metadata = metadata_for(results_dir)

    flight_link = Path("flight") / flight["report"].name
    direct_link = Path("direct") / direct["report"].name
    configured_ids = configured_query_ids(flight["config"])
    expected_query_ids = (
        range(1, 23) if configured_ids == set(range(1, 23)) else configured_ids
    )
    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BenchBase Spark Compare Report - {html.escape(results_dir.name)}</title>
  {STYLE}
</head>
<body>
<main>
  <h1>BenchBase Spark Compare Report</h1>
  <p>{html.escape(results_dir.name)}</p>
  <section>
    <h2>Reports</h2>
    <p><a href="{html.escape(str(flight_link))}">Open Flight-only report</a></p>
    <p><a href="{html.escape(str(direct_link))}">Open Direct-only report</a></p>
  </section>
  {compare_cards(flight, direct)}
  {execution_path_section(results_dir)}
  {settings_section(metadata)}
  {svg_query_latency_chart(
      flight["query_latency_rows"],
      direct["query_latency_rows"],
      expected_query_ids,
  )}
  {svg_compare_chart([
      {"label": "Flight throughput", "rows": flight["rows"], "column": THROUGHPUT, "color": "#2563eb"},
      {"label": "Direct throughput", "rows": direct["rows"], "column": THROUGHPUT, "color": "#f97316"},
  ], "Throughput: Flight vs Direct", "req/s")}
  {svg_compare_chart([
      {"label": "Flight avg latency", "rows": flight["rows"], "column": LAT_AVG, "color": "#2563eb"},
      {"label": "Direct avg latency", "rows": direct["rows"], "column": LAT_AVG, "color": "#f97316"},
  ], "Average Latency: Flight vs Direct", "ms")}
  {svg_compare_chart([
      {"label": "Flight P95 latency", "rows": flight["rows"], "column": LAT_P95, "color": "#2563eb"},
      {"label": "Direct P95 latency", "rows": direct["rows"], "column": LAT_P95, "color": "#f97316"},
  ], "P95 Latency: Flight vs Direct", "ms")}
  {compare_reference_section(results_dir, metadata, flight, direct)}
</main>
</body>
</html>
"""
    output.write_text(html_text, encoding="utf-8")


def main():
    args = parse_args()
    results_dir = args.results.resolve()
    if args.compare:
        output = args.out or results_dir / "compare.report.html"
        build_compare_report(results_dir, output)
    else:
        summary_path = find_summary(results_dir, args.base)
        run_dir = summary_path.parent
        base = summary_path.name.removesuffix(".summary.json")
        output = args.out or run_dir / f"{base}.report.html"
        build_report(run_dir, base, output)
    print(output)


if __name__ == "__main__":
    main()
