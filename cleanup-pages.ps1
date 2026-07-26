param(
    [string[]]$Queries = @('q1', 'q6', 'q14'),
    [double[]]$Scales = @(0.1, 1.0),
    [int[]]$Nodes = @(1, 3, 8)
)

$ErrorActionPreference = 'Continue'

Write-Host "=== ArrowFlight Pages Cleanup ==="
Write-Host "Keeping: queries=$($Queries -join ','), scales=$($Scales -join ','), nodes=$($Nodes -join ',')"
Write-Host ""

# ── Step 1: Filter benchmarks.json ──
Write-Host "[1/4] Filtering benchmarks.json..."

$json = Get-Content 'pages/benchmarks.json' -Raw | ConvertFrom-Json
$totalBefore = $json.Count

function Test-ValidRun($entry) {
    $q = if ($entry.query -eq $null) { '' } else { [string]$entry.query }
    if ($q.ToLower() -notin $Queries) { return $false }

    $s = $entry.scale
    if ($s -eq $null -or $s -notin $Scales) { return $false }

    $n = $entry.flightNodes
    if ($n -ne $null) {
        $nv = [int]$n
        if ($nv -notin $Nodes) { return $false }
    }

    $avg = $entry.avgMs
    if ($avg -ne $null -and (-not ($avg -is [string])) -and [double]$avg -le 0) { return $false }

    return $true
}

function Test-ValidCompare($entry) {
    if ($entry.kind -ne 'compare') { return $false }

    $q = if ($entry.query -eq $null) { '' } else { [string]$entry.query }
    if ($q.ToLower() -notin $Queries) { return $false }

    $s = $entry.scale
    if ($s -eq $null -or $s -notin $Scales) { return $false }

    $n = $entry.flightNodes
    if ($n -ne $null) {
        $nv = [int]$n
        if ($nv -notin $Nodes) { return $false }
    }

    $flight = $entry.flight
    if ($flight -ne $null) {
        $fav = $flight.avgMs
        if ($fav -ne $null -and (-not ($fav -is [string])) -and [double]$fav -le 0) { return $false }
    }
    $direct = $entry.direct
    if ($direct -ne $null) {
        $dav = $direct.avgMs
        if ($dav -ne $null -and (-not ($dav -is [string])) -and [double]$dav -le 0) { return $false }
    }

    return $true
}

$valid = $json | Where-Object {
    $ok = if ($_.kind -eq 'compare') { Test-ValidCompare $_ } else { Test-ValidRun $_ }
    if (-not $ok) { Write-Host "  EXCLUDED: id=$($_.id) kind=$($_.kind) query=$($_.query) scale=$($_.scale) nodes=$($_.flightNodes)" }
    $ok
}

$totalAfter = $valid.Count
Write-Host "  $totalBefore -> $totalAfter entries (removed $($totalBefore - $totalAfter))"

$valid | ConvertTo-Json -Depth 10 | Set-Content 'pages/benchmarks.json'
Write-Host "  Saved pages/benchmarks.json"

# ── Step 2: Remove invalid benchmark directories ──
Write-Host "[2/4] Removing orphan benchmark directories..."

$validDirs = $valid | ForEach-Object {
    $f = $_.files
    # files field: "benchmarks/<dirname>" or "benchmarks/<dirname>/flight"
    if ($f) {
        $parts = $f.Trim('/').Split('/')
        if ($parts.Length -ge 2) { $parts[1] }
    }
} | Where-Object { $_ -ne '' } | Sort-Object -Unique

$allDirs = Get-ChildItem 'pages/benchmarks' -Directory | ForEach-Object { $_.Name }

$toDelete = $allDirs | Where-Object { $_ -notin $validDirs }
$deletedCount = 0
foreach ($d in $toDelete) {
    $path = "pages/benchmarks/$d"
    Write-Host "  Removing: $path"
    Remove-Item -Recurse -Force $path -ErrorAction SilentlyContinue
    $deletedCount++
}
Write-Host "  Removed $deletedCount directories"

# ── Step 3: Regenerate index.html ──
Write-Host "[3/4] Regenerating index.html..."

$html = @'
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Arrow Flight Benchmarks — Valid Matrix</title>
  <style>
    body { margin: 0; font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; background: #f7f8fb; color: #111827; }
    main { max-width: 1220px; margin: 0 auto; padding: 28px; }
    h1 { margin: 0 0 6px; font-size: 30px; }
    p { color: #5b6472; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; margin: 22px 0; }
    select, input { border: 1px solid #d1d5db; border-radius: 7px; padding: 9px 10px; background: #fff; min-width: 150px; }
    input { flex: 1; min-width: 230px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; margin: 18px 0; }
    .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; }
    .label { color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 25px; font-weight: 750; margin-top: 3px; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }
    th, td { padding: 10px 12px; border-bottom: 1px solid #edf0f3; text-align: right; vertical-align: top; }
    th:first-child, td:first-child { text-align: left; }
    th { color: #5b6472; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; background: #fbfcfe; }
    td strong { display: block; }
    td span { display: block; color: #6b7280; font-size: 12px; margin-top: 2px; }
    a { color: #2563eb; margin-right: 10px; white-space: nowrap; }
    .empty { background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 18px; }
    .note { background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px; padding: 12px 16px; margin: 18px 0; color: #92400e; font-size: 13px; }
  </style>
</head>
<body>
<main>
  <h1>Arrow Flight Benchmarks — Valid Matrix</h1>
  <p>Curated TPC-H benchmark results. Only publishable runs — see <a href="https://github.com/nsu-fit/ArrowFlight">README</a> for methodology.</p>

  <div class="note">
    <strong>Matrix scope:</strong> Q1, Q6, Q14 · SF 0.1, 1.0 · 1, 3, 8 nodes · ArrowFlight vs Spark Direct on HDFS Parquet.
    Each paired run has verified engine-order alternation, warmup, and cache policy.
    Exploratory and historical runs are excluded.
  </div>

  <div class="cards">
    <div class="card"><div class="label">Publishable Runs</div><div class="value" id="compare-count">0</div></div>
    <div class="card"><div class="label">Latest Run</div><div class="value" id="latest-run">-</div></div>
  </div>

  <div class="toolbar">
    <select id="query-filter">
      <option value="">All queries</option>
    </select>
    <select id="scale-filter">
      <option value="">All scales</option>
    </select>
    <select id="nodes-filter">
      <option value="">All nodes</option>
    </select>
    <input id="search" type="search" placeholder="Search run name">
  </div>

  <div class="empty" id="empty" style="display:none">No runs match the selected filters.</div>
  <table>
    <thead>
      <tr>
        <th>Run</th>
        <th>Query</th>
        <th>SF</th>
        <th>Nodes</th>
        <th>Engine Order</th>
        <th>Flight thr.</th>
        <th>Direct thr.</th>
        <th>Flight avg</th>
        <th>Direct avg</th>
        <th>Links</th>
      </tr>
    </thead>
    <tbody id="runs"></tbody>
  </table>
</main>
<script>
'@

# Build table rows from valid compares
$rows = @()
foreach ($e in $valid) {
    if ($e.kind -ne 'compare') { continue }
    
    $f = $e.flight
    $d = $e.direct
    
    $id = $e.id
    $title = $e.title
    $q = $e.query
    $s = $e.scale
    $n = $e.flightNodes
    
    $ft = if ($f -and $f.throughput) { "{0:N3} req/s" -f [double]$f.throughput } else { "-" }
    $dt = if ($d -and $d.throughput) { "{0:N3} req/s" -f [double]$d.throughput } else { "-" }
    $fa = if ($f -and $f.avgMs) { "{0:N1} ms" -f [double]$f.avgMs } else { "-" }
    $da = if ($d -and $d.avgMs) { "{0:N1} ms" -f [double]$d.avgMs } else { "-" }

    $links = @()
    if ($e.report) { $links += "<a href=`"$($e.report)`">compare</a>" }
    if ($f -and $f.report) { $links += "<a href=`"$($f.report)`">flight</a>" }
    if ($d -and $d.report) { $links += "<a href=`"$($d.report)`">direct</a>" }
    $linksStr = $links -join ' '

    $row = @"
      <tr data-query="$q" data-scale="$s" data-nodes="$n">
        <td><strong>$title</strong><span>compare</span></td>
        <td>$q</td>
        <td>$s</td>
        <td>$n</td>
        <td><span>paired</span></td>
        <td>$ft</td>
        <td>$dt</td>
        <td>$fa</td>
        <td>$da</td>
        <td>$linksStr</td>
      </tr>
"@
    $rows += $row
}

$html += $rows -join "`n"

$html += @'
  <script>
    const rows = [...document.querySelectorAll('#runs tr')];
    document.getElementById('compare-count').textContent = rows.length;
    document.getElementById('latest-run').textContent = rows[0]?.querySelector('strong')?.textContent || '-';

    const queryFilter = document.getElementById('query-filter');
    const scaleFilter = document.getElementById('scale-filter');
    const nodesFilter = document.getElementById('nodes-filter');
    const search = document.getElementById('search');

    function addOptions(select, values) {
      values.filter(Boolean).sort().forEach((value) => {
        const opt = document.createElement('option');
        opt.value = value;
        opt.textContent = value;
        select.appendChild(opt);
      });
    }

    addOptions(queryFilter, [...new Set(rows.map(r => r.dataset.query))]);
    addOptions(scaleFilter, [...new Set(rows.map(r => r.dataset.scale))]);
    addOptions(nodesFilter, [...new Set(rows.map(r => r.dataset.nodes))]);

    function applyFilters() {
      const q = queryFilter.value;
      const s = scaleFilter.value;
      const n = nodesFilter.value;
      const term = search.value.trim().toLowerCase();
      let visible = 0;
      rows.forEach(row => {
        const show = (!q || row.dataset.query === q)
          && (!s || row.dataset.scale === s)
          && (!n || row.dataset.nodes === n)
          && (!term || row.textContent.toLowerCase().includes(term));
        row.style.display = show ? '' : 'none';
        if (show) visible++;
      });
    }

    [queryFilter, scaleFilter, nodesFilter, search].forEach(el => el.addEventListener('input', applyFilters));
    applyFilters();
  </script>
</body>
</html>
'@

$html | Set-Content 'pages/index.html' -NoNewline
Write-Host "  Saved pages/index.html"

# ── Step 4: Verify ──
Write-Host "[4/4] Verification..."
$finalJson = Get-Content 'pages/benchmarks.json' -Raw | ConvertFrom-Json
$compareCount = ($finalJson | Where-Object { $_.kind -eq 'compare' }).Count
$dirCount = (Get-ChildItem 'pages/benchmarks' -Directory).Count

Write-Host ""
Write-Host "=== Done ==="
Write-Host "benchmarks.json entries: $($finalJson.Count)"
Write-Host "  compare runs: $compareCount"
Write-Host "  directories on disk: $dirCount"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  git add pages/"
Write-Host "  git commit -m 'chore: cleanup Pages to publishable matrix only'"
Write-Host "  git push"
