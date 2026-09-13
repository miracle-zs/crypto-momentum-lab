from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import zlib
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta
from pathlib import Path


ROOT = Path("/Users/zhangshuai/PycharmProjects/crypto-momentum-lab")
OUT = Path(
    "/Users/zhangshuai/.codex/visualizations/2026/09/01/"
    "01a05b05-895a-7442-92d7-bd008bb99649/paper05-rank-comparison.html"
)
RUN_ID = "paper-account-05-orderflow-candle15m-v1"
UTC8 = timezone(timedelta(hours=8))
UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

PAPER_FILLS = ROOT / "data/server-paper-accounts-api-20260815T144127Z/raw/paper_fills.csv"
PAPER_HISTORY = ROOT / (
    "data/server-paper-accounts-api-20260815T144127Z/raw/"
    "paper_account_05_history.json"
)
SNAPSHOT_FILES = [
    ROOT / "data/liquidation-replay-20260810Tw1uYaO/universe_snapshots.csv.gz",
    ROOT / "data/live-trading-20260819T133022Z/raw/universe_snapshots.csv.gz",
    ROOT / "data/live-trading-20260820T160716Z/raw/universe_snapshots.csv.gz",
]
ENTRY_FILES = [
    ROOT / "data/liquidation-replay-20260810Tw1uYaO/universe_entries.csv.gz",
    ROOT / "data/live-trading-20260819T133022Z/raw/universe_entries.csv.gz.partial",
    ROOT / "data/live-trading-20260820T160716Z/raw/universe_entries.csv.gz",
]


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def number(value: str | None, default: float | None = None) -> float | None:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def integer(value: str | None) -> int | None:
    parsed = number(value)
    return int(parsed) if parsed is not None else None


def iter_gzip_rows(path: Path):
    """Yield rows from a complete or partially written gzip CSV.

    The Aug-19 export is a truncated gzip archive.  gzip.open yields its valid
    prefix before raising at EOF, so callers can still use the valid rows.
    """
    try:
        handle = gzip.open(path, "rt", encoding="utf-8", errors="ignore", newline="")
    except OSError:
        return
    try:
        with handle:
            for line in handle:
                yield next(csv.reader([line]))
    except (OSError, EOFError, gzip.BadGzipFile, UnicodeDecodeError, zlib.error):
        return


def load_snapshots() -> list[tuple[datetime, str]]:
    by_id: dict[str, datetime] = {}
    for path in SNAPSHOT_FILES:
        if not path.exists():
            continue
        for row in iter_gzip_rows(path):
            if len(row) != 5 or row[0] == "snapshot_id" or not UUID_RE.match(row[0]):
                continue
            observed_at = parse_dt(row[1])
            if observed_at is not None and row[4].lower() == "t":
                by_id.setdefault(row[0], observed_at)
    return sorted((observed_at, snapshot_id) for snapshot_id, observed_at in by_id.items())


def load_entries() -> dict[tuple[str, str], dict[str, float | int | None]]:
    entries: dict[tuple[str, str], dict[str, float | int | None]] = {}
    for path in ENTRY_FILES:
        if not path.exists():
            continue
        for row in iter_gzip_rows(path):
            if len(row) != 10 or row[0] == "snapshot_id" or not UUID_RE.match(row[0]):
                continue
            symbol = row[1].strip()
            daily_return = number(row[5])
            if not symbol or daily_return is None:
                continue
            candidate = {
                "daily_return": daily_return,
                "gainer_rank": integer(row[6]),
                "loser_rank": integer(row[7]),
            }
            key = (row[0], symbol)
            existing = entries.get(key)
            if existing is None:
                entries[key] = candidate
                continue
            existing_has_rank = existing["gainer_rank"] is not None or existing["loser_rank"] is not None
            candidate_has_rank = candidate["gainer_rank"] is not None or candidate["loser_rank"] is not None
            if candidate_has_rank and not existing_has_rank:
                entries[key] = candidate
    return entries


def load_fills() -> dict[str, dict[str, str]]:
    fills: dict[str, dict[str, str]] = {}
    with PAPER_FILLS.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("run_id") == RUN_ID and row.get("status") == "filled":
                fills[row["fill_id"]] = row
    return fills


def latest_entry(
    snapshots: list[tuple[datetime, str]],
    entries: dict[tuple[str, str], dict[str, float | int | None]],
    observed_at: datetime,
    symbol: str,
) -> tuple[dict[str, float | int | None] | None, datetime | None]:
    times = [item[0] for item in snapshots]
    index = bisect_right(times, observed_at) - 1
    if index < 0:
        return None, None
    snapshot_time, snapshot_id = snapshots[index]
    return entries.get((snapshot_id, symbol)), snapshot_time


def classify_rank(entry: dict[str, float | int | None] | None) -> str:
    if entry is None:
        return "unranked"
    daily_return = float(entry["daily_return"] or 0.0)
    gainer = entry["gainer_rank"] is not None and daily_return > 0
    loser = entry["loser_rank"] is not None and daily_return < 0
    if gainer and loser:
        return "both"
    if gainer:
        return "gainer"
    if loser:
        return "loser"
    return "unranked"


def load_positions() -> list[dict[str, object]]:
    snapshots = load_snapshots()
    entries = load_entries()
    fills = load_fills()
    history = json.loads(PAPER_HISTORY.read_text(encoding="utf-8"))
    positions: list[dict[str, object]] = []
    for raw in history.get("closed_trades", []):
        fill = fills.get(raw.get("entry_fill_id", ""))
        filled_at = parse_dt((fill or {}).get("filled_at")) or parse_dt(raw.get("opened_at"))
        closed_at = parse_dt(raw.get("closed_at"))
        if filled_at is None or closed_at is None:
            continue
        rank_entry, rank_observed_at = latest_entry(
            snapshots, entries, filled_at, raw.get("symbol", "")
        )
        rank_side = classify_rank(rank_entry)
        side = str(raw.get("side", "")).lower()
        net_pnl = number(raw.get("realized_pnl"), 0.0) or 0.0
        positions.append(
            {
                "position_id": raw.get("position_id"),
                "symbol": raw.get("symbol"),
                "side": side,
                "filled_at": filled_at,
                "closed_at": closed_at,
                "closed_day": closed_at.astimezone(UTC8).date().isoformat(),
                "net_pnl": net_pnl,
                "rank_side": rank_side,
                "gainer_rank": (rank_entry or {}).get("gainer_rank"),
                "loser_rank": (rank_entry or {}).get("loser_rank"),
                "rank_return": (rank_entry or {}).get("daily_return"),
                "rank_observed_at": rank_observed_at,
            }
        )
    positions.sort(key=lambda item: (item["closed_at"], item["position_id"]))
    return positions


def load_server_positions(path: Path) -> list[dict[str, object]]:
    """Load the server-side, rank-enriched paper positions TSV."""
    positions: list[dict[str, object]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter="|")
        for row in reader:
            if len(row) < 20:
                continue
            opened_at = parse_dt(row[4])
            closed_at = parse_dt(row[5])
            signal_time = parse_dt(row[14]) or opened_at
            if opened_at is None or closed_at is None or signal_time is None:
                continue
            rank_side = row[15].strip().lower() or "unranked"
            if rank_side not in {"gainer", "loser", "both", "unranked"}:
                rank_side = "unranked"
            positions.append(
                {
                    "position_id": row[0],
                    "symbol": row[1],
                    "side": row[2].lower(),
                    "filled_at": signal_time,
                    "closed_at": closed_at,
                    "closed_day": closed_at.astimezone(UTC8).date().isoformat(),
                    "net_pnl": number(row[12], 0.0) or 0.0,
                    "rank_side": rank_side,
                    "gainer_rank": integer(row[16]),
                    "loser_rank": integer(row[17]),
                    "rank_return": number(row[18]),
                }
            )
    positions.sort(key=lambda item: (item["closed_at"], item["position_id"]))
    return positions


SERIES = [
    {
        "key": "acc05",
        "label": "paper-account-05 全量",
        "short_label": "acc05 全量",
        "description": "全部已平仓多空交易",
    },
    {
        "key": "gainer_long",
        "label": "涨幅榜做多",
        "short_label": "涨幅榜 + 多",
        "description": "严格正向 gainer_rank + long",
    },
    {
        "key": "loser_short",
        "label": "跌幅榜做空",
        "short_label": "跌幅榜 + 空",
        "description": "严格负向 loser_rank + short",
    },
    {
        "key": "combo",
        "label": "涨幅榜做多 + 跌幅榜做空",
        "short_label": "榜单方向组合",
        "description": "两类过滤的并集",
    },
    {
        "key": "gainer_short",
        "label": "涨幅榜做空",
        "short_label": "涨幅榜 + 空",
        "description": "严格正向 gainer_rank + short",
    },
    {
        "key": "loser_long",
        "label": "跌幅榜做多",
        "short_label": "跌幅榜 + 多",
        "description": "严格负向 loser_rank + long",
    },
]


def belongs(position: dict[str, object], key: str) -> bool:
    if key == "acc05":
        return True
    gainer_long = position["side"] == "long" and position["rank_side"] == "gainer"
    loser_short = position["side"] == "short" and position["rank_side"] == "loser"
    gainer_short = position["side"] == "short" and position["rank_side"] == "gainer"
    loser_long = position["side"] == "long" and position["rank_side"] == "loser"
    if key == "gainer_long":
        return gainer_long
    if key == "loser_short":
        return loser_short
    if key == "combo":
        return gainer_long or loser_short
    if key == "gainer_short":
        return gainer_short
    if key == "loser_long":
        return loser_long
    return False


def metrics(rows: list[dict[str, object]]) -> dict[str, float | int | None]:
    values = [float(row["net_pnl"]) for row in rows]
    positive = [value for value in values if value > 0]
    negative = [value for value in values if value < 0]
    total = sum(values)
    gross_profit = sum(positive)
    gross_loss = sum(negative)
    pf = gross_profit / abs(gross_loss) if gross_loss else None
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        running += value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
    return {
        "trades": len(values),
        "net_pnl": total,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": pf,
        "win_rate": len(positive) / len(values) if values else None,
        "average_pnl": total / len(values) if values else None,
        "max_drawdown": max_drawdown,
    }


def build_dataset(
    positions: list[dict[str, object]] | None = None,
    source: str | None = None,
    open_positions: int = 0,
) -> dict[str, object]:
    positions = load_positions() if positions is None else positions
    if not positions:
        raise RuntimeError("paper-account-05 没有可用的已平仓交易")
    by_key = {series["key"]: [row for row in positions if belongs(row, series["key"])] for series in SERIES}
    days = sorted({row["closed_day"] for row in positions})
    daily_values: dict[str, dict[str, float]] = {}
    cumulative: dict[str, list[dict[str, str | float]]] = {}
    drawdown: dict[str, list[dict[str, str | float]]] = {}
    for series in SERIES:
        key = series["key"]
        values_by_day = defaultdict(float)
        for row in by_key[key]:
            values_by_day[row["closed_day"]] += float(row["net_pnl"])
        daily_values[key] = {day: values_by_day[day] for day in days}
        running = 0.0
        peak = 0.0
        cumulative[key] = []
        drawdown[key] = []
        for day in days:
            running += values_by_day[day]
            peak = max(peak, running)
            cumulative[key].append({"date": day, "value": running})
            drawdown[key].append({"date": day, "value": running - peak})

    rank_counts = defaultdict(int)
    for row in positions:
        rank_counts[f"{row['side']}|{row['rank_side']}"] += 1
    rank_covered = sum(value for key, value in rank_counts.items() if not key.endswith("|unranked"))
    rank_both = sum(value for key, value in rank_counts.items() if key.endswith("|both"))
    closed_at = [row["closed_at"] for row in positions]
    opened_at = [row["filled_at"] for row in positions]
    daily = []
    for day in days:
        daily.append(
            {
                "date": day,
                **{series["key"]: daily_values[series["key"]][day] for series in SERIES},
            }
        )
    return {
        "meta": {
            "run_id": RUN_ID,
            "source": source or "本地保存的 paper-account-05 历史快照",
            "window_start": min(opened_at).isoformat(),
            "window_end": max(closed_at).isoformat(),
            "closed_trades": len(positions),
            "open_positions": open_positions,
            "rank_covered": rank_covered,
            "rank_coverage": rank_covered / len(positions) if positions else 0,
            "rank_both": rank_both,
            "rank_definition": "信号/入场时最近榜单快照；gainer_rank 且日收益>0，或 loser_rank 且日收益<0",
            "rank_counts": dict(sorted(rank_counts.items())),
        },
        "series": [
            {
                **series,
                "index": index,
                "summary": metrics(by_key[series["key"]]),
            }
            for index, series in enumerate(SERIES)
        ],
        "days": days,
        "daily": daily,
        "cumulative": cumulative,
        "drawdown": drawdown,
    }


def render_html(dataset: dict[str, object]) -> str:
    data_json = json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
    return f'''<div id="paper05-rank-comparison" class="paper05-viz">
  <style>
    #paper05-rank-comparison {{
      color: var(--foreground);
      font-family: var(--font-family, ui-sans-serif, system-ui, sans-serif);
      line-height: 1.35;
      position: relative;
      width: 100%;
      --paper05-series-1: rgb(37 99 235);
      --paper05-series-2: rgb(234 88 12);
      --paper05-series-3: rgb(22 163 74);
      --paper05-series-4: rgb(147 51 234);
      --paper05-series-5: rgb(8 145 178);
      --paper05-series-6: rgb(219 39 119);
    }}
    #paper05-rank-comparison * {{ box-sizing: border-box; }}
    #paper05-rank-comparison .viz-header {{ margin: 0 0 14px; }}
    #paper05-rank-comparison h1,
    #paper05-rank-comparison h2 {{
      color: var(--foreground);
      font-weight: 650;
      letter-spacing: -0.015em;
      margin: 0;
    }}
    #paper05-rank-comparison h1 {{ font-size: 20px; }}
    #paper05-rank-comparison h2 {{ font-size: 14px; margin: 16px 0 4px; }}
    #paper05-rank-comparison .subtitle,
    #paper05-rank-comparison .caption {{
      color: var(--muted-foreground);
      font-size: 12px;
      margin: 4px 0 0;
    }}
    #paper05-rank-comparison .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 4px 16px;
      margin: 12px 0 4px;
    }}
    #paper05-rank-comparison .legend-button {{
      align-items: baseline;
      background: transparent;
      border: 0;
      color: var(--foreground);
      cursor: pointer;
      display: inline-flex;
      font: inherit;
      gap: 6px;
      padding: 2px 0;
      text-align: left;
    }}
    #paper05-rank-comparison .legend-button[aria-pressed="false"] {{
      color: var(--muted-foreground);
      text-decoration: line-through;
      text-decoration-thickness: 1px;
    }}
    #paper05-rank-comparison .swatch {{
      background: currentColor;
      display: inline-block;
      height: 3px;
      margin-top: 0.45em;
      width: 18px;
    }}
    #paper05-rank-comparison .legend-button.series-1 .swatch {{ background: repeating-linear-gradient(90deg, currentColor 0 8px, transparent 8px 12px); }}
    #paper05-rank-comparison .legend-button.series-2 .swatch {{ background: repeating-linear-gradient(90deg, currentColor 0 3px, transparent 3px 6px); }}
    #paper05-rank-comparison .legend-button.series-3 .swatch {{ background: repeating-linear-gradient(90deg, currentColor 0 10px, transparent 10px 13px, currentColor 13px 16px, transparent 16px 19px); }}
    #paper05-rank-comparison .legend-button.series-4 .swatch {{ background: repeating-linear-gradient(90deg, currentColor 0 5px, transparent 5px 8px, currentColor 8px 9px, transparent 9px 12px); }}
    #paper05-rank-comparison .legend-button.series-5 .swatch {{ background: repeating-linear-gradient(90deg, currentColor 0 1px, transparent 1px 4px); }}
    #paper05-rank-comparison .legend-label {{ font-size: 12px; }}
    #paper05-rank-comparison .legend-stats {{
      color: var(--muted-foreground);
      font-size: 11px;
      white-space: nowrap;
    }}
    #paper05-rank-comparison .legend-button.series-0 {{ color: var(--paper05-series-1); }}
    #paper05-rank-comparison .legend-button.series-1 {{ color: var(--paper05-series-2); }}
    #paper05-rank-comparison .legend-button.series-2 {{ color: var(--paper05-series-3); }}
    #paper05-rank-comparison .legend-button.series-3 {{ color: var(--paper05-series-4); }}
    #paper05-rank-comparison .legend-button.series-4 {{ color: var(--paper05-series-5); }}
    #paper05-rank-comparison .legend-button.series-5 {{ color: var(--paper05-series-6); }}
    #paper05-rank-comparison .plot {{ width: 100%; }}
    #paper05-rank-comparison svg {{
      display: block;
      height: auto;
      overflow: visible;
      width: 100%;
    }}
    #paper05-rank-comparison svg text {{
      fill: var(--foreground);
      font-size: 12px;
    }}
    #paper05-rank-comparison .axis path,
    #paper05-rank-comparison .axis line {{ stroke: var(--border); shape-rendering: crispEdges; }}
    #paper05-rank-comparison .axis text {{ fill: var(--muted-foreground); font-size: 11px; }}
    #paper05-rank-comparison .grid line {{ stroke: var(--border); stroke-opacity: 0.55; }}
    #paper05-rank-comparison .grid path {{ stroke-width: 0; }}
    #paper05-rank-comparison .frame {{
      fill: none;
      stroke: var(--border);
      stroke-width: 1;
      vector-effect: non-scaling-stroke;
    }}
    #paper05-rank-comparison .zero-line {{
      stroke: var(--muted-foreground);
      stroke-dasharray: 3 3;
      stroke-opacity: 0.8;
      vector-effect: non-scaling-stroke;
    }}
    #paper05-rank-comparison .series-line {{
      fill: none;
      stroke: currentColor;
      stroke-linecap: round;
      stroke-linejoin: round;
      stroke-width: 2.1;
      vector-effect: non-scaling-stroke;
    }}
    #paper05-rank-comparison .series-0 {{ color: var(--paper05-series-1); }}
    #paper05-rank-comparison .series-1 {{ color: var(--paper05-series-2); }}
    #paper05-rank-comparison .series-2 {{ color: var(--paper05-series-3); }}
    #paper05-rank-comparison .series-3 {{ color: var(--paper05-series-4); }}
    #paper05-rank-comparison .series-4 {{ color: var(--paper05-series-5); }}
    #paper05-rank-comparison .series-5 {{ color: var(--paper05-series-6); }}
    #paper05-rank-comparison .series-line.series-1 {{ stroke-dasharray: 8 4; }}
    #paper05-rank-comparison .series-line.series-2 {{ stroke-dasharray: 2 3; }}
    #paper05-rank-comparison .series-line.series-3 {{ stroke-dasharray: 10 3 2 3; }}
    #paper05-rank-comparison .series-line.series-4 {{ stroke-dasharray: 5 3 1 3; }}
    #paper05-rank-comparison .series-line.series-5 {{ stroke-dasharray: 1 3; }}
    #paper05-rank-comparison .bar {{ fill: currentColor; opacity: 0.78; stroke: none; }}
    #paper05-rank-comparison .bar.series-1 {{ opacity: 0.62; }}
    #paper05-rank-comparison .bar.series-2 {{ opacity: 0.46; }}
    #paper05-rank-comparison .bar.series-3 {{ opacity: 0.34; }}
    #paper05-rank-comparison .bar.series-4 {{ opacity: 0.55; }}
    #paper05-rank-comparison .bar.series-5 {{ opacity: 0.42; }}
    #paper05-rank-comparison .axis-title {{ fill: var(--foreground); font-size: 12px; }}
    #paper05-rank-comparison .hit {{ cursor: crosshair; fill: transparent; pointer-events: all; }}
    #paper05-rank-comparison .hover-guide {{
      stroke: var(--muted-foreground);
      stroke-dasharray: 2 3;
      stroke-width: 1;
      vector-effect: non-scaling-stroke;
    }}
    #paper05-rank-comparison .hover-marker {{
      fill: var(--background);
      stroke: currentColor;
      stroke-width: 2;
      vector-effect: non-scaling-stroke;
    }}
    #paper05-rank-comparison .tooltip {{
      background: var(--popover);
      border: 1px solid var(--border);
      color: var(--popover-foreground);
      font-size: 12px;
      max-width: 260px;
      opacity: 0;
      padding: 7px 9px;
      pointer-events: none;
      position: absolute;
      transform: translate(10px, 10px);
      z-index: 10;
    }}
    #paper05-rank-comparison .tooltip-date {{ font-weight: 650; margin-bottom: 3px; }}
    #paper05-rank-comparison .tooltip-row {{ align-items: center; display: flex; gap: 5px; justify-content: space-between; white-space: nowrap; }}
    #paper05-rank-comparison .tooltip-dot {{ background: currentColor; height: 7px; width: 7px; }}
    #paper05-rank-comparison .tooltip-name {{ margin-right: 16px; }}
    #paper05-rank-comparison .chart-shell {{ position: relative; }}
    #paper05-rank-comparison .caption {{ border-top: 1px solid var(--border); padding-top: 8px; }}
    @media (max-width: 520px) {{
      #paper05-rank-comparison h1 {{ font-size: 18px; }}
      #paper05-rank-comparison .legend {{ gap: 2px 10px; }}
      #paper05-rank-comparison .legend-stats {{ display: none; }}
    }}
  </style>
  <div class="viz-header">
    <h1>paper-account-05：榜单方向过滤对比</h1>
    <p class="subtitle">累计净 PnL、日度 PnL 与序列回撤；筛选组沿用 acc05 的已完成交易结果，按入场时最近榜单快照分类。</p>
    <div class="legend" aria-label="系列开关"></div>
  </div>
  <section class="plot" aria-labelledby="paper05-cumulative-title">
    <h2 id="paper05-cumulative-title">累计净 PnL</h2>
    <div class="chart-shell"><svg id="paper05-cumulative" role="img" aria-labelledby="paper05-cumulative-title"></svg></div>
  </section>
  <section class="plot" aria-labelledby="paper05-daily-title">
    <h2 id="paper05-daily-title">日度净 PnL</h2>
    <div class="chart-shell"><svg id="paper05-daily" role="img" aria-labelledby="paper05-daily-title"></svg></div>
  </section>
  <section class="plot" aria-labelledby="paper05-drawdown-title">
    <h2 id="paper05-drawdown-title">累计曲线回撤</h2>
    <div class="chart-shell"><svg id="paper05-drawdown" role="img" aria-labelledby="paper05-drawdown-title"></svg></div>
  </section>
  <p class="caption">口径：净 PnL 使用历史 paper position 的 realized_pnl（含双边手续费）；“涨幅榜”=严格正向 gainer_rank，“跌幅榜”=严格负向 loser_rank。数据窗口 <span class="data-window"></span>；榜单可对齐率 <span class="coverage"></span>。<span class="source-note"></span></p>
  <div class="tooltip" role="tooltip"></div>
  <noscript>请启用 JavaScript 查看图表。</noscript>
  <script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
  <script>
  (() => {{
    const root = document.getElementById("paper05-rank-comparison");
    const dataset = {data_json};
    const series = dataset.series;
    const seriesByKey = new Map(series.map((item, index) => [item.key, {{ ...item, index }}]));
    const active = new Set(series.map(item => item.key));
    const tooltip = root.querySelector(".tooltip");
    const colors = series.map((_, index) => `series-${{index}}`);
    const fmtPnl = value => `${{value >= 0 ? "+" : ""}}${{value.toFixed(2)}} U`;
    const fmtPf = value => value == null ? "—" : value.toFixed(3);
    const fmtPct = value => value == null ? "—" : `${{(value * 100).toFixed(1)}}%`;
    const dateFmt = d3.timeFormat("%m-%d");
    const fullDateFmt = d3.timeFormat("%Y-%m-%d");
    const parseDay = value => d3.timeParse("%Y-%m-%d")(value);
    const days = dataset.days.map(parseDay);
    const maxTicks = () => window.innerWidth <= 380 ? 4 : 7;
    const tickValues = values => values.filter((_, index) => index % Math.max(1, Math.ceil(values.length / maxTicks())) === 0);
    const dataWindow = `${{dataset.meta.window_start.slice(0, 10)}} 至 ${{dataset.meta.window_end.slice(0, 10)}}`;
    const openNote = dataset.meta.open_positions ? `；另有 ${{dataset.meta.open_positions}} 笔未平仓，未纳入已实现 PnL` : "";
    root.querySelector(".data-window").textContent = dataWindow;
    root.querySelector(".coverage").textContent = `${{(dataset.meta.rank_coverage * 100).toFixed(1)}}%（${{dataset.meta.rank_covered}}/${{dataset.meta.closed_trades}}）`;
    root.querySelector(".source-note").textContent = ` ${{dataset.meta.source}}${{openNote}}。`;

    const legend = root.querySelector(".legend");
    for (const item of series) {{
      const button = document.createElement("button");
      button.type = "button";
      button.className = `legend-button ${{colors[item.index]}}`;
      button.setAttribute("aria-pressed", "true");
      button.innerHTML = `<span class="swatch" aria-hidden="true"></span><span class="legend-label">${{item.label}}</span><span class="legend-stats">${{item.summary.trades}} 笔 · ${{fmtPnl(item.summary.net_pnl)}} · PF ${{fmtPf(item.summary.profit_factor)}}</span>`;
      button.addEventListener("click", () => {{
        if (active.has(item.key)) active.delete(item.key); else active.add(item.key);
        button.setAttribute("aria-pressed", String(active.has(item.key)));
        drawAll();
      }});
      legend.appendChild(button);
    }}

    function setupSvg(id) {{
      const svg = d3.select(root.querySelector(`#${{id}}`));
      const width = Math.max(320, svg.node().parentElement.getBoundingClientRect().width);
      const height = width <= 520 ? 260 : 310;
      svg.attr("viewBox", `0 0 ${{width}} ${{height}}`).attr("width", width).attr("height", height);
      svg.selectAll("*").remove();
      return {{ svg, width, height, margin: {{ top: 18, right: 16, bottom: 44, left: 64 }} }};
    }}

    function paddedDomain(values) {{
      const extent = d3.extent(values);
      if (extent[0] === extent[1]) {{
        const pad = Math.max(1, Math.abs(extent[0]) * 0.12);
        return [extent[0] - pad, extent[1] + pad];
      }}
      return d3.nice(extent[0], extent[1], 6);
    }}

    function addFrameAndAxes(chart, x, y, xAxis, yAxis, axisLabels, yGrid = true) {{
      const {{ svg, width, height, margin }} = chart;
      const plotWidth = width - margin.left - margin.right;
      const plotHeight = height - margin.top - margin.bottom;
      if (yGrid) {{
        svg.append("g").attr("class", "grid").attr("transform", `translate(${{margin.left}},0)`).call(
          d3.axisLeft(y).ticks(5).tickSize(-plotWidth).tickFormat("")
        );
      }}
      svg.append("g").attr("class", "axis x-axis").attr("transform", `translate(0,${{margin.top + plotHeight}})`).call(xAxis);
      svg.append("g").attr("class", "axis y-axis").attr("transform", `translate(${{margin.left}},0)`).call(yAxis);
      svg.append("rect").attr("class", "frame").attr("data-chart-frame", "true")
        .attr("x", margin.left).attr("y", margin.top).attr("width", plotWidth).attr("height", plotHeight);
      svg.append("text").attr("class", "axis-title").attr("data-axis", "x")
        .attr("x", margin.left + plotWidth / 2).attr("y", height - 5).attr("text-anchor", "middle").text(axisLabels.x);
      svg.append("text").attr("class", "axis-title").attr("data-axis", "y")
        .attr("transform", `translate(15,${{margin.top + plotHeight / 2}}) rotate(-90)`).attr("text-anchor", "middle").text(axisLabels.y);
    }}

    function visibleKeys() {{ return series.filter(item => active.has(item.key)).map(item => item.key); }}

    function interpolate(values, xValue) {{
      if (!values.length) return null;
      const center = d3.bisector(item => item.x).center(values, xValue);
      const i = Math.max(0, Math.min(values.length - 1, center));
      let left = i;
      if (values[i].x > xValue) left = i - 1;
      left = Math.max(0, Math.min(values.length - 2, left));
      const right = Math.min(values.length - 1, left + 1);
      if (values[left].x === values[right].x) return values[left].value;
      const ratio = Math.max(0, Math.min(1, (xValue - values[left].x) / (values[right].x - values[left].x)));
      return values[left].value + (values[right].value - values[left].value) * ratio;
    }}

    function showTooltip(chart, xPx, dateValue, rows, yScale, xScale, label) {{
      const {{ width, height, margin }} = chart;
      const shell = chart.svg.node().parentElement;
      const tooltipX = Math.min(width - 266, Math.max(6, xPx + margin.left));
      const tooltipY = Math.min(height - 80, Math.max(4, yScale(d3.max(rows, row => row.value) || 0)));
      tooltip.style.left = `${{tooltipX}}px`;
      tooltip.style.top = `${{tooltipY}}px`;
      tooltip.style.opacity = "1";
      tooltip.innerHTML = `<div class="tooltip-date">${{label}}</div>` + rows.map(row => `<div class="tooltip-row ${{colors[row.index]}}"><span><span class="tooltip-dot" aria-hidden="true"></span><span class="tooltip-name">${{row.label}}</span></span><strong>${{fmtPnl(row.value)}}</strong></div>`).join("");
    }}

    function clearTooltip() {{
      tooltip.style.opacity = "0";
      tooltip.innerHTML = "";
    }}

    function drawLineChart(id, source, axisLabels, yLabel, title) {{
      const chart = setupSvg(id);
      const {{ svg, width, height, margin }} = chart;
      const plotWidth = width - margin.left - margin.right;
      const plotHeight = height - margin.top - margin.bottom;
      const lines = new Map();
      for (const item of series) {{
        lines.set(item.key, source[item.key].map(point => ({{ x: parseDay(point.date), value: point.value }})));
      }}
      const values = series.filter(item => active.has(item.key)).flatMap(item => lines.get(item.key).map(point => point.value));
      const x = d3.scaleTime().domain(d3.extent(days)).range([margin.left, margin.left + plotWidth]);
      const y = d3.scaleLinear().domain(paddedDomain([...values, 0])).range([margin.top + plotHeight, margin.top]);
      const xAxis = d3.axisBottom(x).tickValues(tickValues(days)).tickFormat(dateFmt);
      const yAxis = d3.axisLeft(y).ticks(5).tickFormat(value => `${{value.toFixed(0)}}`);
      addFrameAndAxes(chart, x, y, xAxis, yAxis, {{ x: "日期（UTC+8）", y: yLabel }});
      if (y(0) >= margin.top && y(0) <= margin.top + plotHeight) {{
        svg.append("line").attr("class", "zero-line").attr("x1", margin.left).attr("x2", margin.left + plotWidth).attr("y1", y(0)).attr("y2", y(0));
      }}
      const line = d3.line().x(point => x(point.x)).y(point => y(point.value));
      for (const item of series) {{
        if (!active.has(item.key)) continue;
        svg.append("path").datum(lines.get(item.key)).attr("class", `series-line ${{colors[item.index]}}`).attr("d", line);
      }}
      const guide = svg.append("line").attr("class", "hover-guide").attr("data-chart-hover-guide", "true").attr("display", "none");
      const markers = new Map();
      for (const item of series) {{
        markers.set(item.key, svg.append("circle").attr("class", `hover-marker ${{colors[item.index]}}`).attr("data-chart-hover-marker", "true").attr("r", 4).attr("display", "none"));
      }}
      const hit = svg.append("rect").attr("class", "hit").attr("data-chart-hit", "true").attr("data-chart-hover-overlay", "cross-series")
        .attr("x", margin.left).attr("y", margin.top).attr("width", plotWidth).attr("height", plotHeight);
      function update(event) {{
        const [pointerX] = d3.pointer(event, svg.node());
        const boundedX = Math.max(margin.left, Math.min(margin.left + plotWidth, pointerX));
        const dateValue = x.invert(boundedX);
        const rows = [];
        for (const item of series) {{
          const marker = markers.get(item.key);
          if (!active.has(item.key)) {{ marker.attr("display", "none"); continue; }}
          const value = interpolate(lines.get(item.key), dateValue);
          rows.push({{ ...item, value }});
          marker.attr("display", null).attr("cx", boundedX).attr("cy", y(value));
        }}
        guide.attr("display", null).attr("x1", boundedX).attr("x2", boundedX).attr("y1", margin.top).attr("y2", margin.top + plotHeight);
        showTooltip(chart, boundedX - margin.left, dateValue, rows, y, x, fullDateFmt(dateValue));
      }}
      hit.on("pointermove", update).on("pointerenter", update).on("pointerleave", () => {{
        guide.attr("display", "none");
        markers.forEach(marker => marker.attr("display", "none"));
        clearTooltip();
      }}).on("pointerdown", update);
    }}

    function drawDaily() {{
      const chart = setupSvg("paper05-daily");
      const {{ svg, width, height, margin }} = chart;
      const plotWidth = width - margin.left - margin.right;
      const plotHeight = height - margin.top - margin.bottom;
      const x0 = d3.scaleBand().domain(dataset.days).range([margin.left, margin.left + plotWidth]).paddingInner(0.16).paddingOuter(0.04);
      const x1 = d3.scaleBand().domain(series.map(item => item.key)).range([0, x0.bandwidth()]).padding(0.08);
      const values = dataset.daily.flatMap(row => visibleKeys().map(key => row[key]));
      const y = d3.scaleLinear().domain(paddedDomain([...values, 0])).range([margin.top + plotHeight, margin.top]);
      const xAxis = d3.axisBottom(x0).tickValues(tickValues(dataset.days)).tickFormat(value => dateFmt(parseDay(value)));
      const yAxis = d3.axisLeft(y).ticks(5).tickFormat(value => `${{value.toFixed(0)}}`);
      addFrameAndAxes(chart, x0, y, xAxis, yAxis, {{ x: "日期（UTC+8）", y: "日净 PnL（USDT）" }});
      if (y(0) >= margin.top && y(0) <= margin.top + plotHeight) {{
        svg.append("line").attr("class", "zero-line").attr("x1", margin.left).attr("x2", margin.left + plotWidth).attr("y1", y(0)).attr("y2", y(0));
      }}
      const grouped = svg.append("g");
      for (const row of dataset.daily) {{
        const group = grouped.append("g").attr("transform", `translate(${{x0(row.date)}},0)`);
        for (const item of series) {{
          if (!active.has(item.key)) continue;
          const value = row[item.key];
          group.append("rect").attr("class", `bar series-${{item.index}}`).attr("x", x1(item.key)).attr("width", x1.bandwidth())
            .attr("y", Math.min(y(0), y(value))).attr("height", Math.abs(y(value) - y(0)));
        }}
      }}
      const guide = svg.append("line").attr("class", "hover-guide").attr("data-chart-hover-guide", "true").attr("display", "none");
      const markers = new Map();
      for (const item of series) {{ markers.set(item.key, svg.append("circle").attr("class", `hover-marker ${{colors[item.index]}}`).attr("data-chart-hover-marker", "true").attr("r", 4).attr("display", "none")); }}
      const hit = svg.append("rect").attr("class", "hit").attr("data-chart-hit", "true").attr("data-chart-hover-overlay", "cross-series")
        .attr("x", margin.left).attr("y", margin.top).attr("width", plotWidth).attr("height", plotHeight);
      function updateDaily(event) {{
        const [pointerX] = d3.pointer(event, svg.node());
        const rawIndex = Math.round((pointerX - margin.left - x0.step() * x0.paddingOuter()) / x0.step());
        const index = Math.max(0, Math.min(dataset.daily.length - 1, rawIndex));
        const row = dataset.daily[index];
        const xCenter = x0(row.date) + x0.bandwidth() / 2;
        const rows = [];
        for (const item of series) {{
          const marker = markers.get(item.key);
          if (!active.has(item.key)) {{ marker.attr("display", "none"); continue; }}
          const value = row[item.key];
          rows.push({{ ...item, value }});
          marker.attr("display", null).attr("cx", xCenter).attr("cy", y(value));
        }}
        guide.attr("display", null).attr("x1", xCenter).attr("x2", xCenter).attr("y1", margin.top).attr("y2", margin.top + plotHeight);
        showTooltip(chart, xCenter - margin.left, parseDay(row.date), rows, y, x0, row.date);
      }}
      hit.on("pointermove", updateDaily).on("pointerenter", updateDaily).on("pointerleave", () => {{
        guide.attr("display", "none");
        markers.forEach(marker => marker.attr("display", "none"));
        clearTooltip();
      }}).on("pointerdown", updateDaily);
    }}

    function drawAll() {{
      if (!window.d3) return;
      drawLineChart("paper05-cumulative", dataset.cumulative, {{ x: "日期（UTC+8）", y: "累计净 PnL（USDT）" }}, "累计净 PnL（USDT）");
      drawDaily();
      drawLineChart("paper05-drawdown", dataset.drawdown, {{ x: "日期（UTC+8）", y: "回撤（USDT）" }}, "回撤（USDT）");
    }}
    const observer = new ResizeObserver(drawAll);
    observer.observe(root);
    drawAll();
  }})();
  </script>
</div>'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-tsv", type=Path)
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--open-positions", type=int, default=0)
    args = parser.parse_args()
    if args.server_tsv:
        positions = load_server_positions(args.server_tsv)
        source = "服务器 PostgreSQL 当前 paper-account-05 快照"
    else:
        positions = load_positions()
        source = "本地保存的 paper-account-05 历史快照"
    dataset = build_dataset(
        positions=positions,
        source=source,
        open_positions=max(0, args.open_positions),
    )
    html = render_html(dataset)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(json.dumps(dataset["meta"], ensure_ascii=False, indent=2))
    for item in dataset["series"]:
        print(item["key"], json.dumps(item["summary"], ensure_ascii=False))
    print(f"wrote {args.out} ({args.out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
