#!/usr/bin/env python3
"""Build a standalone HTML ledger for daily recommendation changes.

The versioned optimization reports are immutable snapshots.  This script reads
those snapshots, keeps the latest usable snapshot for each report day, and
compares each A-G profile with the previous observed report day.  A missing
calendar day is intentionally not treated as a stable configuration.

Usage:
    python scripts/build_parameter_history_html.py
    python scripts/build_parameter_history_html.py \
        --input-dir reports \
        --output reports/optimization-parameter-history.html
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


GROUPS = tuple("ABCDEFG")
CONFIG_KEYS = (
    "impulse_window_buckets",
    "confirmation_buckets",
    "min_return_pct",
    "min_imbalance",
    "min_intensity",
    "min_volume_ratio",
    "cooldown_buckets",
)
CONFIG_SHORT = ("I", "C", "R", "B", "N", "V", "CD")
CONFIG_LABELS = (
    "冲击窗口",
    "确认窗口",
    "最小涨幅",
    "最小不平衡",
    "最小强度",
    "放量阈值",
    "冷却窗口",
)


def parse_assignment(text: str, marker: str) -> Any | None:
    """Decode a JSON value assigned after a JavaScript const declaration."""

    position = text.find(marker)
    if position < 0:
        return None
    start = position + len(marker)
    decoder = json.JSONDecoder()
    try:
        value, _ = decoder.raw_decode(text[start:].lstrip())
    except json.JSONDecodeError:
        return None
    return value


def parse_number(token: str) -> int | float | None:
    cleaned = token.strip().replace("%", "").replace("x", "")
    if not cleaned or cleaned in {"-", "—", "null", "None"}:
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def parse_config_text(config: str | None) -> tuple[dict[str, Any], int]:
    """Parse both the historical six-field and current seven-field display."""

    if not config:
        return {}, 0
    parts = [part.strip() for part in config.split("/")]
    if len(parts) not in {6, 7}:
        return {}, len(parts)

    if len(parts) == 6:
        # Historical reports omitted V.  Omitted volume filtering means the
        # seven-dimensional equivalent is explicitly disabled at 0.00x.
        values = parts[:5] + ["0.00x", parts[5]]
        dimensions = 6
    else:
        values = parts
        dimensions = 7

    raw = {
        key: parse_number(value)
        for key, value in zip(CONFIG_KEYS, values, strict=True)
    }
    return raw, dimensions


def raw_config(profile: dict[str, Any]) -> tuple[dict[str, Any], int]:
    configured = profile.get("config_raw")
    if isinstance(configured, dict):
        values: dict[str, Any] = {}
        for key in CONFIG_KEYS:
            value = configured.get(key)
            if value is None and key == "min_volume_ratio":
                value = configured.get("min_notional_5m_vs_30m")
            values[key] = value
        dimensions = 7 if any(value is not None for value in values.values()) else 0
        if values.get("min_volume_ratio") is None and dimensions:
            values["min_volume_ratio"] = 0.0
            dimensions = 6
        return values, dimensions
    return parse_config_text(profile.get("config"))


def decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        decimal = Decimal(str(value))
    except InvalidOperation:
        return str(value)
    if decimal == 0:
        return "0"
    return format(decimal.normalize(), "f")


def config_tuple(values: dict[str, Any]) -> tuple[str | None, ...]:
    return tuple(decimal_text(values.get(key)) for key in CONFIG_KEYS)


def format_config(values: dict[str, Any]) -> str:
    if not values or all(values.get(key) is None for key in CONFIG_KEYS):
        return "—"
    rendered: list[str] = []
    for index, key in enumerate(CONFIG_KEYS):
        value = values.get(key)
        if value is None:
            rendered.append("—")
            continue
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            rendered.append(str(value))
            continue
        if index in {0, 1, 6}:
            rendered.append(str(int(number)))
        elif index == 2:
            rendered.append(f"{number:.2f}%")
        elif index == 3:
            rendered.append(f"{number:.2f}")
        elif index == 4:
            rendered.append(f"{number:.1f}")
        else:
            rendered.append(f"{number:.2f}x")
    return " / ".join(rendered)


def extract_metrics(profile: dict[str, Any]) -> dict[str, Any]:
    aliases = {
        "pnl": ("full_pnl", "pnl"),
        "dd": ("full_dd", "dd"),
        "margin": ("margin",),
        "score": ("score",),
        "validation_pnl": ("validation_pnl",),
        "holdout_pnl": ("holdout_pnl",),
    }
    metrics: dict[str, Any] = {}
    for output_key, keys in aliases.items():
        for key in keys:
            if profile.get(key) is not None:
                metrics[output_key] = profile[key]
                break
    return metrics


def normalize_profile(profile: dict[str, Any]) -> dict[str, Any]:
    values, dimensions = raw_config(profile)
    return {
        "config": format_config(values),
        "values": values,
        "dimensions": dimensions,
        "metrics": extract_metrics(profile),
    }


def legacy_best_profiles(text: str) -> dict[str, dict[str, Any]]:
    """Read the one older report whose DATA was a JavaScript object literal."""

    profiles: dict[str, dict[str, Any]] = {}
    for group in GROUPS:
        start_match = re.search(rf"(?m)^\s*{group}\s*:\s*\{{", text)
        if not start_match:
            continue
        next_match = re.search(r"\n\s*[A-G]\s*:\s*\{", text[start_match.end() :])
        block_end = start_match.end() + next_match.start() if next_match else len(text)
        block = text[start_match.end() : block_end]
        config_match = re.search(
            r"best\s*:\s*\{.*?config\s*:\s*(['\"])(.*?)\1",
            block,
            re.DOTALL,
        )
        if config_match:
            profiles[group] = {"config": config_match.group(2)}
    return profiles


def report_day(path: Path, text: str, metadata: dict[str, Any]) -> str | None:
    date_match = re.search(r"(20\d{2})(\d{2})(\d{2})", path.name)
    if date_match:
        return "-".join(date_match.groups())
    for pattern in (
        r"LOCAL RESEARCH REPLAY / (20\d{2}-\d{2}-\d{2})",
        r"(?:历史版|本轮|最新完整回补)[^0-9]*(20\d{2}-\d{2}-\d{2})",
    ):
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    data_end = metadata.get("data_end")
    if isinstance(data_end, str) and len(data_end) >= 10:
        return data_end[:10]
    return None


def parse_report(path: Path) -> dict[str, Any] | None:
    text = path.read_text(encoding="utf-8")
    metadata: dict[str, Any] = {}
    profiles_raw: dict[str, Any] = {}
    source_format = ""

    report = parse_assignment(text, "const REPORT =")
    if isinstance(report, dict) and isinstance(report.get("profiles"), dict):
        metadata = report.get("meta") if isinstance(report.get("meta"), dict) else {}
        profiles_raw = report["profiles"]
        source_format = "report"
    else:
        data = parse_assignment(text, "const DATA =")
        if isinstance(data, dict):
            profiles_raw = {
                group: value.get("best", {})
                for group, value in data.items()
                if group in GROUPS and isinstance(value, dict)
            }
            source_format = "legacy-json"
        else:
            profiles_raw = legacy_best_profiles(text)
            source_format = "legacy-js" if profiles_raw else ""

    if not profiles_raw:
        return None
    day = report_day(path, text, metadata)
    if not day:
        return None

    profiles = {
        group: normalize_profile(profile)
        for group, profile in profiles_raw.items()
        if group in GROUPS and isinstance(profile, dict)
    }
    if not profiles:
        return None
    observed_dimensions = max(
        (profile["dimensions"] for profile in profiles.values()),
        default=7 if source_format == "report" else 6,
    )

    feature = metadata.get("feature")
    if not isinstance(feature, str) or not feature:
        feature = "历史快照旧格式"
    exclusion = metadata.get("exclusion")
    if isinstance(exclusion, dict):
        exclusion_label = exclusion.get("window") or "未记录"
    else:
        exclusion_label = "未记录"
    return {
        "day": day,
        "source_file": path.name,
        "source_format": source_format,
        "dimensions": observed_dimensions,
        "data_start": metadata.get("data_start"),
        "data_end": metadata.get("data_end"),
        "optimization_start": metadata.get("optimization_start"),
        "feature": feature,
        "exclusion": exclusion_label,
        "workers": metadata.get("workers"),
        "candidate_count": metadata.get("candidate_count"),
        "profiles": profiles,
    }


def instant(value: str | None) -> float:
    if not value:
        return float("-inf")
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return float("-inf")


def selection_rank(record: dict[str, Any]) -> tuple[Any, ...]:
    name = record["source_file"]
    format_rank = {"report": 3, "legacy-json": 2, "legacy-js": 1}.get(
        record["source_format"], 0
    )
    return (
        instant(record.get("data_end")),
        format_rank,
        1 if "volume7" in name else 0,
        1 if "exclude-0800-0830" in name else 0,
        name,
    )


def compare_profiles(
    current: dict[str, Any] | None,
    previous: dict[str, Any] | None,
) -> tuple[str, list[str]]:
    if not current or current.get("config") == "—":
        return "missing", []
    if not previous or previous.get("config") == "—":
        return "first", []
    current_values = current.get("values", {})
    previous_values = previous.get("values", {})
    changed = [
        short
        for key, short in zip(CONFIG_KEYS, CONFIG_SHORT, strict=True)
        if decimal_text(current_values.get(key))
        != decimal_text(previous_values.get(key))
    ]
    return ("changed", changed) if changed else ("stable", [])


def build_payload(input_dir: Path, output: Path) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    skipped: list[str] = []
    for path in sorted(input_dir.glob("optimization-comparison*.html")):
        if path.resolve() == output.resolve() or path.name == "optimization-comparison-index.html":
            continue
        try:
            record = parse_report(path)
        except (OSError, UnicodeDecodeError) as exc:
            skipped.append(f"{path.name}: {exc}")
            continue
        if record:
            candidates.append(record)

    by_day: dict[str, list[dict[str, Any]]] = {}
    for record in candidates:
        by_day.setdefault(record["day"], []).append(record)

    selected: list[dict[str, Any]] = []
    for day, records in by_day.items():
        chosen = max(records, key=selection_rank)
        chosen = dict(chosen)
        chosen["duplicate_files"] = sorted(
            record["source_file"]
            for record in records
            if record["source_file"] != chosen["source_file"]
        )
        selected.append(chosen)
    selected.sort(key=lambda record: (record["day"], instant(record.get("data_end"))))

    previous_by_group: dict[str, dict[str, Any] | None] = {group: None for group in GROUPS}
    timeline: dict[str, list[dict[str, Any]]] = {group: [] for group in GROUPS}
    for run_index, record in enumerate(selected):
        for group in GROUPS:
            current = record["profiles"].get(group)
            previous = previous_by_group[group]
            status, changed_fields = compare_profiles(current, previous)
            timeline[group].append(
                {
                    "run_index": run_index,
                    "day": record["day"],
                    "source_file": record["source_file"],
                    "status": status,
                    "changed_fields": changed_fields,
                    "config": current.get("config", "—") if current else "—",
                    "previous_config": previous.get("config", "—") if previous else None,
                    "dimensions": current.get("dimensions", 0) if current else 0,
                    "metrics": current.get("metrics", {}) if current else {},
                }
            )
            if current and current.get("config") != "—":
                previous_by_group[group] = current

    latest = selected[-1] if selected else None
    latest_status = {
        group: timeline[group][-1]["status"] if timeline[group] else "missing"
        for group in GROUPS
    }
    latest_changed = [group for group, status in latest_status.items() if status == "changed"]
    latest_stable = [group for group, status in latest_status.items() if status == "stable"]
    total_changes = sum(
        sum(item["status"] == "changed" for item in items) for items in timeline.values()
    )

    generated_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    return {
        "generated_at": generated_at,
        "fields": [
            {"key": key, "short": short, "label": label}
            for key, short, label in zip(CONFIG_KEYS, CONFIG_SHORT, CONFIG_LABELS, strict=True)
        ],
        "groups": list(GROUPS),
        "runs": selected,
        "timeline": timeline,
        "summary": {
            "observed_days": len(selected),
            "first_day": selected[0]["day"] if selected else None,
            "latest_day": latest["day"] if latest else None,
            "latest_source": latest["source_file"] if latest else None,
            "latest_changed": latest_changed,
            "latest_stable": latest_stable,
            "total_changes": total_changes,
            "skipped_files": skipped,
        },
        "notes": [
            "比较对象是每个报告日选定的最终可用快照；同日更早的重复快照不会进入日矩阵，但会记录在该日的来源信息中。",
            "没有报告的自然日不会被补成“保持不变”，因此空白表示没有观测，而不是参数稳定。",
            "旧六维报告的 V 统一按 0.00x 展示，用于与七维放量关闭的结果进行可比比较。",
        ],
    }


HTML_TEMPLATE = r'''<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>参数演进账本 · 每日推荐变化</title>
  <style>
    :root {
      --ink: #eaf1ef;
      --muted: #91a5a4;
      --muted-2: #6b7f80;
      --bg: #0c1317;
      --panel: #121e24;
      --panel-2: #17262e;
      --line: #2b4048;
      --teal: #55d4be;
      --teal-dim: #1c655f;
      --amber: #f1b45e;
      --coral: #f07c7b;
      --blue: #8ab8ff;
      --paper: #f1eadc;
      --mono: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      --sans: "Avenir Next", "Noto Sans SC", "PingFang SC", sans-serif;
    }
    * { box-sizing: border-box; }
    html { background: var(--bg); color: var(--ink); }
    body {
      margin: 0;
      min-width: 320px;
      background:
        linear-gradient(rgba(255,255,255,.022) 1px, transparent 1px),
        #0c1317;
      background-size: 100% 48px;
      font-family: var(--sans);
      letter-spacing: .01em;
    }
    a { color: var(--teal); }
    .page { max-width: 1520px; margin: 0 auto; padding: 36px 34px 76px; }
    .masthead {
      display: flex;
      justify-content: space-between;
      gap: 32px;
      align-items: end;
      padding-bottom: 28px;
      border-bottom: 1px solid var(--line);
    }
    .eyebrow, .section-kicker, .mono { font-family: var(--mono); }
    .eyebrow {
      color: var(--teal);
      font-size: 11px;
      letter-spacing: .18em;
      text-transform: uppercase;
      margin-bottom: 15px;
    }
    h1 { margin: 0; font-size: clamp(32px, 5vw, 66px); line-height: .98; letter-spacing: -.055em; font-weight: 720; }
    .lead { max-width: 650px; margin: 17px 0 0; color: var(--muted); line-height: 1.72; font-size: 14px; }
    .stamp { min-width: 220px; color: var(--muted); font-size: 12px; line-height: 1.7; text-align: right; }
    .stamp strong { color: var(--paper); font-family: var(--mono); font-weight: 500; }
    .summary-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 22px 0 32px; }
    .summary-card { background: var(--panel); border: 1px solid var(--line); padding: 17px 18px 15px; min-height: 112px; }
    .summary-label { color: var(--muted); font-size: 11px; letter-spacing: .12em; text-transform: uppercase; }
    .summary-value { color: var(--paper); font-family: var(--mono); font-size: 27px; margin-top: 13px; }
    .summary-sub { color: var(--muted-2); font-size: 11px; margin-top: 5px; }
    .panel { background: rgba(18,30,36,.93); border: 1px solid var(--line); margin-top: 16px; }
    .panel-head { display: flex; justify-content: space-between; gap: 20px; align-items: end; padding: 22px 24px 16px; }
    .section-kicker { color: var(--amber); font-size: 10px; letter-spacing: .16em; }
    h2 { margin: 7px 0 0; font-size: 22px; letter-spacing: -.025em; }
    .panel-intro { color: var(--muted); font-size: 13px; line-height: 1.65; margin: 0; max-width: 860px; }
    .controls { display: flex; flex-wrap: wrap; justify-content: flex-end; align-items: center; gap: 8px; }
    select, .toggle {
      background: var(--panel-2); color: var(--ink); border: 1px solid var(--line);
      border-radius: 2px; min-height: 34px; padding: 0 11px; font: 12px var(--sans);
    }
    .toggle { display: inline-flex; align-items: center; gap: 8px; cursor: pointer; color: var(--muted); }
    .toggle input { accent-color: var(--teal); }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; padding: 0 24px 15px; color: var(--muted); font-size: 11px; }
    .legend span { display: inline-flex; align-items: center; gap: 5px; }
    .dot { width: 7px; height: 7px; display: inline-block; border-radius: 50%; background: var(--muted-2); }
    .dot.changed { background: var(--teal); box-shadow: 0 0 0 3px rgba(85,212,190,.12); }
    .dot.stable { background: #52686c; }
    .dot.first { background: var(--blue); }
    .dot.missing { background: #3b4a4e; }
    .scroll-x { overflow-x: auto; border-top: 1px solid var(--line); }
    table { width: max(100%, 1120px); border-collapse: collapse; table-layout: fixed; }
    th, td { border-bottom: 1px solid rgba(43,64,72,.78); text-align: left; vertical-align: top; }
    th { background: #15242b; color: var(--muted); font: 10px var(--mono); letter-spacing: .08em; padding: 12px 13px; }
    th:first-child, td:first-child { position: sticky; left: 0; z-index: 2; width: 104px; }
    th:first-child { background: #15242b; }
    td:first-child { background: #111d22; }
    th:not(:first-child), td:not(:first-child) { width: 153px; }
    td { padding: 11px 12px; min-height: 104px; }
    .group-label { font-size: 16px; font-weight: 700; }
    .group-caption { color: var(--muted-2); font: 10px var(--mono); margin-top: 4px; }
    .matrix-cell { appearance: none; width: 100%; min-height: 80px; text-align: left; border: 1px solid transparent; border-radius: 2px; color: var(--ink); background: transparent; padding: 8px; cursor: pointer; }
    .matrix-cell:hover, .matrix-cell:focus-visible { border-color: var(--teal-dim); background: rgba(85,212,190,.06); outline: none; }
    .matrix-cell.is-changed { background: rgba(85,212,190,.085); border-color: rgba(85,212,190,.3); }
    .matrix-cell.is-missing { opacity: .5; cursor: default; }
    .cell-top { display: flex; justify-content: space-between; gap: 6px; align-items: center; }
    .status { display: inline-flex; align-items: center; gap: 5px; font-size: 10px; white-space: nowrap; }
    .status::before { content: ""; width: 6px; height: 6px; border-radius: 50%; background: var(--muted-2); }
    .status.changed { color: var(--teal); }
    .status.changed::before { background: var(--teal); }
    .status.first { color: var(--blue); }
    .status.first::before { background: var(--blue); }
    .status.stable { color: var(--muted); }
    .status.missing { color: var(--muted-2); }
    .cell-delta { color: var(--amber); font: 10px var(--mono); }
    .cell-config { color: var(--paper); font: 11px/1.65 var(--mono); margin-top: 9px; white-space: normal; word-break: keep-all; }
    .cell-meta { color: var(--muted-2); font-size: 10px; margin-top: 5px; }
    .timeline-wrap { padding: 0 24px 25px; }
    .selected-overview { display: grid; grid-template-columns: minmax(0,1fr) minmax(220px,.55fr); gap: 14px; margin-bottom: 19px; }
    .current-config { background: var(--paper); color: #192326; padding: 18px 20px; }
    .current-config small { display: block; color: #5e6d6c; font-size: 10px; letter-spacing: .12em; }
    .current-config strong { display: block; font: 17px/1.6 var(--mono); margin-top: 7px; overflow-wrap: anywhere; }
    .current-config em { display: block; color: #5e6d6c; font-size: 12px; font-style: normal; margin-top: 6px; }
    .change-summary { border: 1px solid var(--line); padding: 16px 18px; }
    .change-summary small { color: var(--muted); font-size: 10px; letter-spacing: .12em; }
    .change-summary strong { display: block; color: var(--amber); font: 24px var(--mono); margin: 8px 0; }
    .pill-row { display: flex; flex-wrap: wrap; gap: 5px; }
    .pill { color: var(--teal); border: 1px solid rgba(85,212,190,.32); padding: 4px 7px; font: 10px var(--mono); }
    .pill.neutral { color: var(--muted); border-color: var(--line); }
    .timeline { display: grid; gap: 7px; }
    .timeline-row { display: grid; grid-template-columns: 108px 118px minmax(0,1fr) minmax(0,1fr) minmax(155px,.45fr); gap: 12px; align-items: center; background: rgba(23,38,46,.75); border-left: 3px solid #3b4a4e; padding: 11px 13px; min-height: 71px; }
    .timeline-row.changed { border-left-color: var(--teal); }
    .timeline-row.first { border-left-color: var(--blue); }
    .timeline-date { font: 13px var(--mono); color: var(--paper); }
    .timeline-date small { display: block; color: var(--muted-2); font: 10px var(--sans); margin-top: 3px; }
    .timeline-config { font: 11px/1.55 var(--mono); color: var(--paper); overflow-wrap: anywhere; }
    .timeline-config span { display: block; color: var(--muted-2); font: 10px var(--sans); margin-bottom: 4px; }
    .arrow { color: var(--amber); text-align: center; font: 16px var(--mono); }
    .timeline-metrics { color: var(--muted); font: 10px/1.7 var(--mono); text-align: right; }
    .timeline-metrics a { font-family: var(--sans); }
    .timeline-empty { color: var(--muted); padding: 20px 0; }
    .heatmap-scroll { overflow-x: auto; padding: 0 24px 25px; }
    .heatmap { min-width: 980px; display: grid; grid-template-columns: 70px repeat(var(--days), minmax(105px, 1fr)); gap: 5px; }
    .heat-head { color: var(--muted); font: 10px var(--mono); padding: 4px 7px 8px; }
    .heat-label { display: flex; align-items: center; color: var(--paper); font: 13px var(--mono); }
    .heat-cell { appearance: none; border: 1px solid var(--line); min-height: 55px; color: var(--muted); background: #17242a; cursor: pointer; text-align: left; padding: 7px; }
    .heat-cell:hover, .heat-cell:focus-visible { outline: 1px solid var(--teal); }
    .heat-cell.changed { color: var(--teal); border-color: rgba(85,212,190,.48); background: rgba(85,212,190,.16); }
    .heat-cell.first { color: var(--blue); border-color: rgba(138,184,255,.38); background: rgba(138,184,255,.10); }
    .heat-cell.missing { color: var(--muted-2); opacity: .62; }
    .heat-count { display: block; font: 15px var(--mono); }
    .heat-detail { display: block; font-size: 10px; margin-top: 4px; }
    .method-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; padding: 0 24px 24px; }
    .method-item { border-top: 1px solid var(--line); padding-top: 12px; color: var(--muted); font-size: 12px; line-height: 1.7; }
    .method-item strong { display: block; color: var(--paper); font-size: 12px; margin-bottom: 4px; }
    .footer { color: var(--muted-2); border-top: 1px solid var(--line); margin-top: 28px; padding-top: 16px; font-size: 11px; line-height: 1.7; }
    .footer a { text-decoration: none; }
    @media (max-width: 850px) {
      .page { padding: 25px 16px 60px; }
      .masthead { display: block; }
      .stamp { text-align: left; margin-top: 17px; }
      .summary-grid { grid-template-columns: repeat(2, 1fr); }
      .panel-head { display: block; padding: 19px 16px 14px; }
      .controls { justify-content: flex-start; margin-top: 14px; }
      .legend, .timeline-wrap, .heatmap-scroll { padding-left: 16px; padding-right: 16px; }
      .selected-overview, .method-grid { grid-template-columns: 1fr; }
      .timeline-row { grid-template-columns: 80px 95px minmax(150px,1fr); }
      .timeline-row .arrow { display: none; }
      .timeline-metrics { text-align: left; grid-column: 2 / -1; }
    }
    @media (max-width: 480px) {
      .summary-grid { gap: 7px; }
      .summary-card { padding: 12px; min-height: 96px; }
      .summary-value { font-size: 21px; }
      h2 { font-size: 19px; }
    }
  </style>
</head>
<body>
  <main class="page">
    <header class="masthead">
      <div>
        <div class="eyebrow">PARAMETER LEDGER / DAILY SEARCH</div>
        <h1>推荐参数<br>的漂移轨迹</h1>
        <p class="lead">把每次参数寻优的最终推荐保存成一条可追踪的时间线。矩阵回答“哪一组在什么时候变了”，详情回答“具体是哪几个维度变了”。</p>
      </div>
      <div class="stamp">生成时间<br><strong id="generated-at">—</strong><br><span id="source-note">—</span></div>
    </header>

    <section class="summary-grid" aria-label="摘要">
      <article class="summary-card"><div class="summary-label">已观测更新日</div><div class="summary-value" id="summary-days">—</div><div class="summary-sub" id="summary-range">—</div></article>
      <article class="summary-card"><div class="summary-label">最新更新</div><div class="summary-value" id="summary-latest">—</div><div class="summary-sub">以报告文件的 data_end 选最终快照</div></article>
      <article class="summary-card"><div class="summary-label">最新发生变化</div><div class="summary-value" id="summary-changed">—</div><div class="summary-sub" id="summary-changed-list">—</div></article>
      <article class="summary-card"><div class="summary-label">历史变化次数</div><div class="summary-value" id="summary-total">—</div><div class="summary-sub">跨 A–G、按相邻观测日比较</div></article>
    </section>

    <section class="panel" aria-labelledby="matrix-title">
      <div class="panel-head">
        <div><div class="section-kicker">01 / CHANGE MATRIX</div><h2 id="matrix-title">每日推荐参数变化矩阵</h2><p class="panel-intro">横向滚动查看全部更新日。点击任意单元格，下面的时间线会切换到对应组。</p></div>
        <div class="controls"><select id="group-select" aria-label="选择策略组"></select><label class="toggle"><input id="changes-only" type="checkbox"> 只看有变化的组</label></div>
      </div>
      <div class="legend"><span><i class="dot changed"></i>发生变化</span><span><i class="dot stable"></i>保持</span><span><i class="dot first"></i>首次记录</span><span><i class="dot missing"></i>未记录 / 旧页面没有该组</span></div>
      <div class="scroll-x"><table><thead id="matrix-head"></thead><tbody id="matrix-body"></tbody></table></div>
    </section>

    <section class="panel" aria-labelledby="timeline-title">
      <div class="panel-head"><div><div class="section-kicker">02 / SELECTED GROUP</div><h2 id="timeline-title">选中组的参数时间线</h2><p class="panel-intro">“变化”会列出发生变化的维度；如果当天没有报告，不会伪造一个保持状态。</p></div></div>
      <div class="timeline-wrap"><div class="selected-overview" id="selected-overview"></div><div class="timeline" id="timeline"></div></div>
    </section>

    <section class="panel" aria-labelledby="heatmap-title">
      <div class="panel-head"><div><div class="section-kicker">03 / DIMENSION HEATMAP</div><h2 id="heatmap-title">变化强度 · 每次改变了几个维度</h2><p class="panel-intro">数字是相对上一次有报告的推荐变化维度数量；这能区分“只改一个阈值”和“整组重新漂移”。</p></div></div>
      <div class="heatmap-scroll"><div class="heatmap" id="heatmap"></div></div>
    </section>

    <section class="panel" aria-labelledby="method-title">
      <div class="panel-head"><div><div class="section-kicker">04 / READING NOTES</div><h2 id="method-title">口径与来源</h2></div></div>
      <div class="method-grid" id="method-grid"></div>
    </section>

    <footer class="footer"><span>参数顺序：I / C / R / B / N / V / CD。</span>　<a href="optimization-comparison-index.html">返回寻优报告版本索引 ↗</a></footer>
  </main>
  <script>
    const DATA = __PAYLOAD__;
    const state = { group: 'A', onlyChanges: false };
    const esc = value => String(value ?? '').replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
    const dayLabel = day => day ? day.slice(5).replace('-', '/') : '—';
    const statusLabel = status => ({changed:'变化', stable:'保持', first:'首次', missing:'未记录'}[status] || status);
    const dimensionsText = count => count === 6 ? '六维旧格式' : count === 7 ? '七维' : '—';
    const money = value => value == null ? '—' : `${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(2)}U`;
    const metricLine = metrics => {
      if (!metrics || Object.keys(metrics).length === 0) return '暂无指标';
      const bits = [];
      if (metrics.pnl != null) bits.push(`PnL ${money(metrics.pnl)}`);
      if (metrics.dd != null) bits.push(`回撤 ${Number(metrics.dd).toFixed(2)}U`);
      if (metrics.margin != null) bits.push(`保证金 ${Number(metrics.margin).toFixed(0)}U`);
      return bits.join(' · ') || '暂无指标';
    };
    function currentTimeline(group) { return DATA.timeline[group] || []; }
    function changedGroups() { return DATA.groups.filter(group => currentTimeline(group).some(item => item.status === 'changed')); }

    function renderSummary() {
      const summary = DATA.summary;
      document.getElementById('generated-at').textContent = summary.latest_day ? `${summary.latest_day} UTC` : '—';
      document.getElementById('source-note').textContent = summary.latest_source || '暂无结构化报告';
      document.getElementById('summary-days').textContent = summary.observed_days;
      document.getElementById('summary-range').textContent = summary.first_day && summary.latest_day ? `${summary.first_day} → ${summary.latest_day}` : '—';
      document.getElementById('summary-latest').textContent = summary.latest_day || '—';
      document.getElementById('summary-changed').textContent = summary.latest_changed.length;
      document.getElementById('summary-changed-list').textContent = summary.latest_changed.length ? summary.latest_changed.map(group => `组 ${group}`).join(' · ') : '最新一轮全部保持';
      document.getElementById('summary-total').textContent = summary.total_changes;
    }

    function renderControls() {
      const select = document.getElementById('group-select');
      select.innerHTML = `<option value="ALL">全部 A–G</option>${DATA.groups.map(group => `<option value="${group}">策略组 ${group}</option>`).join('')}`;
      select.value = state.group;
      select.onchange = () => { state.group = select.value; renderTimeline(); };
      const checkbox = document.getElementById('changes-only');
      checkbox.checked = state.onlyChanges;
      checkbox.onchange = () => { state.onlyChanges = checkbox.checked; renderMatrix(); };
    }

    function cellMarkup(group, item) {
      const change = item.changed_fields?.length ? item.changed_fields.join(' · ') : '';
      const source = DATA.runs[item.run_index]?.source_file || item.source_file;
      const missing = item.status === 'missing';
      return `<button class="matrix-cell ${item.status === 'changed' ? 'is-changed' : ''} ${missing ? 'is-missing' : ''}" data-group="${group}" data-run="${item.run_index}" ${missing ? 'disabled' : ''} title="${esc(source)}">
        <span class="cell-top"><span class="status ${item.status}">${statusLabel(item.status)}</span><span class="cell-delta">${esc(change)}</span></span>
        <span class="cell-config">${esc(item.config)}</span>
        <span class="cell-meta">${esc(dimensionsText(item.dimensions))}</span>
      </button>`;
    }

    function renderMatrix() {
      const head = document.getElementById('matrix-head');
      head.innerHTML = `<tr><th>策略组</th>${DATA.runs.map(run => `<th><span>${dayLabel(run.day)}</span><br><small>${run.data_end ? esc(run.data_end.slice(11,16)) + ' UTC' : '旧格式'}</small></th>`).join('')}</tr>`;
      const visible = DATA.groups.filter(group => !state.onlyChanges || currentTimeline(group).some(item => item.status === 'changed'));
      document.getElementById('matrix-body').innerHTML = visible.length ? visible.map(group => `<tr><td><div class="group-label">${group}</div><div class="group-caption">推荐组</div></td>${currentTimeline(group).map(item => `<td>${cellMarkup(group, item)}</td>`).join('')}</tr>`).join('') : `<tr><td colspan="${DATA.runs.length + 1}"><div class="timeline-empty">当前筛选没有发生过参数变化的组。</div></td></tr>`;
      document.querySelectorAll('.matrix-cell:not([disabled])').forEach(button => button.addEventListener('click', () => { state.group = button.dataset.group; document.getElementById('group-select').value = state.group; renderTimeline(); document.getElementById('timeline-title').scrollIntoView({behavior:'smooth', block:'start'}); }));
    }

    function renderTimeline() {
      const group = state.group === 'ALL' ? 'A' : state.group;
      const items = currentTimeline(group);
      const latest = [...items].reverse().find(item => item.status !== 'missing');
      const allChanges = items.flatMap(item => item.changed_fields || []);
      const counts = allChanges.reduce((map, field) => { map[field] = (map[field] || 0) + 1; return map; }, {});
      const dominant = Object.entries(counts).sort((a,b) => b[1] - a[1]).slice(0, 4);
      document.getElementById('timeline-title').textContent = `策略组 ${group} · 参数时间线`;
      document.getElementById('selected-overview').innerHTML = latest ? `<div class="current-config"><small>最新可见推荐 / ${esc(latest.day)}</small><strong>${esc(latest.config)}</strong><em>${esc(dimensionsText(latest.dimensions))}　·　${esc(metricLine(latest.metrics))}</em></div><div class="change-summary"><small>历史上最常变化的维度</small><strong>${items.filter(item => item.status === 'changed').length} 次</strong><div class="pill-row">${dominant.length ? dominant.map(([field,count]) => `<span class="pill">${esc(field)} × ${count}</span>`).join('') : '<span class="pill neutral">尚无变化</span>'}</div></div>` : '<div class="timeline-empty">没有可显示的记录。</div>';
      document.getElementById('timeline').innerHTML = items.length ? items.map(item => `<article class="timeline-row ${item.status}"><div class="timeline-date">${dayLabel(item.day)}<small>${esc(statusLabel(item.status))}</small></div><div><span class="status ${item.status}">${esc(statusLabel(item.status))}</span><div class="cell-meta">${esc(item.changed_fields?.length ? item.changed_fields.join(' · ') : dimensionsText(item.dimensions))}</div></div><div class="timeline-config"><span>本次</span>${esc(item.config)}</div><div class="arrow">${item.previous_config && item.status === 'changed' ? '→' : '·'}</div><div class="timeline-config"><span>${item.previous_config && item.status === 'changed' ? '上一观测日' : '来源'}</span>${item.previous_config && item.status === 'changed' ? esc(item.previous_config) : `<a href="${esc(item.source_file)}">打开报告 ↗</a>`}<div class="timeline-metrics">${esc(metricLine(item.metrics))}</div></div></article>`).join('') : '<div class="timeline-empty">没有可显示的记录。</div>';
    }

    function renderHeatmap() {
      const heatmap = document.getElementById('heatmap');
      heatmap.style.setProperty('--days', DATA.runs.length);
      const head = `<div></div>${DATA.runs.map(run => `<div class="heat-head">${dayLabel(run.day)}</div>`).join('')}`;
      const rows = DATA.groups.map(group => `<div class="heat-label">${group}</div>${currentTimeline(group).map(item => { const count = item.status === 'changed' ? item.changed_fields.length : item.status === 'first' ? '·' : '—'; return `<button class="heat-cell ${item.status}" data-group="${group}" data-run="${item.run_index}" title="${esc(item.config)}"><span class="heat-count">${count}</span><span class="heat-detail">${esc(item.status === 'changed' ? item.changed_fields.join(' · ') : statusLabel(item.status))}</span></button>`; }).join('')}`).join('');
      heatmap.innerHTML = head + rows;
      document.querySelectorAll('.heat-cell').forEach(button => button.addEventListener('click', () => { state.group = button.dataset.group; document.getElementById('group-select').value = state.group; renderTimeline(); document.getElementById('timeline-title').scrollIntoView({behavior:'smooth', block:'start'}); }));
    }

    function renderMethods() {
      const latest = DATA.runs[DATA.runs.length - 1];
      const methods = [
        ['比较单位', '每个报告日选一个最终快照。同日有多个文件时，优先 data_end 较晚的版本，再优先结构化 REPORT 和 volume7。'],
        ['特征口径', latest ? `${latest.feature}；排除时段：${latest.exclusion}。来源：${latest.source_file}。` : '暂无报告。'],
        ['字段解释', 'I 冲击窗口，C 确认窗口，R 最小涨幅，B 不平衡，N 强度，V 放量，CD 冷却窗口。'],
      ];
      document.getElementById('method-grid').innerHTML = methods.map(([title, body]) => `<div class="method-item"><strong>${esc(title)}</strong>${esc(body)}</div>`).join('');
    }

    renderSummary();
    renderControls();
    renderMatrix();
    renderTimeline();
    renderHeatmap();
    renderMethods();
  </script>
</body>
</html>
'''


def write_html(payload: dict[str, Any], output: Path) -> None:
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    serialized = serialized.replace("</", "<\\/")
    html = HTML_TEMPLATE.replace("__PAYLOAD__", serialized)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("reports"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/optimization-parameter-history.html"),
    )
    args = parser.parse_args()

    payload = build_payload(args.input_dir, args.output)
    if not payload["runs"]:
        print("No readable optimization report snapshots found.", file=sys.stderr)
        return 1
    write_html(payload, args.output)
    summary = payload["summary"]
    print(
        f"Wrote {args.output} with {summary['observed_days']} observed days "
        f"({summary['first_day']} -> {summary['latest_day']}); "
        f"latest changed groups: {','.join(summary['latest_changed']) or 'none'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
