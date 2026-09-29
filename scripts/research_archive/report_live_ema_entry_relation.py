#!/usr/bin/env python3
"""Report live trades by entry price relative to 15m EMA5 and EMA10."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

from analyze_live_ema_filters import enrich_trades, load_klines, load_trades, pnl_stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trades", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def bucket(row: dict[str, Any]) -> str:
    above_ema5 = row["price_above_ema5"]
    above_ema10 = row["price_above_ema10"]
    if above_ema5 and above_ema10:
        return "above_both"
    if above_ema5 and not above_ema10:
        return "above_ema5_only"
    if not above_ema5 and above_ema10:
        return "above_ema10_only"
    return "below_both"


def add_relations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["price_above_ema5"] = row["entry_price"] >= row["ema5"]
        enriched["price_above_ema10"] = row["entry_price"] >= row["ema10"]
        enriched["ema_price_bucket"] = bucket(enriched)
        enriched["result"] = "win" if row["net_pnl"] > 0 else "loss"
        output.append(enriched)
    return output


def serialise_detail(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    columns = (
        "symbol",
        "entry_order_id",
        "exit_order_id",
        "opened_at",
        "ema_candle_end",
        "entry_price",
        "ema5",
        "ema10",
        "price_vs_ema5_pct",
        "price_vs_ema10_pct",
        "ema5_vs_ema10_pct",
        "price_above_ema5",
        "price_above_ema10",
        "ema_price_bucket",
        "ema_alignment",
        "net_pnl",
        "return_pct",
        "closed_at",
        "hold_minutes",
        "entry_reason",
        "exit_reason",
        "result",
    )
    output: list[dict[str, Any]] = []
    for rank, row in enumerate(
        sorted(rows, key=lambda item: item["net_pnl"], reverse=True),
        start=1,
    ):
        item = {column: row.get(column) for column in columns}
        item["pnl_rank"] = rank
        item["opened_at"] = row["opened_at"].isoformat()
        output.append(item)
    ordered = ["pnl_rank", *columns]
    return [{column: item.get(column) for column in ordered} for item in output]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)


def summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chronological = sorted(rows, key=lambda row: row["opened_at"])
    split_index = int(len(chronological) * 0.6)
    scopes = {
        "all": chronological,
        "train60": chronological[:split_index],
        "holdout40": chronological[split_index:],
    }
    dimensions = (
        ("price_vs_ema5", lambda row: "above_ema5" if row["price_above_ema5"] else "below_ema5"),
        ("price_vs_ema10", lambda row: "above_ema10" if row["price_above_ema10"] else "below_ema10"),
        ("ema_price_bucket", lambda row: row["ema_price_bucket"]),
    )
    output: list[dict[str, Any]] = []
    for scope, scope_rows in scopes.items():
        for dimension, key_function in dimensions:
            buckets: dict[str, list[dict[str, Any]]] = {}
            for row in scope_rows:
                key = key_function(row)
                buckets.setdefault(key, []).append(row)
            for key, items in buckets.items():
                stats = pnl_stats(items)
                output.append(
                    {
                        "scope": scope,
                        "dimension": dimension,
                        "bucket": key,
                        **stats,
                    }
                )
    return output


def fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_report(
    path: Path,
    rows: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
) -> None:
    sorted_rows = sorted(rows, key=lambda row: row["net_pnl"], reverse=True)
    lines = [
        "# 实盘逐笔开仓价与 15m EMA5/EMA10 关系",
        "",
        "EMA 取开仓时刻之前最近一根已收盘的 Binance 官方 15m K 线；未使用开仓所在的未收盘 K 线。EMA 初值采用对应周期收盘价的 SMA，之后按标准 EMA 递推。",
        "",
        f"有效交易：{len(rows)}；跳过：{len(skipped)}。净收益使用 `net_pnl`（已包含交易费用）。",
        "`price_above_ema5` / `price_above_ema10` 使用开仓价 >= 对应 EMA 作为“在均线之上”。完整逐笔结果按净收益降序保存在 `ema_entry_relation_sorted_by_pnl.csv`。",
        "",
        "## 分组统计",
        "",
        "| 样本 | 维度 | 分组 | 笔数 | 胜率 | 净收益 | PF | 单笔期望 |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        if row["scope"] not in {"all", "holdout40"}:
            continue
        lines.append(
            f"| {row['scope']} | {row['dimension']} | {row['bucket']} | {row['trades']} | "
            f"{fmt(row['win_rate_pct'])}% | {fmt(row['net_pnl'])} | "
            f"{fmt(row['profit_factor'])} | {fmt(row['expectancy'])} |"
        )
    lines.extend(["", "## 净收益最高的 10 笔", "", "| 排名 | 标的 | 开仓时间 | 开仓价 | EMA5 | EMA10 | 净收益 |", "|---:|---|---|---:|---:|---:|---:|"])
    for rank, row in enumerate(sorted_rows[:10], start=1):
        lines.append(
            f"| {rank} | {row['symbol']} | {row['opened_at'].isoformat()} | "
            f"{row['entry_price']:.10g} | {row['ema5']:.10g} | {row['ema10']:.10g} | "
            f"{row['net_pnl']:.6f} |"
        )
    lines.extend(["", "## 净收益最低的 10 笔", "", "| 排名 | 标的 | 开仓时间 | 开仓价 | EMA5 | EMA10 | 净收益 |", "|---:|---|---|---:|---:|---:|---:|"])
    for rank, row in enumerate(sorted_rows[-10:], start=len(sorted_rows) - 9):
        lines.append(
            f"| {rank} | {row['symbol']} | {row['opened_at'].isoformat()} | "
            f"{row['entry_price']:.10g} | {row['ema5']:.10g} | {row['ema10']:.10g} | "
            f"{row['net_pnl']:.6f} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    candles = load_klines(args.klines)
    trades = load_trades(args.trades)
    enriched, skipped = enrich_trades(trades, candles)
    rows = add_relations(enriched)
    summaries = summary_rows(rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    write_csv(
        args.output_dir / "ema_entry_relation_sorted_by_pnl.csv",
        serialise_detail(rows),
    )
    write_csv(args.output_dir / "ema_entry_relation_summary.csv", summaries)
    write_report(
        args.output_dir / "ema_entry_relation_report.md",
        rows,
        skipped,
        summaries,
    )
    print(f"EMA entry relation report completed: rows={len(rows)} skipped={len(skipped)}")


if __name__ == "__main__":
    main()
