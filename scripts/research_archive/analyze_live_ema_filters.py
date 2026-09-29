"""Analyze entry-time 15m EMA filters for live trades."""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

FEATURES = (
    "price_vs_ema5_pct",
    "price_vs_ema10_pct",
    "ema5_vs_ema10_pct",
    "ema5_slope_3bar_pct",
    "ema10_slope_3bar_pct",
    "entry_vs_ema5_atr",
    "entry_vs_ema10_atr",
    "entry_vs_prev_high_atr",
    "previous_candle_body_pct",
    "previous_candle_range_atr",
    "previous_candle_close_position",
)


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def as_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def ema(values: list[float], period: int) -> list[float | None]:
    output: list[float | None] = [None] * len(values)
    if len(values) < period:
        return output
    output[period - 1] = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    for index in range(period, len(values)):
        previous = output[index - 1]
        if previous is None:
            continue
        output[index] = values[index] * alpha + previous * (1 - alpha)
    return output


def load_klines(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            grouped[row["symbol"]].append(
                {
                    "start": parse_dt(row["candle_start"]),
                    "end": parse_dt(row["candle_end"]),
                    "open": float(row["open_price"]),
                    "high": float(row["high_price"]),
                    "low": float(row["low_price"]),
                    "close": float(row["close_price"]),
                    "volume": float(row["volume"]),
                }
            )
    for candles in grouped.values():
        candles.sort(key=lambda row: row["start"])
        closes = [row["close"] for row in candles]
        ema5 = ema(closes, 5)
        ema10 = ema(closes, 10)
        true_ranges: list[float] = []
        for index, candle in enumerate(candles):
            previous_close = candles[index - 1]["close"] if index else candle["close"]
            true_ranges.append(
                max(
                    candle["high"] - candle["low"],
                    abs(candle["high"] - previous_close),
                    abs(candle["low"] - previous_close),
                )
            )
        for index, candle in enumerate(candles):
            candle["ema5"] = ema5[index]
            candle["ema10"] = ema10[index]
            candle["atr14"] = (
                sum(true_ranges[index - 13 : index + 1]) / 14 if index >= 13 else None
            )
            if index >= 3 and ema5[index] and ema5[index - 3]:
                candle["ema5_slope_3bar_pct"] = ema5[index] / ema5[index - 3] - 1
            else:
                candle["ema5_slope_3bar_pct"] = None
            if index >= 3 and ema10[index] and ema10[index - 3]:
                candle["ema10_slope_3bar_pct"] = ema10[index] / ema10[index - 3] - 1
            else:
                candle["ema10_slope_3bar_pct"] = None
    return grouped


def load_trades(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        trades: list[dict[str, Any]] = []
        for row in csv.DictReader(handle):
            parsed = dict(row)
            parsed["opened_at"] = parse_dt(row["opened_at"])
            parsed["net_pnl"] = float(row["net_pnl"])
            parsed["entry_price"] = float(row["entry_price"])
            for key in (
                "impulse_trade_notional",
                "impulse_trade_count",
            ):
                parsed[key] = float(row[key])
            trades.append(parsed)
    return sorted(trades, key=lambda row: row["opened_at"])


def enrich_trades(
    trades: list[dict[str, Any]],
    candles_by_symbol: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    enriched: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for trade in trades:
        candles = candles_by_symbol.get(trade["symbol"], [])
        ends = [candle["end"] for candle in candles]
        index = bisect.bisect_right(ends, trade["opened_at"]) - 1
        if index < 0:
            skipped.append({"symbol": trade["symbol"], "reason": "no_prior_candle"})
            continue
        candle = candles[index]
        ema5 = candle["ema5"]
        ema10 = candle["ema10"]
        if ema5 is None or ema10 is None:
            skipped.append({"symbol": trade["symbol"], "reason": "ema_warmup"})
            continue
        entry_price = trade["entry_price"]
        price_vs_ema5 = entry_price / ema5 - 1
        price_vs_ema10 = entry_price / ema10 - 1
        ema_spread = ema5 / ema10 - 1
        atr14 = candle["atr14"]
        if atr14 is not None and atr14 > 0:
            entry_vs_ema5_atr = (entry_price - ema5) / atr14
            entry_vs_ema10_atr = (entry_price - ema10) / atr14
            entry_vs_prev_high_atr = (entry_price - candle["high"]) / atr14
            previous_candle_range_atr = (candle["high"] - candle["low"]) / atr14
        else:
            entry_vs_ema5_atr = None
            entry_vs_ema10_atr = None
            entry_vs_prev_high_atr = None
            previous_candle_range_atr = None
        previous_candle_body_pct = candle["close"] / candle["open"] - 1
        previous_candle_close_position = (
            (candle["close"] - candle["low"]) / (candle["high"] - candle["low"])
            if candle["high"] > candle["low"]
            else None
        )
        if entry_price >= ema5 and entry_price >= ema10:
            position = "above_both"
        elif entry_price < ema5 and entry_price < ema10:
            position = "below_both"
        else:
            position = "between"
        if ema5 >= ema10:
            alignment = "bull_aligned"
        else:
            alignment = "bear_aligned"
        if entry_price >= ema5 >= ema10:
            regime = "price_above_bull_stack"
        elif entry_price >= ema10 > ema5:
            regime = "price_above_bear_stack"
        elif ema5 > entry_price >= ema10:
            regime = "price_between_bull_stack"
        elif ema10 > entry_price >= ema5:
            regime = "price_between_bear_stack"
        elif ema5 >= ema10 > entry_price:
            regime = "price_below_bull_stack"
        else:
            regime = "price_below_bear_stack"
        output = dict(trade)
        output.update(
            {
                "ema_candle_end": candle["end"].isoformat(),
                "ema5": ema5,
                "ema10": ema10,
                "price_vs_ema5_pct": price_vs_ema5 * 100,
                "price_vs_ema10_pct": price_vs_ema10 * 100,
                "ema5_vs_ema10_pct": ema_spread * 100,
                "atr14": atr14,
                "entry_vs_ema5_atr": entry_vs_ema5_atr,
                "entry_vs_ema10_atr": entry_vs_ema10_atr,
                "entry_vs_prev_high_atr": entry_vs_prev_high_atr,
                "previous_candle_body_pct": previous_candle_body_pct * 100,
                "previous_candle_range_atr": previous_candle_range_atr,
                "previous_candle_close_position": previous_candle_close_position,
                "ema5_slope_3bar_pct": (
                    candle["ema5_slope_3bar_pct"] * 100
                    if candle["ema5_slope_3bar_pct"] is not None
                    else None
                ),
                "ema10_slope_3bar_pct": (
                    candle["ema10_slope_3bar_pct"] * 100
                    if candle["ema10_slope_3bar_pct"] is not None
                    else None
                ),
                "ema_position": position,
                "ema_alignment": alignment,
                "ema_regime": regime,
                "activity_gate": (
                    trade["impulse_trade_notional"] >= 50000
                    and trade["impulse_trade_count"] >= 200
                ),
            }
        )
        enriched.append(output)
    return enriched, skipped


def pnl_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    wins = [row for row in items if row["net_pnl"] > 0]
    losses = [row for row in items if row["net_pnl"] < 0]
    positive = sum(row["net_pnl"] for row in wins)
    negative = -sum(row["net_pnl"] for row in losses)
    return {
        "trades": len(items),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": 100 * len(wins) / len(items) if items else None,
        "net_pnl": sum(row["net_pnl"] for row in items),
        "profit_factor": positive / negative if negative else None,
        "expectancy": (
            sum(row["net_pnl"] for row in items) / len(items) if items else None
        ),
    }


def fixed_bucket(value: float | None, edges: tuple[float, ...]) -> str:
    if value is None:
        return "missing"
    for edge in edges:
        if value < edge:
            return f"<{edge:g}%"
    return f">={edges[-1]:g}%"


def grouped_rows(
    rows: list[dict[str, Any]],
    dimension: str,
    key_function: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    base_by_split = {
        "all": rows,
        "train60": rows[: int(len(rows) * 0.6)],
        "holdout40": rows[int(len(rows) * 0.6) :],
    }
    output: list[dict[str, Any]] = []
    for split, base in base_by_split.items():
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in base:
            grouped[key_function(row)].append(row)
        base_stats = pnl_stats(base)
        for bucket, items in sorted(grouped.items()):
            stats = pnl_stats(items)
            output.append(
                {
                    "dimension": dimension,
                    "bucket": bucket,
                    "split": split,
                    **stats,
                    "loss_capture_pct": (
                        100
                        * (base_stats["losses"] - stats["losses"])
                        / base_stats["losses"]
                        if base_stats["losses"]
                        else None
                    ),
                    "winner_retention_pct": (
                        100 * stats["wins"] / base_stats["wins"]
                        if base_stats["wins"]
                        else None
                    ),
                }
            )
    return output


def filter_rows(
    rows: list[dict[str, Any]],
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for split, base in (
        ("all", rows),
        ("train60", rows[: int(len(rows) * 0.6)]),
        ("holdout40", rows[int(len(rows) * 0.6) :]),
    ):
        selected = [row for row in base if predicate(row)]
        base_stats = pnl_stats(base)
        stats = pnl_stats(selected)
        output.append(
            {
                "filter": name,
                "split": split,
                **stats,
                "loss_capture_pct": (
                    100
                    * (base_stats["losses"] - stats["losses"])
                    / base_stats["losses"]
                    if base_stats["losses"]
                    else None
                ),
                "winner_retention_pct": (
                    100 * stats["wins"] / base_stats["wins"]
                    if base_stats["wins"]
                    else None
                ),
            }
        )
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def detail_rows(enriched: list[dict[str, Any]]) -> list[dict[str, Any]]:
    columns = (
        "symbol",
        "entry_order_id",
        "exit_order_id",
        "opened_at",
        "ema_candle_end",
        "entry_price",
        "ema5",
        "ema10",
        "ema_position",
        "ema_alignment",
        "ema_regime",
        "price_vs_ema5_pct",
        "price_vs_ema10_pct",
        "ema5_vs_ema10_pct",
        "exit_price",
        "closed_at",
        "net_pnl",
        "return_pct",
        "hold_minutes",
    )
    output: list[dict[str, Any]] = []
    for row in enriched:
        detail = {column: row.get(column) for column in columns}
        detail["opened_at"] = row["opened_at"].isoformat()
        detail["outcome"] = "win" if row["net_pnl"] > 0 else "loss"
        output.append(detail)
    return output


def write_report(
    path: Path,
    enriched: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    summary: list[dict[str, Any]],
    filters: list[dict[str, Any]],
) -> None:
    lines = [
        "# 实盘入场 EMA 过滤分析",
        "",
        (
            "EMA 使用入场时刻之前最近一根已收盘的 Binance 官方 15m K 线；"
            "未使用入场所在未收盘 K 线。"
        ),
        "",
        f"交易样本：{len(enriched)}；因 EMA 预热或缺少历史 K 线跳过：{len(skipped)}。",
        "逐笔关键字段明细：ema_trade_detail.csv。",
        "",
        "## 开仓价相对 EMA5/EMA10 的关系",
        "",
        (
            "上方表示开仓价同时不低于 EMA5 和 EMA10；下方表示同时低于两条均线；"
            "其余为两线之间。"
        ),
        "",
        "| 时间段 | 价格位置 | 样本 | 胜率 | 净 PnL | Profit Factor |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in summary:
        if row["dimension"] != "ema_position":
            continue
        pf = "NA" if row["profit_factor"] is None else f"{row['profit_factor']:.3f}"
        lines.append(
            f"| {row['split']} | {row['bucket']} | {row['trades']} | "
            f"{row['win_rate_pct']:.2f}% | {row['net_pnl']:.4f} | {pf} |"
        )
    lines.extend(
        [
            "",
            "## 组合状态",
            "",
            "| 状态 | 样本 | 胜率 | 净 PnL | Profit Factor |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in summary:
        if row["dimension"] != "ema_regime" or row["split"] not in {"all", "holdout40"}:
            continue
        pf = "NA" if row["profit_factor"] is None else f"{row['profit_factor']:.3f}"
        lines.append(
            f"| {row['split']} / {row['bucket']} | {row['trades']} | "
            f"{row['win_rate_pct']:.2f}% | {row['net_pnl']:.4f} | {pf} |"
        )
    lines.extend(
        [
            "",
            "## 入口过滤候选",
            "",
            "以下过滤器只使用开仓时可得字段；holdout40 是按开仓时间排序后的最后 40%。",
            "",
            "| 过滤器 | 样本 | 胜率 | 净 PnL | Profit Factor | 拦截亏损 | 保留盈利 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in filters:
        if row["split"] != "holdout40":
            continue
        pf = "NA" if row["profit_factor"] is None else f"{row['profit_factor']:.3f}"
        lines.append(
            f"| {row['filter']} | {row['trades']} | {row['win_rate_pct']:.2f}% | "
            f"{row['net_pnl']:.4f} | {pf} | {row['loss_capture_pct']:.1f}% | "
            f"{row['winner_retention_pct']:.1f}% |"
        )
    lines.extend(
        [
            "",
            "## 使用边界",
            "",
            (
                "EMA 状态是候选过滤器，不是因果证明；本样本只有四天。"
                "先在更长的时间外样本和 paper/shadow 中验证，再考虑实盘启用。"
            ),
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trades", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    candles = load_klines(args.klines)
    trades = load_trades(args.trades)
    enriched, skipped = enrich_trades(trades, candles)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summary: list[dict[str, Any]] = []
    summary.extend(
        grouped_rows(enriched, "ema_position", lambda row: row["ema_position"])
    )
    summary.extend(
        grouped_rows(enriched, "ema_alignment", lambda row: row["ema_alignment"])
    )
    summary.extend(grouped_rows(enriched, "ema_regime", lambda row: row["ema_regime"]))
    summary.extend(
        grouped_rows(
            enriched,
            "price_vs_ema5_pct",
            lambda row: fixed_bucket(
                row["price_vs_ema5_pct"], (-1, -0.25, 0, 0.25, 0.75)
            ),
        )
    )
    summary.extend(
        grouped_rows(
            enriched,
            "ema5_vs_ema10_pct",
            lambda row: fixed_bucket(row["ema5_vs_ema10_pct"], (-0.5, -0.1, 0.1, 0.5)),
        )
    )
    summary.extend(
        grouped_rows(
            enriched,
            "entry_vs_ema5_atr",
            lambda row: fixed_bucket(
                row["entry_vs_ema5_atr"], (-1, 0, 0.25, 0.5, 1, 2)
            ),
        )
    )
    summary.extend(
        grouped_rows(
            enriched,
            "entry_vs_prev_high_atr",
            lambda row: fixed_bucket(
                row["entry_vs_prev_high_atr"], (-1, 0, 0.25, 0.5, 1)
            ),
        )
    )

    filters = []
    filters.extend(
        filter_rows(
            enriched,
            "price >= EMA5 >= EMA10",
            lambda row: row["ema_regime"] == "price_above_bull_stack",
        )
    )
    filters.extend(
        filter_rows(
            enriched, "price >= EMA10", lambda row: row["price_vs_ema10_pct"] >= 0
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "EMA5 >= EMA10",
            lambda row: row["ema_alignment"] == "bull_aligned",
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "price >= EMA5 >= EMA10 and distance EMA5 <= 1%",
            lambda row: (
                row["ema_regime"] == "price_above_bull_stack"
                and row["price_vs_ema5_pct"] <= 1
            ),
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "activity gate and price >= EMA5 >= EMA10",
            lambda row: (
                row["activity_gate"] and row["ema_regime"] == "price_above_bull_stack"
            ),
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "activity gate and EMA5 >= EMA10",
            lambda row: row["activity_gate"] and row["ema_alignment"] == "bull_aligned",
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "1.0 <= (price - EMA5) / ATR14 < 2.0",
            lambda row: (
                row["entry_vs_ema5_atr"] is not None
                and 1 <= row["entry_vs_ema5_atr"] < 2
            ),
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "activity gate and 1.0 <= (price - EMA5) / ATR14 < 2.0",
            lambda row: (
                row["activity_gate"]
                and row["entry_vs_ema5_atr"] is not None
                and 1 <= row["entry_vs_ema5_atr"] < 2
            ),
        )
    )
    filters.extend(
        filter_rows(
            enriched,
            "activity gate and 0 <= (price - prev high) / ATR14 < 0.5",
            lambda row: (
                row["activity_gate"]
                and row["entry_vs_prev_high_atr"] is not None
                and 0 <= row["entry_vs_prev_high_atr"] < 0.5
            ),
        )
    )

    serialised = []
    for row in enriched:
        output = dict(row)
        output["opened_at"] = output["opened_at"].isoformat()
        serialised.append(output)
    write_csv(args.output_dir / "ema_enriched_trades.csv", serialised)
    write_csv(args.output_dir / "ema_trade_detail.csv", detail_rows(enriched))
    write_csv(args.output_dir / "ema_filter_summary.csv", summary)
    write_csv(args.output_dir / "ema_filter_candidates.csv", filters)
    (args.output_dir / "ema_filter_metadata.json").write_text(
        json.dumps(
            {
                "trade_rows": len(trades),
                "enriched_rows": len(enriched),
                "skipped_rows": len(skipped),
                "symbols": len({row["symbol"] for row in enriched}),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    write_report(
        args.output_dir / "ema_filter_report.md",
        enriched,
        skipped,
        summary,
        filters,
    )
    print(
        "EMA filter analysis completed: "
        f"enriched={len(enriched)} skipped={len(skipped)}"
    )


if __name__ == "__main__":
    main()
