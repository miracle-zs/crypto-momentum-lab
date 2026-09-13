"""Analyze entry-time EMA/ATR filters for the B1 paper account."""

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


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def ema(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    result[period - 1] = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    for index in range(period, len(values)):
        previous = result[index - 1]
        if previous is not None:
            result[index] = values[index] * alpha + previous * (1 - alpha)
    return result


def load_klines(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            grouped[row["symbol"]].append(
                {
                    "end": parse_dt(row["candle_end"]),
                    "open": float(row["open_price"]),
                    "high": float(row["high_price"]),
                    "low": float(row["low_price"]),
                    "close": float(row["close_price"]),
                }
            )

    for candles in grouped.values():
        candles.sort(key=lambda row: row["end"])
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
    return grouped


def load_positions(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = []
        for row in csv.DictReader(handle):
            if row["status"] != "closed" or not row["closed_at"]:
                continue
            parsed = dict(row)
            parsed["opened_at"] = parse_dt(row["opened_at"])
            parsed["net_pnl"] = float(row["realized_pnl"])
            parsed["entry_price"] = float(row["entry_price"])
            rows.append(parsed)
    return sorted(rows, key=lambda row: row["opened_at"])


def enrich(
    positions: list[dict[str, Any]],
    candles_by_symbol: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    enriched: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for position in positions:
        candles = candles_by_symbol.get(position["symbol"], [])
        ends = [candle["end"] for candle in candles]
        index = bisect.bisect_right(ends, position["opened_at"]) - 1
        if index < 0:
            skipped.append({"symbol": position["symbol"], "reason": "no_prior_candle"})
            continue
        candle = candles[index]
        ema5 = candle["ema5"]
        ema10 = candle["ema10"]
        atr14 = candle["atr14"]
        if ema5 is None or ema10 is None or atr14 is None or atr14 <= 0:
            skipped.append({"symbol": position["symbol"], "reason": "indicator_warmup"})
            continue

        price = position["entry_price"]
        d5 = (price - ema5) / atr14
        d10 = (price - ema10) / atr14
        prev_high = (price - candle["high"]) / atr14
        spread = (ema5 / ema10 - 1) * 100
        if price >= ema5 >= ema10:
            regime = "price_above_bull_stack"
        elif price >= ema10 > ema5:
            regime = "price_above_bear_stack"
        elif ema5 > price >= ema10:
            regime = "price_between_bull_stack"
        elif ema10 > price >= ema5:
            regime = "price_between_bear_stack"
        elif ema5 >= ema10 > price:
            regime = "price_below_bull_stack"
        else:
            regime = "price_below_bear_stack"

        output = dict(position)
        output.update(
            {
                "ema_candle_end": candle["end"].isoformat(),
                "ema5": ema5,
                "ema10": ema10,
                "atr14": atr14,
                "price_vs_ema5_pct": (price / ema5 - 1) * 100,
                "price_vs_ema10_pct": (price / ema10 - 1) * 100,
                "ema5_vs_ema10_pct": spread,
                "entry_vs_ema5_atr": d5,
                "entry_vs_ema10_atr": d10,
                "entry_vs_prev_high_atr": prev_high,
                "ema_position": (
                    "above_both"
                    if price >= ema5 and price >= ema10
                    else "below_both"
                    if price < ema5 and price < ema10
                    else "between"
                ),
                "ema_alignment": "bull_aligned" if ema5 >= ema10 else "bear_aligned",
                "ema_regime": regime,
            }
        )
        enriched.append(output)
    return enriched, skipped


def stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [row["net_pnl"] for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_loss = -sum(losses)
    return {
        "trades": len(rows),
        "win_rate_pct": 100 * len(wins) / len(rows) if rows else None,
        "net_pnl": sum(pnls),
        "profit_factor": sum(wins) / gross_loss if gross_loss else None,
        "expectancy": sum(pnls) / len(rows) if rows else None,
    }


def splits(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    cut = int(len(rows) * 0.6)
    return {"all": rows, "train60": rows[:cut], "holdout40": rows[cut:]}


def grouped_summary(
    rows: list[dict[str, Any]],
    dimension: str,
    key: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for split, base in splits(rows).items():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in base:
            groups[key(row)].append(row)
        for bucket, items in sorted(groups.items()):
            output.append(
                {
                    "dimension": dimension,
                    "bucket": bucket,
                    "split": split,
                    **stats(items),
                }
            )
    return output


def filter_summary(
    rows: list[dict[str, Any]],
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for split, base in splits(rows).items():
        selected = [row for row in base if predicate(row)]
        output.append({"filter": name, "split": split, **stats(selected)})
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    rows: list[dict[str, Any]],
    skipped: list[dict[str, str]],
    summary: list[dict[str, Any]],
    filters: list[dict[str, Any]],
) -> None:
    lines = [
        "# B1 虚拟账户 EMA/ATR 入场过滤分析",
        "",
        "账户：`paper-account-12-orderflow-b1-long-candle15m-v1`。",
        "EMA/ATR 均使用入场时刻之前最近一根已收盘的 Binance 官方 15m K 线；",
        "未使用当前未收盘 K 线。",
        "",
        f"已平仓样本：{len(rows)}；因指标预热或缺少 K 线跳过：{len(skipped)}。",
        (
            f"入场时间：{rows[0]['opened_at'].isoformat()} 至 "
            f"{rows[-1]['opened_at'].isoformat()}。"
        ),
        "",
        "## 组合状态",
        "",
        "| 状态 | 样本 | 胜率 | 净 PnL | Profit Factor |",
        "|---|---:|---:|---:|---:|",
    ]
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
            "## 过滤候选",
            "",
            "holdout40 为按入场时间排序后的最后 40%。",
            "",
            "| 过滤器 | 样本 | 胜率 | 净 PnL | Profit Factor |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in filters:
        if row["split"] != "holdout40":
            continue
        pf = "NA" if row["profit_factor"] is None else f"{row['profit_factor']:.3f}"
        lines.append(
            f"| {row['filter']} | {row['trades']} | {row['win_rate_pct']:.2f}% | "
            f"{row['net_pnl']:.4f} | {pf} |"
        )
    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "这是 B1 账户的直接验证，但样本仍来自短时间窗口；",
            "任何 EMA/ATR 条件都只能先进入 shadow/paper，不能根据本报告直接修改实盘。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--positions", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    positions = load_positions(args.positions)
    candles = load_klines(args.klines)
    enriched, skipped = enrich(positions, candles)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summary: list[dict[str, Any]] = []
    summary.extend(
        grouped_summary(enriched, "ema_regime", lambda row: row["ema_regime"])
    )
    summary.extend(
        grouped_summary(enriched, "ema_alignment", lambda row: row["ema_alignment"])
    )
    filters: list[dict[str, Any]] = []
    predicates = {
        "price >= EMA5 >= EMA10": lambda row: (
            row["ema_regime"] == "price_above_bull_stack"
        ),
        "price >= EMA10": lambda row: row["price_vs_ema10_pct"] >= 0,
        "EMA5 >= EMA10": lambda row: row["ema_alignment"] == "bull_aligned",
        "1.0 <= (price - EMA5) / ATR14 < 2.0": lambda row: (
            1 <= row["entry_vs_ema5_atr"] < 2
        ),
        "1.5 <= (price - EMA5) / ATR14 < 2.0": lambda row: (
            1.5 <= row["entry_vs_ema5_atr"] < 2
        ),
        "2.0 <= (price - EMA5) / ATR14 < 3.0": lambda row: (
            2 <= row["entry_vs_ema5_atr"] < 3
        ),
        "0 <= (price - prev high) / ATR14 < 0.5": lambda row: (
            0 <= row["entry_vs_prev_high_atr"] < 0.5
        ),
        "1.0 <= distance < 2.0 and EMA5 >= EMA10": lambda row: (
            1 <= row["entry_vs_ema5_atr"] < 2 and row["ema_alignment"] == "bull_aligned"
        ),
        "exclude price_between_bear_stack": lambda row: (
            row["ema_regime"] != "price_between_bear_stack"
        ),
    }
    for name, predicate in predicates.items():
        filters.extend(filter_summary(enriched, name, predicate))

    serialised: list[dict[str, Any]] = []
    for row in enriched:
        output = dict(row)
        output["opened_at"] = output["opened_at"].isoformat()
        serialised.append(output)
    write_csv(args.output_dir / "b1_ema_enriched_positions.csv", serialised)
    write_csv(args.output_dir / "b1_ema_summary.csv", summary)
    write_csv(args.output_dir / "b1_ema_filter_candidates.csv", filters)
    (args.output_dir / "b1_ema_metadata.json").write_text(
        json.dumps(
            {
                "position_rows": len(positions),
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
        args.output_dir / "b1_ema_filter_report.md",
        enriched,
        skipped,
        summary,
        filters,
    )
    print(f"B1 EMA analysis completed: enriched={len(enriched)} skipped={len(skipped)}")


if __name__ == "__main__":
    main()
