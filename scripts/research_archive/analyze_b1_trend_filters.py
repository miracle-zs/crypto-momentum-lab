"""Mine multi-timeframe downtrend filters for B1 long entries."""

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
    output: list[float | None] = [None] * len(values)
    if len(values) < period:
        return output
    output[period - 1] = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    for index in range(period, len(values)):
        previous = output[index - 1]
        if previous is not None:
            output[index] = values[index] * alpha + previous * (1 - alpha)
    return output


def load_candles(path: Path) -> dict[str, list[dict[str, Any]]]:
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
        for period in (5, 10, 20, 50):
            values = ema(closes, period)
            for index, candle in enumerate(candles):
                candle[f"ema{period}"] = values[index]
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
            candle["red"] = candle["close"] < candle["open"]
        for index, candle in enumerate(candles):
            candle["atr14"] = (
                sum(true_ranges[index - 13 : index + 1]) / 14 if index >= 13 else None
            )
    return grouped


def build_features(
    candles_by_symbol: dict[str, list[dict[str, Any]]],
    *,
    symbol: str,
    opened_at: datetime,
    entry_price: float,
) -> dict[str, Any] | None:
    candles = candles_by_symbol.get(symbol, [])
    ends = [candle["end"] for candle in candles]
    index = bisect.bisect_right(ends, opened_at) - 1
    if index < 96:
        return None
    candle = candles[index]
    required = [candle[f"ema{period}"] for period in (5, 10, 20, 50)]
    if any(value is None for value in required):
        return None

    def return_from(bars_ago: int) -> float:
        return entry_price / candles[index - bars_ago]["close"] - 1

    def slope(period: int, bars: int) -> float:
        current = candle[f"ema{period}"]
        previous = candles[index - bars][f"ema{period}"]
        if current is None or previous is None or previous == 0:
            return 0.0
        return current / previous - 1

    def range_features(bars: int) -> tuple[float, float]:
        window = candles[index - bars + 1 : index + 1]
        high = max(row["high"] for row in window)
        low = min(row["low"] for row in window)
        position = (entry_price - low) / (high - low) if high > low else 0.5
        return entry_price / high - 1, position

    consecutive_red = 0
    cursor = index
    while cursor >= 0 and candles[cursor]["red"]:
        consecutive_red += 1
        cursor -= 1
    drawdown_4h, position_4h = range_features(16)
    drawdown_24h, position_24h = range_features(96)
    ret_4h = return_from(16)
    ret_12h = return_from(48)
    price_vs_ema20 = entry_price / candle["ema20"] - 1
    ema20_vs_ema50 = candle["ema20"] / candle["ema50"] - 1
    ema20_slope_4h = slope(20, 16)
    downtrend_score = sum(
        (
            ret_4h < 0,
            ret_12h < 0,
            price_vs_ema20 < 0,
            ema20_vs_ema50 < 0,
            ema20_slope_4h < 0,
            consecutive_red >= 2,
            drawdown_24h < -0.10,
        )
    )
    return {
        "ema_candle_end": candle["end"].isoformat(),
        "ret_1h_pct": (return_from(4)) * 100,
        "ret_4h_pct": ret_4h * 100,
        "ret_12h_pct": ret_12h * 100,
        "ret_24h_pct": return_from(96) * 100,
        "price_vs_ema20_pct": price_vs_ema20 * 100,
        "price_vs_ema50_pct": (entry_price / candle["ema50"] - 1) * 100,
        "ema20_vs_ema50_pct": ema20_vs_ema50 * 100,
        "ema5_slope_1h_pct": slope(5, 4) * 100,
        "ema20_slope_4h_pct": ema20_slope_4h * 100,
        "ema50_slope_4h_pct": slope(50, 16) * 100,
        "consecutive_red": consecutive_red,
        "red_count_8": sum(row["red"] for row in candles[index - 7 : index + 1]),
        "red_count_16": sum(row["red"] for row in candles[index - 15 : index + 1]),
        "drawdown_from_4h_high_pct": drawdown_4h * 100,
        "drawdown_from_24h_high_pct": drawdown_24h * 100,
        "position_in_4h_range": position_4h,
        "position_in_24h_range": position_24h,
        "downtrend_score": downtrend_score,
    }


def load_rows(path: Path, kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if kind == "paper" and (row["status"] != "closed" or not row["closed_at"]):
                continue
            parsed = {
                "symbol": row["symbol"],
                "opened_at": parse_dt(row["opened_at"]),
                "entry_price": float(row["entry_price"]),
                "net_pnl": float(
                    row["realized_pnl"] if kind == "paper" else row["net_pnl"]
                ),
            }
            if kind == "live":
                parsed["impulse_trade_notional"] = float(
                    row.get("impulse_trade_notional") or 0
                )
                parsed["impulse_trade_count"] = float(
                    row.get("impulse_trade_count") or 0
                )
                parsed["activity_gate"] = (
                    parsed["impulse_trade_notional"] >= 50000
                    and parsed["impulse_trade_count"] >= 200
                )
            rows.append(parsed)
    return sorted(rows, key=lambda row: row["opened_at"])


def enrich_rows(
    rows: list[dict[str, Any]], candles: dict[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        features = build_features(
            candles,
            symbol=row["symbol"],
            opened_at=row["opened_at"],
            entry_price=row["entry_price"],
        )
        if features is not None:
            enriched.append({**row, **features})
    return enriched


def stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [row["net_pnl"] for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_loss = -sum(losses)
    return {
        "trades": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": 100 * len(wins) / len(rows) if rows else None,
        "net_pnl": sum(pnls),
        "profit_factor": sum(wins) / gross_loss if gross_loss else None,
        "expectancy": sum(pnls) / len(rows) if rows else None,
    }


def split_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    cut = int(len(rows) * 0.6)
    return {"all": rows, "train60": rows[:cut], "holdout40": rows[cut:]}


def feature_bucket(value: float, edges: tuple[float, ...]) -> str:
    for edge in edges:
        if value < edge:
            return f"<{edge:g}"
    return f">={edges[-1]:g}"


def candidate_rows(
    rows: list[dict[str, Any]],
    dataset: str,
    rule: str,
    predicate: Callable[[dict[str, Any]], bool],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for split, base in split_rows(rows).items():
        vetoed = [row for row in base if predicate(row)]
        kept = [row for row in base if not predicate(row)]
        base_stats = stats(base)
        for subset, selected in (("vetoed", vetoed), ("kept", kept)):
            selected_stats = stats(selected)
            output.append(
                {
                    "dataset": dataset,
                    "rule": rule,
                    "split": split,
                    "subset": subset,
                    **selected_stats,
                    "loss_capture_pct": (
                        100
                        * (base_stats["losses"] - selected_stats["losses"])
                        / base_stats["losses"]
                        if base_stats["losses"]
                        else None
                    ),
                    "winner_retention_pct": (
                        100 * selected_stats["wins"] / base_stats["wins"]
                        if base_stats["wins"]
                        else None
                    ),
                }
            )
    return output


def feature_rows(
    rows: list[dict[str, Any]], dataset: str, feature: str, edges: tuple[float, ...]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for split, base in split_rows(rows).items():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in base:
            groups[feature_bucket(row[feature], edges)].append(row)
        for bucket, items in sorted(groups.items()):
            output.append(
                {
                    "dataset": dataset,
                    "feature": feature,
                    "bucket": bucket,
                    "split": split,
                    **stats(items),
                }
            )
    return output


def serialise_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        parsed = dict(row)
        parsed["opened_at"] = parsed["opened_at"].isoformat()
        output.append(parsed)
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
    enriched: dict[str, list[dict[str, Any]]],
    candidates: list[dict[str, Any]],
) -> None:
    lines = [
        "# B1 多周期趋势与下跌结构过滤研究",
        "",
        "目标：识别 15 秒订单流冲量发生在持续下跌结构中的多头假信号。",
        "所有特征均使用入场前最近一根已收盘的官方 15m K 线；",
        "没有使用入场所在未收盘 K 线。",
        "",
        "## 数据集",
        "",
        "| 数据集 | 已平仓 | holdout |",
        "|---|---:|---:|",
    ]
    for name, rows in enriched.items():
        lines.append(f"| {name} | {len(rows)} | {len(split_rows(rows)['holdout40'])} |")
    lines.extend(
        [
            "",
            "## Downtrend score",
            "",
            "每满足一项加 1 分：4h/12h 收益为负、价格低于 EMA20、EMA20 低于 EMA50、",
            "EMA20 过去 4h 斜率为负、连续至少 2 根阴线、",
            "价格较过去 24h 高点回撤超过 10%。",
            "",
            "| 数据集 | 条件 | holdout 样本 | 净 PnL | PF | 拦截亏损 | 保留盈利 |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in candidates:
        if row["split"] == "holdout40" and row["subset"] in {"vetoed", "kept"}:
            if row["rule"] != "downtrend_score >= 5":
                continue
            pf = "NA" if row["profit_factor"] is None else f"{row['profit_factor']:.3f}"
            lines.append(
                f"| {row['dataset']} | {row['subset']} | {row['trades']} | "
                f"{row['net_pnl']:.2f} | {pf} | {row['loss_capture_pct']:.1f}% | "
                f"{row['winner_retention_pct']:.1f}% |"
            )
    lines.extend(
        [
            "",
            "## Downtrend score 分层：holdout40",
            "",
            "分数越高，代表同时满足的下跌结构越多；这里按精确分数展示，便于检查阈值是否只是偶然。",
            "",
            "| 数据集 | score | 样本 | 胜率 | 净 PnL | PF |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for dataset, rows in enriched.items():
        holdout = split_rows(rows)["holdout40"]
        for score in range(8):
            result = stats([row for row in holdout if row["downtrend_score"] == score])
            if not result["trades"]:
                continue
            pf = (
                "NA"
                if result["profit_factor"] is None
                else f"{result['profit_factor']:.3f}"
            )
            lines.append(
                f"| {dataset} | {score} | {result['trades']} | "
                f"{result['win_rate_pct']:.1f}% | {result['net_pnl']:.2f} | {pf} |"
            )
    lines.extend(
        [
            "",
            "## 候选 veto 对比：holdout40",
            "",
            "| 数据集 | veto 条件 | 被 veto | 被 veto PF | 保留 | 保留 PF |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    rule_order = [
        "ret_4h <= -5%",
        "ret_12h <= -10%",
        "price <= EMA20 -5%",
        "EMA20 <= EMA50 -2%",
        "EMA20 4h slope <= -2%",
        "24h drawdown <= -20%",
        "downtrend_score >= 5",
        "4h down + 12h down + price below EMA20 + EMA20 below EMA50",
    ]
    for rule in rule_order:
        for dataset in enriched:
            veto = next(
                row
                for row in candidates
                if row["dataset"] == dataset
                and row["rule"] == rule
                and row["split"] == "holdout40"
                and row["subset"] == "vetoed"
            )
            kept = next(
                row
                for row in candidates
                if row["dataset"] == dataset
                and row["rule"] == rule
                and row["split"] == "holdout40"
                and row["subset"] == "kept"
            )
            veto_pf = (
                "NA"
                if veto["profit_factor"] is None
                else f"{veto['profit_factor']:.3f}"
            )
            kept_pf = (
                "NA"
                if kept["profit_factor"] is None
                else f"{kept['profit_factor']:.3f}"
            )
            lines.append(
                f"| {dataset} | {rule} | {veto['trades']} | {veto_pf} | "
                f"{kept['trades']} | {kept_pf} |"
            )
    lines.extend(
        [
            "",
            "## ONUSDT 入场诊断",
            "",
            (
                "| 数据集 | 入场 | PnL | 4h收益 | 12h收益 | 价格/EMA20 | "
                "EMA20/EMA50 | 连阴 | score |"
            ),
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for dataset, rows in enriched.items():
        for row in rows:
            if row["symbol"] != "ONUSDT":
                continue
            lines.append(
                f"| {dataset} | {row['opened_at'].isoformat()} | "
                f"{row['net_pnl']:.2f} | {row['ret_4h_pct']:.2f}% | "
                f"{row['ret_12h_pct']:.2f}% | {row['price_vs_ema20_pct']:.2f}% | "
                f"{row['ema20_vs_ema50_pct']:.2f}% | "
                f"{row['consecutive_red']} | {row['downtrend_score']} |"
            )
    live_rows = enriched.get("live B1", [])
    if live_rows and "activity_gate" in live_rows[0]:
        lines.extend(
            [
                "",
                "## 活跃度条件与下跌状态叠加：live B1 holdout40",
                "",
                (
                    "这里把原有活跃度条件定义为 impulse_trade_notional >= 50000 "
                    "且 impulse_trade_count >= 200，"
                ),
                "再观察排除持续下跌状态后，剩余交易的表现。",
                "",
                "| 入口条件 | 样本 | 胜率 | 净 PnL | PF | 单笔期望 |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        live_holdout = split_rows(live_rows)["holdout40"]
        selections = {
            "baseline": lambda row: True,
            "activity gate": lambda row: row["activity_gate"],
            "activity gate + score < 5": lambda row: (
                row["activity_gate"] and row["downtrend_score"] < 5
            ),
            "activity gate + score >= 5": lambda row: (
                row["activity_gate"] and row["downtrend_score"] >= 5
            ),
            "activity gate + 4h return > -5%": lambda row: (
                row["activity_gate"] and row["ret_4h_pct"] > -5
            ),
            "activity gate + 12h return > -10%": lambda row: (
                row["activity_gate"] and row["ret_12h_pct"] > -10
            ),
            "activity gate + no composite downtrend": lambda row: (
                row["activity_gate"]
                and not (
                    row["ret_4h_pct"] < 0
                    and row["ret_12h_pct"] < 0
                    and row["price_vs_ema20_pct"] < 0
                    and row["ema20_vs_ema50_pct"] < 0
                )
            ),
        }
        for rule, predicate in selections.items():
            selected = [row for row in live_holdout if predicate(row)]
            result = stats(selected)
            pf = (
                "NA"
                if result["profit_factor"] is None
                else f"{result['profit_factor']:.3f}"
            )
            expectancy = (
                "NA" if result["expectancy"] is None else f"{result['expectancy']:.3f}"
            )
            lines.append(
                f"| {rule} | {result['trades']} | {result['win_rate_pct']:.1f}% | "
                f"{result['net_pnl']:.2f} | {pf} | {expectancy} |"
            )
    lines.extend(
        [
            "",
            "## 使用边界",
            "",
            "这些特征用于发现候选 veto，不代表因果关系。",
            "实盘和 B1 虚拟盘处在相同市场时期，且入口高度重叠，",
            "不能把两者交易数相加当作独立样本。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-positions", type=Path, required=True)
    parser.add_argument("--paper-klines", type=Path, required=True)
    parser.add_argument("--live-trades", type=Path, required=True)
    parser.add_argument("--live-klines", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    datasets = {
        "B1 paper": enrich_rows(
            load_rows(args.paper_positions, "paper"), load_candles(args.paper_klines)
        ),
        "live B1": enrich_rows(
            load_rows(args.live_trades, "live"), load_candles(args.live_klines)
        ),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)

    candidate_predicates: dict[str, Callable[[dict[str, Any]], bool]] = {
        "ret_4h <= -5%": lambda row: row["ret_4h_pct"] <= -5,
        "ret_12h <= -10%": lambda row: row["ret_12h_pct"] <= -10,
        "price <= EMA20 -5%": lambda row: row["price_vs_ema20_pct"] <= -5,
        "EMA20 <= EMA50 -2%": lambda row: row["ema20_vs_ema50_pct"] <= -2,
        "EMA20 4h slope <= -2%": lambda row: row["ema20_slope_4h_pct"] <= -2,
        "24h drawdown <= -20%": lambda row: row["drawdown_from_24h_high_pct"] <= -20,
        "downtrend_score >= 5": lambda row: row["downtrend_score"] >= 5,
        "4h down + 12h down + price below EMA20 + EMA20 below EMA50": lambda row: (
            row["ret_4h_pct"] < 0
            and row["ret_12h_pct"] < 0
            and row["price_vs_ema20_pct"] < 0
            and row["ema20_vs_ema50_pct"] < 0
        ),
    }
    candidates: list[dict[str, Any]] = []
    for dataset, rows in datasets.items():
        for rule, predicate in candidate_predicates.items():
            candidates.extend(candidate_rows(rows, dataset, rule, predicate))

    feature_specs = {
        "ret_4h_pct": (-20, -10, -5, 0, 5, 10),
        "ret_12h_pct": (-20, -10, -5, 0, 5, 10),
        "ret_24h_pct": (-30, -20, -10, 0, 10),
        "price_vs_ema20_pct": (-10, -5, 0, 5, 10),
        "price_vs_ema50_pct": (-20, -10, 0, 10),
        "ema20_vs_ema50_pct": (-10, -2, 0, 2, 10),
        "ema20_slope_4h_pct": (-10, -2, 0, 2, 10),
        "drawdown_from_24h_high_pct": (-30, -20, -10, -5, 0),
        "downtrend_score": (1, 2, 3, 4, 5, 6),
        "consecutive_red": (1, 2, 3, 4, 6),
        "position_in_24h_range": (0.2, 0.4, 0.6, 0.8),
    }
    feature_summary: list[dict[str, Any]] = []
    for dataset, rows in datasets.items():
        for feature, edges in feature_specs.items():
            for split, base in split_rows(rows).items():
                groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for row in base:
                    groups[feature_bucket(row[feature], edges)].append(row)
                for bucket, items in sorted(groups.items()):
                    feature_summary.append(
                        {
                            "dataset": dataset,
                            "feature": feature,
                            "bucket": bucket,
                            "split": split,
                            **stats(items),
                        }
                    )

    for dataset, rows in datasets.items():
        write_csv(
            args.output_dir / f"{dataset.lower().replace(' ', '_')}_trend_enriched.csv",
            serialise_rows(rows),
        )
    write_csv(args.output_dir / "trend_candidate_summary.csv", candidates)
    write_csv(args.output_dir / "trend_feature_summary.csv", feature_summary)
    live_rows = datasets.get("live B1", [])
    if live_rows and "activity_gate" in live_rows[0]:
        selections = {
            "baseline": lambda row: True,
            "activity_gate": lambda row: row["activity_gate"],
            "activity_gate_and_score_lt_5": lambda row: (
                row["activity_gate"] and row["downtrend_score"] < 5
            ),
            "activity_gate_and_score_gte_5": lambda row: (
                row["activity_gate"] and row["downtrend_score"] >= 5
            ),
            "activity_gate_and_ret_4h_gt_-5pct": lambda row: (
                row["activity_gate"] and row["ret_4h_pct"] > -5
            ),
            "activity_gate_and_ret_12h_gt_-10pct": lambda row: (
                row["activity_gate"] and row["ret_12h_pct"] > -10
            ),
            "activity_gate_and_no_composite_downtrend": lambda row: (
                row["activity_gate"]
                and not (
                    row["ret_4h_pct"] < 0
                    and row["ret_12h_pct"] < 0
                    and row["price_vs_ema20_pct"] < 0
                    and row["ema20_vs_ema50_pct"] < 0
                )
            ),
        }
        activity_summary = []
        for split, base in split_rows(live_rows).items():
            for rule, predicate in selections.items():
                selected = [row for row in base if predicate(row)]
                activity_summary.append(
                    {
                        "dataset": "live B1",
                        "split": split,
                        "rule": rule,
                        **stats(selected),
                    }
                )
        write_csv(args.output_dir / "trend_activity_summary.csv", activity_summary)
    (args.output_dir / "trend_metadata.json").write_text(
        json.dumps(
            {
                dataset: {
                    "rows": len(rows),
                    "symbols": len({r["symbol"] for r in rows}),
                }
                for dataset, rows in datasets.items()
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    write_report(args.output_dir / "trend_filter_report.md", datasets, candidates)
    print(
        "Trend filter analysis completed: "
        + ", ".join(f"{dataset}={len(rows)}" for dataset, rows in datasets.items())
    )


if __name__ == "__main__":
    main()
