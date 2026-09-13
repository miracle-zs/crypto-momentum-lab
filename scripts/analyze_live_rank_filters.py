"""Compare live long trades from cross-sectional gain/loss leaderboards."""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

LOOKBACKS = {
    "1h": 4,
    "4h": 16,
    "12h": 48,
    "24h": 96,
}
BANDS = ("top10", "top10_to_20", "middle60", "bottom10_to_20", "bottom10")
TAIL_GROUPS = ("top20", "middle60", "bottom20")


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_candles(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            grouped[row["symbol"]].append(
                {
                    "end": parse_dt(row["candle_end"]),
                    "close": float(row["close_price"]),
                }
            )
    for candles in grouped.values():
        candles.sort(key=lambda row: row["end"])
    return grouped


def load_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                {
                    "symbol": row["symbol"],
                    "entry_order_id": row["entry_order_id"],
                    "exit_order_id": row["exit_order_id"],
                    "opened_at": parse_dt(row["opened_at"]),
                    "closed_at": row["closed_at"],
                    "entry_price": float(row["entry_price"]),
                    "exit_price": float(row["exit_price"]),
                    "net_pnl": float(row["net_pnl"]),
                    "return_pct": float(row["return_pct"]),
                    "hold_minutes": float(row["hold_minutes"]),
                    "impulse_trade_notional": float(
                        row.get("impulse_trade_notional") or 0
                    ),
                    "impulse_trade_count": float(row.get("impulse_trade_count") or 0),
                    "activity_gate": (
                        float(row.get("impulse_trade_notional") or 0) >= 50000
                        and float(row.get("impulse_trade_count") or 0) >= 200
                    ),
                }
            )
    return sorted(rows, key=lambda row: row["opened_at"])


def snapshot_returns(
    candles_by_symbol: dict[str, list[dict[str, Any]]],
    opened_at: datetime,
    bars: int,
) -> dict[str, dict[str, Any]]:
    snapshot: dict[str, dict[str, Any]] = {}
    for symbol, candles in candles_by_symbol.items():
        ends = [candle["end"] for candle in candles]
        index = bisect.bisect_right(ends, opened_at) - 1
        if index < bars:
            continue
        current = candles[index]["close"]
        previous = candles[index - bars]["close"]
        if previous <= 0:
            continue
        snapshot[symbol] = {
            "return": current / previous - 1,
            "candle_end": candles[index]["end"].isoformat(),
        }
    return snapshot


def rank_snapshot(
    snapshot: dict[str, dict[str, Any]],
) -> dict[str, tuple[int, int]]:
    ordered = sorted(
        snapshot.items(),
        key=lambda item: (-item[1]["return"], item[0]),
    )
    total = len(ordered)
    return {
        symbol: (rank, total) for rank, (symbol, _value) in enumerate(ordered, start=1)
    }


def rank_band(rank: int, total: int) -> str:
    top10 = max(1, math.ceil(total * 0.10))
    top20 = max(1, math.ceil(total * 0.20))
    bottom20_start = total - top20 + 1
    bottom10_start = total - top10 + 1
    if rank <= top10:
        return "top10"
    if rank <= top20:
        return "top10_to_20"
    if rank >= bottom10_start:
        return "bottom10"
    if rank >= bottom20_start:
        return "bottom10_to_20"
    return "middle60"


def tail_group(rank: int, total: int) -> str:
    tail = max(1, math.ceil(total * 0.20))
    if rank <= tail:
        return "top20"
    if rank > total - tail:
        return "bottom20"
    return "middle60"


def enrich_rows(
    trades: list[dict[str, Any]],
    candles_by_symbol: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for trade in trades:
        row = dict(trade)
        valid = True
        for label, bars in LOOKBACKS.items():
            snapshot = snapshot_returns(candles_by_symbol, trade["opened_at"], bars)
            ranks = rank_snapshot(snapshot)
            target = snapshot.get(trade["symbol"])
            target_rank = ranks.get(trade["symbol"])
            if target is None or target_rank is None:
                valid = False
                break
            rank, total = target_rank
            row[f"return_{label}_pct"] = target["return"] * 100
            row[f"rank_{label}"] = rank
            row[f"universe_{label}"] = total
            row[f"band_{label}"] = rank_band(rank, total)
            row[f"tail_group_{label}"] = tail_group(rank, total)
            row[f"rank_candle_end_{label}"] = target["candle_end"]
        if valid:
            enriched.append(row)
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


def grouped_summary(
    rows: list[dict[str, Any]],
    dimension: str,
    groups: tuple[str, ...],
    key_function: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for split, base in split_rows(rows).items():
        for group in groups:
            selected = [row for row in base if key_function(row) == group]
            output.append(
                {
                    "dimension": dimension,
                    "group": group,
                    "split": split,
                    **stats(selected),
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


def activity_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selections: dict[str, Callable[[dict[str, Any]], bool]] = {
        "activity_gate": lambda row: row["activity_gate"],
        "activity_and_12h_not_bottom20": lambda row: (
            row["activity_gate"] and row["tail_group_12h"] != "bottom20"
        ),
        "activity_and_12h_top20": lambda row: (
            row["activity_gate"] and row["tail_group_12h"] == "top20"
        ),
        "activity_and_24h_not_bottom20": lambda row: (
            row["activity_gate"] and row["tail_group_24h"] != "bottom20"
        ),
        "activity_and_12h_24h_not_bottom20": lambda row: (
            row["activity_gate"]
            and row["tail_group_12h"] != "bottom20"
            and row["tail_group_24h"] != "bottom20"
        ),
        "activity_and_24h_top20": lambda row: (
            row["activity_gate"] and row["tail_group_24h"] == "top20"
        ),
        "activity_and_24h_bottom20": lambda row: (
            row["activity_gate"] and row["tail_group_24h"] == "bottom20"
        ),
    }
    output: list[dict[str, Any]] = []
    for split, base in split_rows(rows).items():
        for rule, predicate in selections.items():
            selected = [row for row in base if predicate(row)]
            output.append(
                {
                    "split": split,
                    "rule": rule,
                    **stats(selected),
                }
            )
    return output


def fmt(value: float | None, digits: int = 3) -> str:
    return "NA" if value is None else f"{value:.{digits}f}"


def write_report(
    path: Path,
    rows: list[dict[str, Any]],
    tail_summary: list[dict[str, Any]],
    band_summary: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
) -> None:
    lines = [
        "# 实盘涨幅榜做多与跌幅榜做多区分度分析",
        "",
        "榜单定义：对每笔开仓，使用开仓前最近一根已收盘的官方 15m K 线，",
        "在当时可用的全部币种中按 1h/4h/12h/24h 收益率横截面排名。",
        "top20 是排名前 20%，bottom20 是排名后 20%。",
        "",
        f"有效交易：{len(rows)}。",
        "",
        "## Holdout40：top20 与 bottom20",
        "",
        "| 回看周期 | top20 笔数 | top20 胜率 | top20 PF | top20 净 PnL | "
        "bottom20 笔数 | bottom20 胜率 | bottom20 PF | bottom20 净 PnL |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in LOOKBACKS:
        top = next(
            row
            for row in tail_summary
            if row["dimension"] == label
            and row["group"] == "top20"
            and row["split"] == "holdout40"
        )
        bottom = next(
            row
            for row in tail_summary
            if row["dimension"] == label
            and row["group"] == "bottom20"
            and row["split"] == "holdout40"
        )
        lines.append(
            f"| {label} | {top['trades']} | {top['win_rate_pct']:.1f}% | "
            f"{fmt(top['profit_factor'])} | {top['net_pnl']:.2f} | "
            f"{bottom['trades']} | {bottom['win_rate_pct']:.1f}% | "
            f"{fmt(bottom['profit_factor'])} | {bottom['net_pnl']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## 24h 榜单五段分层：holdout40",
            "",
            "| 分层 | 样本 | 胜率 | 净 PnL | PF | 单笔期望 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for group in BANDS:
        item = next(
            row
            for row in band_summary
            if row["dimension"] == "24h"
            and row["group"] == group
            and row["split"] == "holdout40"
        )
        lines.append(
            f"| {group} | {item['trades']} | {item['win_rate_pct']:.1f}% | "
            f"{item['net_pnl']:.2f} | {fmt(item['profit_factor'])} | "
            f"{item['expectancy']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## 与订单流活跃度叠加：holdout40",
            "",
            (
                "活跃度条件为 impulse_trade_notional >= 50000 "
                "且 impulse_trade_count >= 200。"
            ),
            "",
            "| 规则 | 样本 | 胜率 | 净 PnL | PF | 单笔期望 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for rule in (
        "activity_gate",
        "activity_and_12h_not_bottom20",
        "activity_and_12h_top20",
        "activity_and_24h_not_bottom20",
        "activity_and_12h_24h_not_bottom20",
        "activity_and_24h_top20",
        "activity_and_24h_bottom20",
    ):
        item = next(
            row
            for row in activity_rows
            if row["split"] == "holdout40" and row["rule"] == rule
        )
        lines.append(
            f"| {rule} | {item['trades']} | {item['win_rate_pct']:.1f}% | "
            f"{item['net_pnl']:.2f} | {fmt(item['profit_factor'])} | "
            f"{item['expectancy']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## 逐笔明细",
            "",
            (
                "rank_enriched_trades.csv 包含每笔交易的榜单收益、横截面排名、"
                "榜单分组和最终收益。"
            ),
            "",
            "## 使用边界",
            "",
            "榜单是横截面描述变量，不代表因果关系；当前只有约四天样本。",
            "top/bottom 的结论应先在冻结规则的 paper/shadow 中继续前向验证。",
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

    candles = load_candles(args.klines)
    trades = load_trades(args.trades)
    enriched = enrich_rows(trades, candles)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    tail_summary: list[dict[str, Any]] = []
    band_summary: list[dict[str, Any]] = []
    for label in LOOKBACKS:
        tail_summary.extend(
            grouped_summary(
                enriched,
                label,
                TAIL_GROUPS,
                lambda row, label=label: row[f"tail_group_{label}"],
            )
        )
        band_summary.extend(
            grouped_summary(
                enriched,
                label,
                BANDS,
                lambda row, label=label: row[f"band_{label}"],
            )
        )

    activity_rows = activity_summary(enriched)
    serialised: list[dict[str, Any]] = []
    for row in enriched:
        output = dict(row)
        output["opened_at"] = output["opened_at"].isoformat()
        serialised.append(output)
    write_csv(args.output_dir / "rank_enriched_trades.csv", serialised)
    write_csv(args.output_dir / "rank_tail_summary.csv", tail_summary)
    write_csv(args.output_dir / "rank_band_summary.csv", band_summary)
    write_csv(args.output_dir / "rank_activity_summary.csv", activity_rows)
    (args.output_dir / "rank_metadata.json").write_text(
        json.dumps(
            {
                "trade_rows": len(trades),
                "enriched_rows": len(enriched),
                "symbols": len(candles),
                "lookbacks": LOOKBACKS,
                "ranking_candle": "latest closed 15m candle at or before opened_at",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    write_report(
        args.output_dir / "rank_filter_report.md",
        enriched,
        tail_summary,
        band_summary,
        activity_rows,
    )
    print(
        "Rank filter analysis completed: "
        f"trades={len(trades)} enriched={len(enriched)} symbols={len(candles)}"
    )


if __name__ == "__main__":
    main()
