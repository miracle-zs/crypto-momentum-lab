#!/usr/bin/env python3
"""Analyze live trades by the server-recorded momentum-pool labels."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trades", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def as_float(value: str | None) -> float:
    return float(value or 0.0)


def as_rank(value: str | None) -> int | None:
    value = (value or "").strip()
    return int(value) if value else None


def category(label: dict[str, str]) -> str:
    gainer_rank = as_rank(label.get("gainer_rank"))
    loser_rank = as_rank(label.get("loser_rank"))
    in_gainer_top20 = gainer_rank is not None and 1 <= gainer_rank <= 20
    in_loser_top20 = loser_rank is not None and 1 <= loser_rank <= 20
    if in_gainer_top20 and in_loser_top20:
        return "both_top20"
    if in_gainer_top20:
        return "gainer_top20"
    if in_loser_top20:
        return "loser_top20"
    return "other"


def summarize(rows: list[dict[str, str]]) -> dict[str, float | int | None]:
    pnls = [as_float(row.get("net_pnl")) for row in rows]
    wins = sum(pnl > 0 for pnl in pnls)
    losses = sum(pnl < 0 for pnl in pnls)
    gross_profit = sum(pnl for pnl in pnls if pnl > 0)
    gross_loss = -sum(pnl for pnl in pnls if pnl < 0)
    return {
        "n": len(pnls),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": wins / len(pnls) * 100 if pnls else None,
        "net_pnl": sum(pnls),
        "expectancy": sum(pnls) / len(pnls) if pnls else None,
        "pf": gross_profit / gross_loss if gross_loss else None,
    }


def fmt(value: float | int | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, int):
        return str(value)
    return f"{value:.6f}"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    trades = load_csv(args.trades)
    labels = {row["entry_order_id"]: row for row in load_csv(args.labels)}

    missing = [row["entry_order_id"] for row in trades if row["entry_order_id"] not in labels]
    if missing:
        raise SystemExit(f"missing server labels for {len(missing)} trades: {missing[:5]}")

    joined: list[dict[str, str]] = []
    for trade in trades:
        label = labels[trade["entry_order_id"]]
        row = dict(trade)
        for field in (
            "snapshot_at",
            "gainer_rank",
            "loser_rank",
            "is_target",
            "membership_status",
            "membership_side",
        ):
            row[field] = label.get(field, "")
        row["label_category"] = category(label)
        joined.append(row)

    joined.sort(key=lambda row: datetime.fromisoformat(row["opened_at"]))
    split_index = int(len(joined) * 0.6)
    scopes = {
        "all": joined,
        "train60": joined[:split_index],
        "holdout40": joined[split_index:],
    }

    categories = ["gainer_top20", "loser_top20", "both_top20", "other"]
    summary_rows: list[dict[str, str]] = []
    for scope_name, scope_rows in scopes.items():
        for label_category in categories:
            rows = [row for row in scope_rows if row["label_category"] == label_category]
            stats = summarize(rows)
            summary_rows.append(
                {
                    "scope": scope_name,
                    "label_category": label_category,
                    **{key: fmt(value) for key, value in stats.items()},
                }
            )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    joined_fields = list(trades[0].keys()) if trades else []
    for field in (
        "snapshot_at",
        "gainer_rank",
        "loser_rank",
        "is_target",
        "membership_status",
        "membership_side",
        "label_category",
    ):
        if field not in joined_fields:
            joined_fields.append(field)
    write_csv(args.output_dir / "server_pool_label_trades.csv", joined, joined_fields)
    write_csv(
        args.output_dir / "server_pool_label_summary.csv",
        summary_rows,
        [
            "scope",
            "label_category",
            "n",
            "wins",
            "losses",
            "win_rate_pct",
            "net_pnl",
            "expectancy",
            "pf",
        ],
    )

    lines = [
        "# 实盘交易：动量池原始涨跌幅榜标签统计",
        "",
        "本报告直接使用服务器 `universe_entries.gainer_rank` / `loser_rank`，并按每笔开仓时间关联此前最近的 active 动量池快照；没有用 K 线重新推算榜单。",
        "",
        f"- 交易数：{len(joined)}",
        f"- 时间范围：{joined[0]['opened_at']} 至 {joined[-1]['opened_at']}" if joined else "- 时间范围：无",
        "- `gainer_top20` / `loser_top20` 只计入原始 rank 1–20。其余记录单列为 `other`，不强行归入任一榜单。",
        "",
        "| 样本 | 标签 | 笔数 | 胜率 | 净收益 | PF | 单笔期望 |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        if row["n"] == "0":
            continue
        lines.append(
            f"| {row['scope']} | {row['label_category']} | {row['n']} | "
            f"{row['win_rate_pct']}% | {row['net_pnl']} | {row['pf']} | {row['expectancy']} |"
        )
    lines.extend(
        [
            "",
            "## 口径说明",
            "",
            "`PF = 所有盈利交易之和 / 所有亏损交易绝对值之和`。`holdout40` 是按开仓时间排序后最后 40% 的交易，仅用于观察后段稳定性，不代表独立的未来样本。",
        ]
    )
    (args.output_dir / "server_pool_label_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
