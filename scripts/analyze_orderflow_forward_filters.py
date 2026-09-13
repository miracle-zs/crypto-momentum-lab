from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

TARGET_RUN = "paper-account-05-orderflow-candle15m-v1"
FROZEN_IMPULSE_CAP = 0.016109388347222524
LOCAL_TZ = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class Trade:
    position_id: str
    signal_id: str
    symbol: str
    side: str
    status: str
    opened_at: datetime
    closed_at: datetime | None
    updated_at: datetime
    impulse_return_abs: float
    realized_pnl: float | None


@dataclass(frozen=True)
class Rule:
    rule_id: str
    label: str
    description: str
    predicate: Callable[[Trade], bool]


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
        "sha256": digest.hexdigest(),
    }


def load_baseline_ids(path: Path) -> tuple[set[str], set[str], datetime]:
    rows = [row for row in read_csv(path) if row.get("run_id") == TARGET_RUN]
    if not rows:
        raise ValueError(f"no {TARGET_RUN!r} rows in {path}")
    ids = {row["position_id"] for row in rows}
    if len(ids) != len(rows):
        raise ValueError("baseline position_id values are not unique")
    open_ids = {
        row["position_id"]
        for row in rows
        if row["status"].strip().lower() != "closed"
    }
    cutoff = max(parse_dt(row["opened_at"]) for row in rows)
    return ids, open_ids, cutoff


def load_current(path: Path) -> list[Trade]:
    trades: list[Trade] = []
    for row in read_csv(path):
        status = row["status"].strip().lower()
        closed_at = parse_dt(row["closed_at"]) if row.get("closed_at") else None
        pnl = float(row["realized_pnl"]) if row.get("realized_pnl") else None
        if status == "closed" and (closed_at is None or pnl is None):
            raise ValueError(f"closed trade lacks outcome: {row['position_id']}")
        if status != "closed" and (closed_at is not None or pnl is not None):
            raise ValueError(f"open trade has realized outcome: {row['position_id']}")
        trades.append(
            Trade(
                position_id=row["position_id"],
                signal_id=row["signal_id"],
                symbol=row["symbol"],
                side=row["side"].strip().lower(),
                status=status,
                opened_at=parse_dt(row["opened_at"]),
                closed_at=closed_at,
                updated_at=parse_dt(row["updated_at"]),
                impulse_return_abs=abs(float(row["impulse_return_pct"])),
                realized_pnl=pnl,
            )
        )
    ids = {trade.position_id for trade in trades}
    if len(ids) != len(trades):
        raise ValueError("current position_id values are not unique")
    return sorted(trades, key=lambda trade: (trade.opened_at, trade.position_id))


def profit_factor(pnls: list[float]) -> float | None:
    gross_profit = sum(value for value in pnls if value > 0)
    gross_loss = -sum(value for value in pnls if value < 0)
    if gross_loss == 0:
        return math.inf if gross_profit > 0 else None
    return gross_profit / gross_loss


def payoff_ratio(pnls: list[float]) -> float | None:
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    if not wins or not losses:
        return None
    return statistics.fmean(wins) / abs(statistics.fmean(losses))


def grouped_close_events(trades: list[Trade]) -> list[tuple[datetime, float]]:
    grouped: dict[datetime, float] = defaultdict(float)
    for trade in trades:
        if trade.closed_at is not None and trade.realized_pnl is not None:
            grouped[trade.closed_at] += trade.realized_pnl
    return sorted(grouped.items())


def sequence_drawdown(trades: list[Trade]) -> float:
    cumulative = 0.0
    peak = 0.0
    maximum = 0.0
    for _, increment in grouped_close_events(trades):
        cumulative += increment
        peak = max(peak, cumulative)
        maximum = max(maximum, peak - cumulative)
    return maximum


def metrics(selected: list[Trade], forward_total: int) -> dict[str, Any]:
    closed = [trade for trade in selected if trade.status == "closed"]
    open_trades = [trade for trade in selected if trade.status != "closed"]
    pnls = [trade.realized_pnl for trade in closed if trade.realized_pnl is not None]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    sorted_winners = sorted(wins, reverse=True)
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    net = sum(pnls)
    return {
        "selected_total": len(selected),
        "closed_count": len(closed),
        "open_count": len(open_trades),
        "retention": len(selected) / forward_total if forward_total else None,
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(pnls) - len(wins) - len(losses),
        "win_rate": len(wins) / len(pnls) if pnls else None,
        "net_pnl": net,
        "mean_pnl": statistics.fmean(pnls) if pnls else None,
        "median_pnl": statistics.median(pnls) if pnls else None,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor(pnls),
        "average_win": statistics.fmean(wins) if wins else None,
        "average_loss": statistics.fmean(losses) if losses else None,
        "payoff_ratio": payoff_ratio(pnls),
        "max_realized_drawdown": sequence_drawdown(closed),
        "net_without_top_1_winner": net - sum(sorted_winners[:1]),
        "net_without_top_3_winners": net - sum(sorted_winners[:3]),
    }


def daily_metrics(selected: list[Trade]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Trade]] = defaultdict(list)
    for trade in selected:
        if trade.closed_at is not None:
            local_day = trade.closed_at.astimezone(LOCAL_TZ).date().isoformat()
            grouped[local_day].append(trade)
    output: dict[str, dict[str, Any]] = {}
    for day, trades in sorted(grouped.items()):
        pnls = [
            trade.realized_pnl
            for trade in trades
            if trade.realized_pnl is not None
        ]
        output[day] = {
            "closed_count": len(pnls),
            "net_pnl": sum(pnls),
            "profit_factor": profit_factor(pnls),
            "win_rate": sum(value > 0 for value in pnls) / len(pnls),
        }
    return output


def make_rules(cap: float) -> list[Rule]:
    return [
        Rule("B0", "B0 全方向", "全方向，不增加过滤", lambda trade: True),
        Rule(
            "B1",
            "B1 冲量上限",
            f"全方向，abs(impulse_return_pct) <= {cap:.9f}",
            lambda trade: trade.impulse_return_abs <= cap,
        ),
        Rule("B2", "B2 只做多", "只保留 long", lambda trade: trade.side == "long"),
        Rule(
            "B3",
            "B3 多头+上限",
            f"只保留 long，且 abs(impulse_return_pct) <= {cap:.9f}",
            lambda trade: trade.side == "long" and trade.impulse_return_abs <= cap,
        ),
    ]


def build_series(
    selected_by_rule: dict[str, list[Trade]],
    cutoff: datetime,
    analysis_at: datetime,
) -> list[dict[str, Any]]:
    event_times = sorted(
        {
            trade.closed_at
            for trades in selected_by_rule.values()
            for trade in trades
            if trade.closed_at is not None
        }
    )
    timeline = [cutoff, *event_times]
    if analysis_at > timeline[-1]:
        timeline.append(analysis_at)

    increments: dict[str, dict[datetime, float]] = {}
    for rule_id, trades in selected_by_rule.items():
        by_time: dict[datetime, float] = defaultdict(float)
        for trade in trades:
            if trade.closed_at is not None and trade.realized_pnl is not None:
                by_time[trade.closed_at] += trade.realized_pnl
        increments[rule_id] = by_time

    running = {rule_id: 0.0 for rule_id in selected_by_rule}
    output: list[dict[str, Any]] = []
    for timestamp in timeline:
        row: dict[str, Any] = {"timestamp": timestamp.isoformat()}
        for rule_id in selected_by_rule:
            running[rule_id] += increments[rule_id].get(timestamp, 0.0)
            row[rule_id] = running[rule_id]
        output.append(row)
    return output


def write_series_csv(path: Path, series: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["timestamp", "B0", "B1", "B2", "B3"],
        )
        writer.writeheader()
        writer.writerows(series)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate frozen B0-B3 orderflow filters on true forward trades."
    )
    parser.add_argument(
        "--baseline-positions",
        type=Path,
        default=Path("/tmp/paper_positions-complete.csv"),
    )
    parser.add_argument(
        "--current-trades",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/forward/"
            "orderflow-05-current-20260809.csv"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_forward_filters_20260809.json"
        ),
    )
    parser.add_argument(
        "--series-csv",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_forward_filters_20260809_series.csv"
        ),
    )
    parser.add_argument("--impulse-cap", type=float, default=FROZEN_IMPULSE_CAP)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    baseline_ids, baseline_open_ids, cutoff = load_baseline_ids(
        args.baseline_positions
    )
    current = load_current(args.current_trades)
    current_ids = {trade.position_id for trade in current}
    missing_old_ids = baseline_ids - current_ids
    if missing_old_ids:
        raise ValueError(
            f"current export is missing {len(missing_old_ids)} baseline positions"
        )

    forward = [trade for trade in current if trade.position_id not in baseline_ids]
    if any(trade.opened_at <= cutoff for trade in forward):
        raise ValueError("new position ID exists at or before the frozen cutoff")
    analysis_at = max(trade.updated_at for trade in current)
    rules = make_rules(args.impulse_cap)
    selected_by_rule = {
        rule.rule_id: [trade for trade in forward if rule.predicate(trade)]
        for rule in rules
    }

    result_metrics: dict[str, dict[str, Any]] = {}
    baseline_net = None
    for rule in rules:
        selected = selected_by_rule[rule.rule_id]
        rejected = [trade for trade in forward if not rule.predicate(trade)]
        values = metrics(selected, len(forward))
        rejected_values = metrics(rejected, len(forward))
        if baseline_net is None:
            baseline_net = values["net_pnl"]
        values["net_pnl_delta_vs_B0"] = values["net_pnl"] - baseline_net
        values["rejected_total"] = rejected_values["selected_total"]
        values["rejected_closed_count"] = rejected_values["closed_count"]
        values["rejected_net_pnl"] = rejected_values["net_pnl"]
        values["rejected_profit_factor"] = rejected_values["profit_factor"]
        values["rejected_win_rate"] = rejected_values["win_rate"]
        values["daily"] = daily_metrics(selected)
        result_metrics[rule.rule_id] = {
            "label": rule.label,
            "description": rule.description,
            **values,
        }

    direction_breakdown = {
        side: metrics(
            [trade for trade in forward if trade.side == side],
            len(forward),
        )
        for side in ("long", "short")
    }
    impulse_cap_breakdown = {
        side: {
            state: metrics(
                [
                    trade
                    for trade in forward
                    if trade.side == side
                    and (
                        trade.impulse_return_abs <= args.impulse_cap
                        if state == "pass"
                        else trade.impulse_return_abs > args.impulse_cap
                    )
                ],
                len(forward),
            )
            for state in ("pass", "fail")
        }
        for side in ("long", "short")
    }
    legacy_carry = [
        trade for trade in current if trade.position_id in baseline_open_ids
    ]
    series = build_series(selected_by_rule, cutoff, analysis_at)
    output = {
        "generated_at": datetime.now(UTC).isoformat(),
        "target_run": TARGET_RUN,
        "method": {
            "forward_membership": (
                "current position_id not present in the frozen local snapshot; "
                "all such entries must also be strictly after the frozen max opened_at"
            ),
            "entry_filter_inputs": "side and impulse_return_pct known at signal time",
            "exit_and_costs": (
                "realized_pnl from account 05; unchanged first opposite 15m candle "
                "exit and existing paper fee model"
            ),
            "equity_curve": (
                "cumulative realized net PnL aggregated by closed_at; every rule "
                "starts at zero on the same frozen cutoff; open positions are not "
                "marked to market"
            ),
            "counterfactual_assumption": (
                "rejected entries do not change later fills, prices, position sizing, "
                "or portfolio constraints"
            ),
        },
        "inputs": {
            "baseline_positions": fingerprint(args.baseline_positions),
            "current_trades": fingerprint(args.current_trades),
        },
        "frozen": {
            "local_position_count": len(baseline_ids),
            "local_cutoff": cutoff.isoformat(),
            "impulse_cap": args.impulse_cap,
        },
        "current": {
            "position_count": len(current),
            "analysis_at": analysis_at.isoformat(),
            "latest_opened_at": max(trade.opened_at for trade in current).isoformat(),
        },
        "forward": {
            "position_count": len(forward),
            "closed_count": sum(trade.status == "closed" for trade in forward),
            "open_count": sum(trade.status != "closed" for trade in forward),
            "first_opened_at": min(trade.opened_at for trade in forward).isoformat(),
            "last_opened_at": max(trade.opened_at for trade in forward).isoformat(),
        },
        "legacy_carry": {
            "note": (
                "positions already open at the frozen cutoff; excluded from all four "
                "filter curves because the entry decision predates implementation"
            ),
            **metrics(legacy_carry, len(legacy_carry)),
        },
        "rules": result_metrics,
        "direction_breakdown": direction_breakdown,
        "impulse_cap_breakdown": impulse_cap_breakdown,
        "series": series,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_series_csv(args.series_csv, series)
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
