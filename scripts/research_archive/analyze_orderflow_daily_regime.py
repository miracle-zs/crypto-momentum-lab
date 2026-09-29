from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from analyze_orderflow_forward_filters import (
    FROZEN_IMPULSE_CAP,
    LOCAL_TZ,
    Trade,
    fingerprint,
    load_baseline_ids,
    load_current,
    make_rules,
    metrics,
)


def local_entry_day(trade: Trade) -> str:
    return trade.opened_at.astimezone(LOCAL_TZ).date().isoformat()


def load_btc_daily(path: Path, analysis_at: datetime) -> dict[str, dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    grouped: dict[str, list[list[Any]]] = defaultdict(list)
    cutoff_ms = int(analysis_at.timestamp() * 1000)
    for bar in raw:
        open_ms = int(bar[0])
        close_ms = int(bar[6])
        if close_ms > cutoff_ms:
            continue
        opened_at = datetime.fromtimestamp(open_ms / 1000, tz=UTC)
        day = opened_at.astimezone(LOCAL_TZ).date().isoformat()
        grouped[day].append(bar)

    output: dict[str, dict[str, Any]] = {}
    analysis_day = analysis_at.astimezone(LOCAL_TZ).date().isoformat()
    for day, bars in sorted(grouped.items()):
        bars.sort(key=lambda row: int(row[0]))
        first_open = float(bars[0][1])
        last_close = float(bars[-1][4])
        output[day] = {
            "open": first_open,
            "close": last_close,
            "return_pct": (last_close / first_open - 1.0) * 100.0,
            "bar_count": len(bars),
            "partial_day": day == analysis_day,
            "last_bar_close_at": datetime.fromtimestamp(
                int(bars[-1][6]) / 1000,
                tz=UTC,
            ).isoformat(),
        }
    return output


def average_rank(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(indexed):
        end = cursor + 1
        while end < len(indexed) and indexed[end][1] == indexed[cursor][1]:
            end += 1
        rank = (cursor + 1 + end) / 2.0
        for original_index, _ in indexed[cursor:end]:
            ranks[original_index] = rank
        cursor = end
    return ranks


def correlation(values_x: list[float], values_y: list[float]) -> float | None:
    if len(values_x) < 2 or len(set(values_x)) < 2 or len(set(values_y)) < 2:
        return None
    return statistics.correlation(values_x, values_y)


def daily_rows(
    trades: list[Trade],
    baseline_ids: set[str],
    btc_daily: dict[str, dict[str, Any]],
    impulse_cap: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Trade]] = defaultdict(list)
    for trade in trades:
        grouped[local_entry_day(trade)].append(trade)
    rules = make_rules(impulse_cap)
    output: list[dict[str, Any]] = []
    for day, day_trades in sorted(grouped.items()):
        historical_count = sum(
            trade.position_id in baseline_ids for trade in day_trades
        )
        forward_count = len(day_trades) - historical_count
        if historical_count and forward_count:
            phase = "mixed"
        elif forward_count:
            phase = "forward"
        else:
            phase = "historical"
        rule_metrics = {
            rule.rule_id: metrics(
                [trade for trade in day_trades if rule.predicate(trade)],
                len(day_trades),
            )
            for rule in rules
        }
        direction = {
            side: metrics(
                [trade for trade in day_trades if trade.side == side],
                len(day_trades),
            )
            for side in ("long", "short")
        }
        output.append(
            {
                "date": day,
                "phase": phase,
                "position_count": len(day_trades),
                "historical_count": historical_count,
                "forward_count": forward_count,
                "long_signal_share": sum(
                    trade.side == "long" for trade in day_trades
                )
                / len(day_trades),
                "btc": btc_daily.get(day),
                "rules": rule_metrics,
                "direction": direction,
            }
        )
    return output


def regime_metrics(
    trades: list[Trade],
    days: list[dict[str, Any]],
    impulse_cap: float,
) -> dict[str, Any]:
    regime_by_day = {
        row["date"]: "up" if row["btc"]["return_pct"] > 0 else "down"
        for row in days
        if row["btc"] is not None
    }
    rules = make_rules(impulse_cap)
    result: dict[str, Any] = {}
    for regime in ("up", "down"):
        regime_trades = [
            trade
            for trade in trades
            if regime_by_day.get(local_entry_day(trade)) == regime
        ]
        result[regime] = {
            "day_count": sum(value == regime for value in regime_by_day.values()),
            "btc_days": [
                day for day, value in regime_by_day.items() if value == regime
            ],
            "rules": {
                rule.rule_id: metrics(
                    [trade for trade in regime_trades if rule.predicate(trade)],
                    len(regime_trades),
                )
                for rule in rules
            },
        }
    return result


def daily_correlations(days: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [row for row in days if row["btc"] is not None]
    btc_returns = [row["btc"]["return_pct"] for row in usable]
    b2_net = [row["rules"]["B2"]["net_pnl"] for row in usable]
    b2_mean = [row["rules"]["B2"]["mean_pnl"] or 0.0 for row in usable]
    long_share = [row["long_signal_share"] for row in usable]
    largest_b2_day = max(
        usable,
        key=lambda row: abs(row["rules"]["B2"]["net_pnl"]),
    )
    leave_one_out = [row for row in usable if row is not largest_b2_day]
    leave_one_out_btc = [row["btc"]["return_pct"] for row in leave_one_out]
    leave_one_out_b2_net = [
        row["rules"]["B2"]["net_pnl"] for row in leave_one_out
    ]
    leave_one_out_b2_mean = [
        row["rules"]["B2"]["mean_pnl"] or 0.0 for row in leave_one_out
    ]
    return {
        "day_count": len(usable),
        "btc_vs_B2_net_pearson": correlation(btc_returns, b2_net),
        "btc_vs_B2_mean_pearson": correlation(btc_returns, b2_mean),
        "btc_vs_B2_net_spearman": correlation(
            average_rank(btc_returns),
            average_rank(b2_net),
        ),
        "btc_vs_long_signal_share_pearson": correlation(
            btc_returns,
            long_share,
        ),
        "leave_largest_abs_B2_day_out": {
            "excluded_day": largest_b2_day["date"],
            "day_count": len(leave_one_out),
            "btc_vs_B2_net_pearson": correlation(
                leave_one_out_btc,
                leave_one_out_b2_net,
            ),
            "btc_vs_B2_mean_pearson": correlation(
                leave_one_out_btc,
                leave_one_out_b2_mean,
            ),
        },
    }


def write_daily_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "date",
        "phase",
        "position_count",
        "historical_count",
        "forward_count",
        "btc_return_pct",
        "btc_partial_day",
        "long_signal_share",
        "long_net_pnl",
        "short_net_pnl",
    ]
    for rule_id in ("B0", "B1", "B2", "B3"):
        fieldnames.extend(
            [
                f"{rule_id}_selected",
                f"{rule_id}_closed",
                f"{rule_id}_net_pnl",
                f"{rule_id}_profit_factor",
                f"{rule_id}_win_rate",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            record: dict[str, Any] = {
                "date": row["date"],
                "phase": row["phase"],
                "position_count": row["position_count"],
                "historical_count": row["historical_count"],
                "forward_count": row["forward_count"],
                "btc_return_pct": row["btc"]["return_pct"],
                "btc_partial_day": row["btc"]["partial_day"],
                "long_signal_share": row["long_signal_share"],
                "long_net_pnl": row["direction"]["long"]["net_pnl"],
                "short_net_pnl": row["direction"]["short"]["net_pnl"],
            }
            for rule_id in ("B0", "B1", "B2", "B3"):
                values = row["rules"][rule_id]
                record[f"{rule_id}_selected"] = values["selected_total"]
                record[f"{rule_id}_closed"] = values["closed_count"]
                record[f"{rule_id}_net_pnl"] = values["net_pnl"]
                record[f"{rule_id}_profit_factor"] = values["profit_factor"]
                record[f"{rule_id}_win_rate"] = values["win_rate"]
            writer.writerow(record)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare B0-B3 daily cohorts with the BTC market regime."
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
        "--btc-klines",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/forward/"
            "btcusdt-spot-1h-20260801-20260809.json"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_daily_regime_20260809.json"
        ),
    )
    parser.add_argument(
        "--daily-csv",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_daily_regime_20260809.csv"
        ),
    )
    parser.add_argument("--impulse-cap", type=float, default=FROZEN_IMPULSE_CAP)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    baseline_ids, _, cutoff = load_baseline_ids(args.baseline_positions)
    trades = load_current(args.current_trades)
    analysis_at = max(trade.updated_at for trade in trades)
    btc_daily = load_btc_daily(args.btc_klines, analysis_at)
    rows = daily_rows(trades, baseline_ids, btc_daily, args.impulse_cap)
    forward_trades = [
        trade for trade in trades if trade.position_id not in baseline_ids
    ]
    forward_rows = daily_rows(
        forward_trades,
        baseline_ids,
        btc_daily,
        args.impulse_cap,
    )
    missing_btc_days = [row["date"] for row in rows if row["btc"] is None]
    if missing_btc_days:
        raise ValueError(f"missing BTC bars for entry days: {missing_btc_days}")
    output = {
        "generated_at": datetime.now(UTC).isoformat(),
        "analysis_at": analysis_at.isoformat(),
        "frozen_cutoff": cutoff.isoformat(),
        "impulse_cap": args.impulse_cap,
        "method": {
            "daily_attribution": (
                "realized trade PnL grouped by entry date in Asia/Shanghai; this "
                "aligns each outcome with the market regime visible at entry"
            ),
            "btc_proxy": (
                "Binance spot BTCUSDT 1h bars grouped into Asia/Shanghai calendar "
                "days; only bars closed by the account snapshot are used"
            ),
            "warning": (
                "daily observations before the frozen cutoff are descriptive and "
                "in-sample for the selected B1-B3 rules"
            ),
        },
        "inputs": {
            "baseline_positions": fingerprint(args.baseline_positions),
            "current_trades": fingerprint(args.current_trades),
            "btc_klines": fingerprint(args.btc_klines),
        },
        "daily": rows,
        "forward_daily": forward_rows,
        "regimes": regime_metrics(trades, rows, args.impulse_cap),
        "forward_regimes": regime_metrics(
            forward_trades,
            forward_rows,
            args.impulse_cap,
        ),
        "correlations": daily_correlations(rows),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_daily_csv(args.daily_csv, rows)
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
