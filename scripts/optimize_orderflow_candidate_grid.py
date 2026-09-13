#!/usr/bin/env python3
"""Grid-search the threshold parameters of an orderflow_impulse event pool.

The input is the relaxed candidate-event table produced by
``build_orderflow_candidate_dataset.py``.  The event windows stay fixed at
the current 45s impulse / 60s baseline / 60s breakout definition in this
first pass.  The scan varies only entry thresholds and per-symbol cooldown.

Labels use the first future bearish 15-minute candle close, not a fixed
take-profit or stop-loss.  The primary metrics require the full eight-candle
label window to be archived, matching the earlier 57-trade control.  Events
where a bearish candle is already visible but the full label window is not
yet complete are reported separately as observed exits.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


DEFAULT_INPUT = Path(
    "server_exports/cml-orderflow-optimization-20260831-20260902/"
    "candidate-pool.csv.gz"
)
DEFAULT_OUTPUT = Path(
    "server_exports/cml-orderflow-optimization-20260831-20260902/grid"
)
FEE_ROUND_TRIP = 0.0008
NOTIONAL = 100.0
INITIAL_EQUITY = 1_000.0
BUCKET = timedelta(seconds=15)
CANDLE = timedelta(minutes=15)


@dataclass(frozen=True, slots=True)
class Event:
    symbol: str
    detected_at: datetime
    signal_at: datetime
    entry_price: float
    impulse_return: float
    imbalance: float
    intensity: float
    breakout_level: float
    confirm_price: float | None
    confirm_imbalance: float | None
    label_status: str
    exit_return: float | None
    exit_at: datetime | None
    green_run: int | None
    green_run_return: float | None


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(UTC)


def float_or_none(value: str) -> float | None:
    if value in ("", "None", "null"):
        return None
    return float(value)


def int_or_none(value: str) -> int | None:
    if value in ("", "None", "null"):
        return None
    return int(value)


def load_events(path: Path) -> tuple[list[Event], int]:
    events: list[Event] = []
    source_rows = 0
    with gzip.open(path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            source_rows += 1
            if row["direction"] != "up" or row["entry_pool_pass"] != "True":
                continue
            first_start = parse_datetime(row["first_future_candle_start"])
            bearish_offset = int_or_none(row["first_bearish_offset_15m"])
            exit_at = (
                first_start + CANDLE * (bearish_offset + 1)
                if bearish_offset is not None
                else None
            )
            events.append(
                Event(
                    symbol=row["symbol"],
                    detected_at=parse_datetime(row["event_detected_at"]),
                    signal_at=parse_datetime(row["signal_at"]),
                    entry_price=float(row["entry_price"]),
                    impulse_return=float(row["impulse_return_pct"]),
                    imbalance=float(row["aggressive_imbalance"]),
                    intensity=float(row["notional_intensity"]),
                    breakout_level=float(row["breakout_level"]),
                    confirm_price=float_or_none(row["confirm_1_price"]),
                    confirm_imbalance=float_or_none(
                        row["confirm_1_imbalance"]
                    ),
                    label_status=row["label_status"],
                    exit_return=float_or_none(row["decision_exit_return_pct"]),
                    exit_at=exit_at,
                    green_run=int_or_none(row["green_run_from_first"]),
                    green_run_return=float_or_none(
                        row["green_run_from_first_return_pct"]
                    ),
                )
            )
    events.sort(key=lambda item: (item.detected_at, item.symbol))
    return events, source_rows


def selected_events(
    events: list[Event],
    *,
    min_return: float,
    min_imbalance: float,
    min_intensity: float,
    cooldown_buckets: int,
) -> list[Event]:
    """Apply the event gate and per-symbol cooldown in replay order."""

    last_selected: dict[str, datetime] = {}
    selected: list[Event] = []
    cooldown = BUCKET * cooldown_buckets
    for event in events:
        if event.impulse_return < min_return:
            continue
        if event.imbalance < min_imbalance:
            continue
        if event.intensity < min_intensity:
            continue
        if event.confirm_price is None or event.confirm_imbalance is None:
            continue
        # confirmation_buckets=1 in this pass: the detection bucket itself
        # must remain beyond breakout and satisfy the imbalance threshold.
        if event.confirm_price <= event.breakout_level:
            continue
        if event.confirm_imbalance < min_imbalance:
            continue
        previous = last_selected.get(event.symbol)
        if previous is not None and event.detected_at <= previous + cooldown:
            continue
        selected.append(event)
        last_selected[event.symbol] = event.detected_at
    return selected


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def realized(events: list[Event]) -> dict[str, object]:
    observed_exit = [event for event in events if event.exit_return is not None]
    mature = [
        event
        for event in observed_exit
        if event.label_status == "labeled"
    ]
    pending = len(events) - len(mature)
    gross = [float(event.exit_return) for event in mature]
    net = [value - FEE_ROUND_TRIP for value in gross]
    positive = [value for value in net if value > 0]
    negative = [value for value in net if value <= 0]

    grouped_pnl: defaultdict[datetime, float] = defaultdict(float)
    for event, value in zip(mature, net, strict=True):
        if event.exit_at is not None:
            grouped_pnl[event.exit_at] += NOTIONAL * value
    equity = INITIAL_EQUITY
    peak = equity
    max_drawdown = 0.0
    for timestamp in sorted(grouped_pnl):
        equity += grouped_pnl[timestamp]
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    tail_2 = [event for event in mature if (event.green_run or 0) >= 2]
    tail_3 = [event for event in mature if (event.green_run or 0) >= 3]
    tail_returns = [
        float(event.green_run_return)
        for event in tail_2
        if event.green_run_return is not None
    ]
    ordered_gross = sorted(gross, reverse=True)
    top_decile_count = max(1, math.ceil(len(ordered_gross) * 0.10))
    top_decile_sum = sum(ordered_gross[:top_decile_count])
    total_gross = sum(gross)

    return {
        "events": len(events),
        "mature_trades": len(mature),
        "observed_exit_trades": len(observed_exit),
        "observed_exit_not_full_label": len(observed_exit) - len(mature),
        "pending_or_censored": pending,
        "wins_net": sum(value > 0 for value in net),
        "losses_or_fee_breakeven": sum(value <= 0 for value in net),
        "win_rate_pct": (
            sum(value > 0 for value in net) / len(net) * 100.0 if net else None
        ),
        "gross_pnl_usdt": sum(gross) * NOTIONAL,
        "net_pnl_usdt": sum(net) * NOTIONAL,
        "mean_gross_pct": statistics.fmean(gross) * 100.0 if gross else None,
        "mean_net_pct": statistics.fmean(net) * 100.0 if net else None,
        "median_net_pct": statistics.median(net) * 100.0 if net else None,
        "profit_factor": (
            sum(positive) / abs(sum(negative)) if negative else None
        ),
        "max_drawdown_usdt": max_drawdown,
        "max_drawdown_pct": max_drawdown / INITIAL_EQUITY * 100.0,
        "tail_2_green_events": len(tail_2),
        "tail_3_green_events": len(tail_3),
        "tail_2_event_share_pct": (
            len(tail_2) / len(mature) * 100.0 if mature else None
        ),
        "tail_2_green_run_gross_pnl_usdt": sum(tail_returns) * NOTIONAL,
        "top_decile_gross_share_pct": (
            top_decile_sum / total_gross * 100.0 if total_gross > 0 else None
        ),
    }


def split_metrics(events: list[Event], validation_start: datetime) -> dict[str, object]:
    train = [
        event
        for event in events
        if event.signal_at < validation_start
        and event.exit_at is not None
        and event.exit_at <= validation_start
    ]
    validation = [event for event in events if event.signal_at >= validation_start]
    return {
        "train": realized(train),
        "validation": realized(validation),
        "validation_start": validation_start.isoformat(),
    }


def flat_row(
    *,
    min_return: float,
    min_imbalance: float,
    min_intensity: float,
    cooldown_buckets: int,
    all_metrics: dict[str, object],
    split: dict[str, object],
) -> dict[str, object]:
    row: dict[str, object] = {
        "min_return_pct": min_return,
        "min_aggressive_imbalance": min_imbalance,
        "min_notional_intensity": min_intensity,
        "confirmation_buckets": 1,
        "cooldown_buckets": cooldown_buckets,
    }
    row.update({f"all_{key}": value for key, value in all_metrics.items()})
    for partition in ("train", "validation"):
        row.update(
            {
                f"{partition}_{key}": value
                for key, value in split[partition].items()
            }
        )
    return row


def sort_key(row: dict[str, object], partition: str) -> tuple[float, float, int]:
    pnl = row.get(f"{partition}_net_pnl_usdt")
    trades = row.get(f"{partition}_mature_trades")
    pf = row.get(f"{partition}_profit_factor")
    return (
        float(pnl) if pnl is not None else float("-inf"),
        float(pf) if pf is not None else float("-inf"),
        int(trades or 0),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--validation-start",
        default="2026-09-02T00:00:00+08:00",
        help="Shanghai/local time at which the holdout segment starts",
    )
    parser.add_argument(
        "--min-train-trades",
        type=int,
        default=8,
        help="minimum mature trades required in both train and validation",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(args.input)
    validation_start = parse_datetime(args.validation_start)
    events, source_rows = load_events(args.input)
    if not events:
        raise SystemExit("no Top10 up events in candidate table")

    returns = (0.005, 0.0075, 0.010, 0.0125, 0.015, 0.020, 0.025)
    imbalances = (0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60)
    intensities = (1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0)
    cooldowns = (0, 1, 2, 4)

    rows: list[dict[str, object]] = []
    control: dict[str, object] | None = None
    for min_return in returns:
        for min_imbalance in imbalances:
            for min_intensity in intensities:
                for cooldown_buckets in cooldowns:
                    selected = selected_events(
                        events,
                        min_return=min_return,
                        min_imbalance=min_imbalance,
                        min_intensity=min_intensity,
                        cooldown_buckets=cooldown_buckets,
                    )
                    all_metrics = realized(selected)
                    split = split_metrics(selected, validation_start)
                    row = flat_row(
                        min_return=min_return,
                        min_imbalance=min_imbalance,
                        min_intensity=min_intensity,
                        cooldown_buckets=cooldown_buckets,
                        all_metrics=all_metrics,
                        split=split,
                    )
                    rows.append(row)
                    if (
                        min_return == 0.010
                        and min_imbalance == 0.40
                        and min_intensity == 2.0
                        and cooldown_buckets == 0
                    ):
                        control = row
    assert control is not None

    shortlist = [
        row
        for row in rows
        if int(row["validation_mature_trades"] or 0) >= args.min_train_trades
    ]
    validation_ranked = sorted(
        shortlist,
        key=lambda row: sort_key(row, "validation"),
        reverse=True,
    )
    all_ranked = sorted(rows, key=lambda row: sort_key(row, "all"), reverse=True)
    control_all_pnl = float(control["all_net_pnl_usdt"])
    control_validation_pnl = float(control["validation_net_pnl_usdt"])
    robust = [
        row
        for row in shortlist
        if float(row["train_net_pnl_usdt"]) >= 0.0
        and float(row["validation_net_pnl_usdt"]) >= control_validation_pnl
        and float(row["all_net_pnl_usdt"]) >= control_all_pnl
    ]
    robust_ranked = sorted(
        robust,
        key=lambda row: (
            min(
                float(row["train_net_pnl_usdt"]),
                float(row["validation_net_pnl_usdt"]),
            ),
            float(row["all_net_pnl_usdt"]),
            float(row["validation_profit_factor"] or float("-inf")),
        ),
        reverse=True,
    )
    all_with_validation = [
        row
        for row in shortlist
        if float(row["validation_net_pnl_usdt"]) >= control_validation_pnl
    ]
    all_with_validation_ranked = sorted(
        all_with_validation,
        key=lambda row: sort_key(row, "all"),
        reverse=True,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    with (args.output_dir / "grid_results.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "input": str(args.input),
        "source_rows": source_rows,
        "top10_up_events": len(events),
        "fee_round_trip_pct": FEE_ROUND_TRIP,
        "notional_usdt": NOTIONAL,
        "exit_definition": "first future bearish 15m candle close",
        "fixed_event_windows": {
            "impulse_buckets": 3,
            "baseline_buckets": 4,
            "breakout_buckets": 4,
        },
        "grid": {
            "min_return_pct": returns,
            "min_aggressive_imbalance": imbalances,
            "min_notional_intensity": intensities,
            "cooldown_buckets": cooldowns,
            "confirmation_buckets": [1],
            "variants": len(rows),
        },
        "control": control,
        "validation_start": validation_start.isoformat(),
        "shortlist_min_validation_mature_trades": args.min_train_trades,
        "robust_definition": (
            "train net pnl >= 0, validation net pnl >= control, "
            "all-period net pnl >= control, with minimum mature trades in "
            "both partitions"
        ),
        "top_robust": robust_ranked[:20],
        "top_validation": validation_ranked[:20],
        "top_all_period": all_ranked[:20],
        "top_all_with_validation_at_least_control": all_with_validation_ranked[
            :20
        ],
    }
    (args.output_dir / "grid_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    lines = [
        "# orderflow_impulse 第一阶段参数网格",
        "",
        "- 时间窗口：2026-08-31 14:00 至 2026-09-02 17:10（Asia/Shanghai）。",
        "- Top10 正收益池；窗口固定为 impulse 45s / baseline 60s / breakout 60s。",
        "- 扫描：涨幅阈值、订单流 imbalance、成交强度、cooldown；确认桶固定为 1。",
        "- 退出：第一根未来 15 分钟阴线收盘；不使用固定止盈止损。",
        "- 收益按每笔 100 USDT 独立名义金额、往返费 0.08% 计算，未计滑点和资金占用。",
        "",
        "## 当前参数 control",
        "",
        f"- 65 条候选，其中 {control['all_mature_trades']} 条成熟，净收益 "
        f"{float(control['all_net_pnl_usdt']):+.2f} USDT，"
        f"胜率 {float(control['all_win_rate_pct']):.1f}%，"
        f"最大回撤 {float(control['all_max_drawdown_usdt']):.2f} USDT。",
        f"- 验证段起点：{validation_start.isoformat()}；验证成熟交易 "
        f"{int(control['validation_mature_trades'])} 条，净收益 "
        f"{float(control['validation_net_pnl_usdt']):+.2f} USDT。",
        "",
        "## 稳健候选前 20",
        "",
        "- 条件：训练段净收益不为负、验证段净收益不低于 control、全周期净收益不低于 control；两段各至少 8 条成熟交易。",
        "",
        "|排名|涨幅|imbalance|强度|cooldown|训练净收益|验证净收益|全周期净收益|验证PF|全周期回撤|验证2根阳线|",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for index, row in enumerate(robust_ranked[:20], start=1):
        lines.append(
            "| {index} | {ret:.2f}% | {imb:.2f} | {intensity:.1f} | {cooldown} "
            "| {train:+.2f} | {validation:+.2f} | {all_pnl:+.2f} | {pf} | {dd:.2f} | {tail} |".format(
                index=index,
                ret=float(row["min_return_pct"]) * 100.0,
                imb=float(row["min_aggressive_imbalance"]),
                intensity=float(row["min_notional_intensity"]),
                cooldown=int(row["cooldown_buckets"]),
                train=float(row["train_net_pnl_usdt"]),
                validation=float(row["validation_net_pnl_usdt"]),
                all_pnl=float(row["all_net_pnl_usdt"]),
                pf=(
                    f"{float(row['validation_profit_factor']):.2f}"
                    if row["validation_profit_factor"] is not None
                    else "n/a"
                ),
                dd=float(row["all_max_drawdown_usdt"]),
                tail=int(row["validation_tail_2_green_events"]),
            )
        )
    lines.extend(
        [
            "",
            "## 仅按验证段排名的说明",
            "",
            "- 验证段只有一个短时间块；低阈值高频组合可能因样本量和当日行情结构被抬高，不作为推荐。完整原始排名保存在 `grid_results.csv` 和 `grid_summary.json`。",
            "",
            "> 排名只用于筛查，不直接把最优组合写入实盘。由于验证段只有约两天，任何候选都需要滚动窗口或新增日期复核。",
            "",
            "## 输出",
            "",
            "- `grid_results.csv`：全部网格组合。",
            "- `grid_summary.json`：control、验证排名和全周期排名。",
        ]
    )
    (args.output_dir / "grid_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "variants": len(rows),
                "top10_up_events": len(events),
                "control_events": control["all_events"],
                "control_mature_trades": control["all_mature_trades"],
                "control_net_pnl_usdt": control["all_net_pnl_usdt"],
                "control_validation_net_pnl_usdt": control[
                    "validation_net_pnl_usdt"
                ],
                "top_robust": robust_ranked[:5],
                "top_validation": validation_ranked[:5],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
