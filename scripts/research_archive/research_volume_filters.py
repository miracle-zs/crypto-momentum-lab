"""Causal volume-feature ablation on frozen, zero-cooldown replay events."""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "server_exports/cml-research-data-20260908-170354"
OUTPUT = SNAPSHOT / "volume-feature-study-20260909"
FEATURES = {
    "notional_1m_vs_5m": ("trade_notional", 4, 20),
    "notional_1m_vs_15m": ("trade_notional", 4, 60),
    "notional_5m_vs_30m": ("trade_notional", 20, 120),
}
THRESHOLDS = (1.0, 1.25, 1.5, 2.0, 3.0, 4.0)
SPLIT = pd.Timestamp("2026-09-07", tz="UTC")


def metrics(events):
    filled = events[events.entry_at.notna()]
    closed = filled[filled.closed.eq(True) & filled.exit_at.notna()]
    cash = closed.groupby("exit_at").net_pnl_usdt.sum().sort_index().cumsum()
    peak = cash.cummax().clip(lower=0)
    # Keep the old event-order DD as a separately labelled compatibility metric.
    legacy_cash = closed.net_pnl_usdt.cumsum()
    margin = (
        pd.concat(
            [
                pd.Series(20.0, index=filled.entry_at),
                pd.Series(-20.0, index=closed.exit_at),
            ]
        )
        .groupby(level=0)
        .sum()
        .sort_index()
        .cumsum()
    )
    return {
        "selected": len(events),
        "closed": len(closed),
        "pnl": float(closed.net_pnl_usdt.sum()),
        "realized_dd": float((peak - cash).max()) if len(cash) else 0.0,
        "legacy_dd": float((legacy_cash.cummax().clip(lower=0) - legacy_cash).max())
        if len(closed)
        else 0.0,
        "margin": float(margin.max()) if len(margin) else 0.0,
        "win_rate": float(closed.net_pnl_usdt.gt(0).mean()) if len(closed) else 0.0,
    }


def add_features(events):
    symbols = set(events.symbol)
    columns = [
        "symbol",
        "bucket_start",
        "bucket_end",
        "trade_notional",
        "trade_count",
        "data_complete",
        "missing_agg_trade_count",
        "environment",
    ]
    frames = []
    for path in sorted((SNAPSHOT / "parquet").rglob("*.parquet")):
        frame = pq.ParquetFile(path).read(columns=columns).to_pandas()
        frames.append(frame[frame.symbol.isin(symbols)])
    states = pd.concat(frames, ignore_index=True)
    states = states[
        states.environment.eq("research")
        & states.data_complete
        & states.missing_agg_trade_count.fillna(0).eq(0)
    ]
    states = states.drop_duplicates(["symbol", "bucket_start"], keep="first")
    states["trade_notional"] = pd.to_numeric(states.trade_notional)
    states = states.sort_values(["symbol", "bucket_start"])
    outputs = []
    for _symbol, group in states.groupby("symbol", sort=False):
        group = group.copy()
        for name, (column, recent, prior) in FEATURES.items():
            current = group[column].rolling(recent, min_periods=recent).mean()
            baseline = (
                group[column].shift(recent).rolling(prior, min_periods=prior).mean()
            )
            contiguous = (
                group.bucket_start - group.bucket_start.shift(recent + prior - 1)
            ).eq(pd.Timedelta(seconds=15 * (recent + prior - 1)))
            ratio = current / baseline.replace(0, np.nan)
            group[name] = ratio.where(contiguous)
        outputs.append(group[["symbol", "bucket_start", "bucket_end", *FEATURES]])
    values = pd.concat(outputs, ignore_index=True)
    result = events.merge(
        values,
        left_on=["symbol", "detected_at"],
        right_on=["symbol", "bucket_start"],
        how="left",
        validate="many_to_one",
    )
    available = result.bucket_end.notna()
    assert (
        result.loc[available, "bucket_end"] <= result.loc[available, "order_created_at"]
    ).all()
    return result


def main():
    OUTPUT.mkdir(exist_ok=True)
    event_sets = []
    for label in ("A", "F"):
        directory = (
            SNAPSHOT
            / f"optimization-full-pnl-current-{label}-20260909-exclude-0800-1000-asia"
        )
        frame = pd.read_csv(directory / "best_candidate_events.csv")
        frame["profile"] = label
        for col in ("detected_at", "order_created_at", "entry_at", "exit_at"):
            frame[col] = pd.to_datetime(frame[col], utc=True, format="mixed")
        event_sets.append(frame)
    events = add_features(pd.concat(event_sets, ignore_index=True))
    events.to_csv(OUTPUT / "events_with_volume_features.csv", index=False)
    rows, selections = [], []
    for label, group in events.groupby("profile"):
        candidates = [("none", 0.0)] + [(f, t) for f in FEATURES for t in THRESHOLDS]
        profile_rows = []
        for feature, threshold in candidates:
            selected = (
                group if feature == "none" else group[group[feature].ge(threshold)]
            )
            train = selected[selected.entry_at.lt(SPLIT) & selected.exit_at.lt(SPLIT)]
            test = selected[selected.entry_at.ge(SPLIT)]
            row = {
                "profile": label,
                "feature": feature,
                "threshold": threshold,
                **metrics(selected),
                **{"train_" + k: v for k, v in metrics(train).items()},
                **{"test_" + k: v for k, v in metrics(test).items()},
            }
            rows.append(row)
            profile_rows.append(row)
        cap = float("inf") if label == "A" else 280
        feasible = [r for r in profile_rows if r["margin"] <= cap]
        best = max(feasible, key=lambda r: (r["pnl"], -r["realized_dd"]))
        # Threshold selection only sees exits available before the split.
        train_feasible = [
            r
            for r in profile_rows
            if r["train_closed"] >= 20 and r["train_margin"] <= cap
        ]
        train_best = max(
            train_feasible, key=lambda r: (r["train_pnl"], -r["train_realized_dd"])
        )
        baseline = profile_rows[0]
        selections.append(
            {
                "profile": label,
                "baseline": baseline,
                "full_sample_best": best,
                "chronological_choice": train_best,
                "coverage_only": {
                    f: metrics(group[group[f].notna()]) for f in FEATURES
                },
                "missing_features": {f: int(group[f].isna().sum()) for f in FEATURES},
            }
        )
        for choice_name, choice in [("baseline", baseline), ("best", best)]:
            subset = (
                group
                if choice["feature"] == "none"
                else group[group[choice["feature"]].ge(choice["threshold"])]
            )
            closed = subset[subset.closed.eq(True) & subset.exit_at.notna()].copy()
            closed["exit_day_beijing"] = closed.exit_at.dt.tz_convert(
                "Asia/Shanghai"
            ).dt.date
            closed.groupby("exit_day_beijing").net_pnl_usdt.agg(
                ["sum", "count"]
            ).to_csv(OUTPUT / f"{label}-{choice_name}-daily.csv")
    pd.DataFrame(rows).to_csv(OUTPUT / "filter_grid.csv", index=False)
    (OUTPUT / "summary.json").write_text(
        json.dumps(selections, indent=2), encoding="utf-8"
    )
    lines = [
        "# 持续放量特征对照研究",
        "",
        "数据窗口沿用完整回补数据；排除北京时间 [08:00,10:00) 实际成交开仓。",
        "固定 A、F 的六个参数，仅扫描新增特征的下限；不代表七维联合寻优。",
        "所有特征只用下单前已结束的 15 秒桶。"
        "近期与基准窗口不重叠，缺桶或分母为零时不放行。",
        "既有 intensity 的基准只有 4 个桶（1 分钟）；本轮比较三个成交额持续放量窗口。",
        "",
        "|参数组|条件|收益 U|按平仓时间回撤 U|旧口径回撤 U|峰值保证金 U|平仓数|",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for selection in selections:
        for key in ("baseline", "full_sample_best"):
            r = selection[key]
            lines.append(
                f"|{r['profile']} {key}|{r['feature']} ≥ {r['threshold']}|"
                f"{r['pnl']:.2f}|{r['realized_dd']:.2f}|{r['legacy_dd']:.2f}|"
                f"{r['margin']:.0f}|{r['closed']}|"
            )
        b, t = selection["baseline"], selection["chronological_choice"]
        lines.extend(
            [
                "",
                f"{selection['profile']}：用 09-07 00:00 UTC 前已平仓交易选择 "
                f"{t['feature']} ≥ {t['threshold']}，"
                f"之后入场交易收益 {t['test_pnl']:.2f}U / {t['test_closed']} 笔；"
                f"不加特征同期 {b['test_pnl']:.2f}U / {b['test_closed']} 笔。",
                "",
            ]
        )
        if t['feature'] != 'none':
            coverage = selection['coverage_only'][t['feature']]
            lines.append(
                f"只要求该特征有完整历史、不限制倍数时，收益 {coverage['pnl']:.2f}U，"
                f"平仓 {coverage['closed']} 笔；"
                f"缺失信号 {selection['missing_features'][t['feature']]} 个。"
            )
    lines.extend(
        [
            "## 解释范围",
            "",
            "这是已观察历史样本上的特征筛查。基础参数已经用过全量数据，日期分割仅用于稳定性诊断，并非独立样本外验证。",
            "回撤同时给出旧报告按信号顺序累计口径，以及按真实平仓时刻累计的已实现回撤；两者都不包含持仓浮亏。",
                "交易复用已有回放：零 cooldown、逐信号独立成交，"
                "不因删去交易释放资金而补发新单。",
            "本轮未修改实盘配置。",
        ]
    )
    (OUTPUT / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(selections, indent=2))
    print(OUTPUT / "report.md")


if __name__ == "__main__":
    main()
