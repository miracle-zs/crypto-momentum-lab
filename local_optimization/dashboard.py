#!/usr/bin/env python3
"""Interactive HTML dashboard generator for local optimization and MTM results.

Generates a standalone, self-contained single-file HTML report implementing
Astra's 5 reporting views (Overview, Pareto/Cross-scenario, 15s MTM Paths,
7D Neighborhood Stability, and 6-Layer Reconciliation).
"""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Ensure local_optimization can be imported
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from local_optimization.mtm_engine import (  # noqa: E402
    load_15s_price_series,
    load_trades_from_csv,
    reconstruct_mtm_equity,
)
from local_optimization.reconciliation import (  # noqa: E402
    match_per_symbol_trades,
    pair_round_trip_trades,
    to_beijing_str,
)


def sample_equity_series(csv_path: Path, max_points: int = 400) -> list[dict]:
    """Load and downsample MTM equity series for smooth rendering."""
    if not csv_path.exists():
        return []

    points = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            points.append(
                {
                    "time": to_beijing_str(row["timestamp"])[:16],
                    "equity": round(float(row["equity"]), 2),
                    "unrealized": round(float(row["unrealized_pnl"]), 2),
                    "margin": round(float(row["peak_initial_margin"]), 2),
                }
            )

    if len(points) <= max_points:
        return points

    step = max(1, len(points) // max_points)
    sampled = points[::step]
    if points[-1] != sampled[-1]:
        sampled.append(points[-1])
    return sampled


def load_replay_trades_for_account(replay_csv: Path) -> list[dict[str, Any]]:
    """Load replay round-trip trades from account replay events CSV."""
    if not replay_csv.exists():
        return []

    trades = []
    with replay_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            entry_p = float(r.get("entry_price") or 0.0)
            if entry_p <= 0:
                continue
            trades.append(
                {
                    "symbol": r.get("symbol", ""),
                    "entry_at": to_beijing_str(r.get("entry_at", "")),
                    "entry_price": entry_p,
                    "exit_at": to_beijing_str(r.get("exit_at", "")),
                    "exit_price": float(r.get("exit_price") or 0.0),
                    "exit_reason": r.get("exit_reason", ""),
                    "net_pnl_usdt": float(r.get("net_pnl_usdt") or 0.0),
                    "net_return_pct": float(r.get("net_return_pct") or 0.0),
                }
            )
    return trades


def sample_live_account_series(
    live_dir: Path | None,
    replay_dir: Path | None = None,
    max_points: int = 250,
) -> dict[str, list[dict]]:
    """Sample yesterday's 24h live and replay equity curves for 4 accounts."""
    if live_dir and live_dir.exists():
        if (
            not (live_dir / "primary").exists()
            and (live_dir.parent / "primary").exists()
        ):
            live_dir = live_dir.parent
    else:
        fallback = ROOT_DIR / "local_optimization/data/live_20260918"
        if fallback.exists():
            live_dir = fallback
        else:
            return {}

    if replay_dir is None or not replay_dir.exists():
        fallback_replay = ROOT_DIR / "local_optimization/data/replay_20260918"
        if fallback_replay.exists():
            replay_dir = fallback_replay

    accts = ["primary", "acc01", "acc02", "acc03"]
    results: dict[str, list[dict]] = {}

    # 1. Preload replay trades and 15s price series across all accounts
    parquet_root = (
        ROOT_DIR / "local_optimization/data/combined_parquet/environment=research"
    )
    all_acct_trades: dict[str, list] = {}
    all_symbols: set[str] = set()

    for acct in accts:
        if replay_dir:
            replay_file = replay_dir / f"account_{acct}_events.csv"
            if replay_file.exists():
                raw_trades = load_trades_from_csv(replay_file)
                # Filter to yesterday's 24h window
                # (2026-09-18 00:00:00 to 2026-09-19 00:00:00 UTC)
                in_win = [
                    t
                    for t in raw_trades
                    if datetime(2026, 9, 18, 0, 0, tzinfo=UTC)
                    <= t.entry_time
                    <= datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
                ]
                all_acct_trades[acct] = in_win
                all_symbols.update(t.symbol for t in in_win)

    price_series: dict[str, tuple[list[float], list[float]]] = {}
    if parquet_root.exists() and all_symbols:
        try:
            price_series = load_15s_price_series(parquet_root, all_symbols)
        except Exception as e:
            print(f"Warning: could not load 15s price series: {e}")

    for acct in accts:
        gz_path = live_dir / acct / "account_balance_usdt.csv.gz"
        raw_path = live_dir / acct / "account_balance_usdt.csv"
        target = (
            gz_path if gz_path.exists() else (raw_path if raw_path.exists() else None)
        )
        if not target:
            continue

        raw_pts = []
        try:
            opener = (
                gzip.open(target, "rt", encoding="utf-8")
                if target.suffix == ".gz"
                else target.open("r", encoding="utf-8")
            )
            with opener as f:
                reader = csv.DictReader(f)
                for r in reader:
                    bal = float(r.get("wallet_balance", 0.0))
                    avail = float(r.get("available_balance", 0.0))
                    upnl = float(r.get("unrealized_pnl", 0.0))
                    t = to_beijing_str(r.get("observed_at") or "")
                    raw_pts.append(
                        {
                            "time": t,
                            "equity": round(bal + upnl, 2),
                            "wallet": round(bal, 2),
                            "unrealized": round(upnl, 2),
                            "margin": round(max(0.0, bal - avail), 2),
                        }
                    )
        except Exception:
            continue

        if not raw_pts:
            continue

        step = max(1, len(raw_pts) // max_points)
        sampled = raw_pts[::step]
        if raw_pts[-1] != sampled[-1]:
            sampled.append(raw_pts[-1])

        base_eq = sampled[0]["equity"] if sampled else 1.0
        start_time_str = sampled[0]["time"] if sampled else ""

        # Reconstruct continuous 15s MTM points for this account
        acct_trades = all_acct_trades.get(acct, [])
        mtm_pts = []
        mtm_times = []
        if acct_trades and price_series:
            mtm_pts = reconstruct_mtm_equity(
                trades=acct_trades,
                price_series=price_series,
                initial_equity=0.0,
                grid_seconds=15,
                start_time=datetime(2026, 9, 18, 0, 0, tzinfo=UTC),
                end_time=datetime(2026, 9, 19, 0, 0, tzinfo=UTC),
            )
            mtm_times = [to_beijing_str(pt.timestamp) for pt in mtm_pts]

        # Calculate live return % and time-aligned continuous replay equity
        for p in sampled:
            live_return = (
                round(((p["equity"] - base_eq) / base_eq) * 100.0, 2)
                if base_eq > 0
                else 0.0
            )
            p["return_pct"] = live_return

            t_str = p["time"]
            if mtm_pts:
                idx = bisect.bisect_right(mtm_times, t_str) - 1
                if idx < 0:
                    rep_pnl = 0.0
                    rep_margin = 0.0
                else:
                    pt = mtm_pts[idx]
                    rep_pnl = pt.realized_pnl + pt.unrealized_pnl
                    rep_margin = pt.peak_initial_margin
                rep_equity = round(base_eq + rep_pnl, 2)
                rep_return = (
                    round((rep_pnl / base_eq) * 100.0, 2) if base_eq > 0 else 0.0
                )
                p["replay_equity"] = rep_equity
                p["replay_return_pct"] = rep_return
                p["replay_margin"] = round(rep_margin, 2)
            else:
                # Graceful fallback to discrete cumulative pnl
                cum_replay_pnl = sum(
                    tr.calculated_net_pnl
                    for tr in acct_trades
                    if start_time_str <= to_beijing_str(tr.exit_time) <= t_str
                )
                p["replay_equity"] = round(base_eq + cum_replay_pnl, 2)
                p["replay_return_pct"] = (
                    round((cum_replay_pnl / base_eq) * 100.0, 2) if base_eq > 0 else 0.0
                )
                p["replay_margin"] = 0.0

        results[acct] = sampled

    return results


def load_per_symbol_reconciliation(
    live_dir: Path | None,
    replay_dir: Path | None = None,
) -> dict[str, Any]:
    """Extract round-trip trades from fills and match with replay events."""
    if live_dir and live_dir.exists():
        if (
            not (live_dir / "primary").exists()
            and (live_dir.parent / "primary").exists()
        ):
            live_dir = live_dir.parent
    else:
        fallback = ROOT_DIR / "local_optimization/data/live_20260918"
        if fallback.exists():
            live_dir = fallback
        else:
            return {}

    if replay_dir is None or not replay_dir.exists():
        fallback_replay = ROOT_DIR / "local_optimization/data/replay_20260918"
        if fallback_replay.exists():
            replay_dir = fallback_replay

    accts = ["primary", "acc01", "acc02", "acc03"]
    per_account_recon: dict[str, Any] = {}

    for acct in accts:
        gz_path = live_dir / acct / "account_fill_events.csv.gz"
        raw_path = live_dir / acct / "account_fill_events.csv"
        target = (
            gz_path if gz_path.exists() else (raw_path if raw_path.exists() else None)
        )
        if not target:
            continue

        fills = []
        try:
            opener = (
                gzip.open(target, "rt", encoding="utf-8")
                if target.suffix == ".gz"
                else target.open("r", encoding="utf-8")
            )
            with opener as f:
                reader = csv.DictReader(f)
                for r in reader:
                    fills.append(r)
        except Exception:
            continue

        # Pair into round-trip trades
        live_trades = pair_round_trip_trades(fills)

        # Load replay trades
        replay_trades = []
        if replay_dir:
            replay_file = replay_dir / f"account_{acct}_events.csv"
            raw_replay = load_replay_trades_for_account(replay_file)
            replay_trades = [
                tr
                for tr in raw_replay
                if str(tr.get("entry_at") or "") >= "2026-09-18 08:00:00"
            ]

        match_res = match_per_symbol_trades(live_trades, replay_trades)
        per_account_recon[acct] = match_res

    per_account_recon["all"] = aggregate_account_reconciliations(per_account_recon)
    return per_account_recon


def aggregate_account_reconciliations(
    per_account_recon: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate records and summary across accounts.

    Performs aggregation without cross-account trade matching.
    """
    all_records: list[dict[str, Any]] = []
    tot_live_trades = 0
    tot_replay_trades = 0
    tot_matched_count = 0
    tot_live_only = 0
    tot_replay_only = 0
    tot_live_pnl = 0.0
    tot_replay_pnl = 0.0
    entry_slips: list[float] = []
    exit_slips: list[float] = []

    for k, r in per_account_recon.items():
        if k == "all":
            continue
        recs = r.get("records", [])
        all_records.extend(recs)
        s = r.get("summary", {})
        tot_live_trades += s.get("total_live_trades", 0)
        tot_replay_trades += s.get("total_replay_trades", 0)
        tot_matched_count += s.get("matched_count", 0)
        tot_live_only += s.get("live_only_count", 0)
        tot_replay_only += s.get("replay_only_count", 0)
        tot_live_pnl += s.get("total_live_net_pnl", s.get("live_total_pnl", 0.0))
        tot_replay_pnl += s.get("total_replay_net_pnl", s.get("replay_total_pnl", 0.0))
        for rec in recs:
            if rec.get("status") == "MATCHED":
                entry_slips.append(rec.get("entry_slippage_bps", 0.0))
                exit_slips.append(rec.get("exit_slippage_bps", 0.0))

    mean_entry_slip = sum(entry_slips) / len(entry_slips) if entry_slips else 0.0
    mean_exit_slip = sum(exit_slips) / len(exit_slips) if exit_slips else 0.0
    match_rate = tot_matched_count / max(1, tot_live_trades)

    return {
        "records": all_records,
        "summary": {
            "total_live_trades": tot_live_trades,
            "total_replay_trades": tot_replay_trades,
            "matched_count": tot_matched_count,
            "live_only_count": tot_live_only,
            "replay_only_count": tot_replay_only,
            "mean_entry_slippage_bps": round(mean_entry_slip, 2),
            "mean_exit_slippage_bps": round(mean_exit_slip, 2),
            "live_total_pnl": round(tot_live_pnl, 2),
            "replay_total_pnl": round(tot_replay_pnl, 2),
            "total_live_net_pnl": round(tot_live_pnl, 2),
            "total_replay_net_pnl": round(tot_replay_pnl, 2),
            "total_pnl_delta_usdt": round(tot_live_pnl - tot_replay_pnl, 2),
            "total_slippage_usdt": round(tot_live_pnl - tot_replay_pnl, 2),
            "match_rate": round(match_rate, 4),
        },
    }


def sample_discrete_vs_mtm_equity(csv_path: Path, max_points: int = 400) -> list[dict]:
    """Load discrete closed trade equity vs continuous 15s MTM equity and margin."""
    if not csv_path.exists():
        return []

    points = []
    hwm = 0.0
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if not rows:
            return []

        first_eq = float(rows[0]["equity"])
        first_realized = float(rows[0].get("realized_pnl", 0.0))
        first_unrealized = float(rows[0].get("unrealized_pnl", 0.0))
        init_eq = round(first_eq - first_realized - first_unrealized, 2)
        if init_eq <= 0:
            init_eq = 100.0

        for row in rows:
            eq = float(row["equity"])
            if eq > hwm:
                hwm = eq
            realized = float(row.get("realized_pnl", 0.0))
            discrete = init_eq + realized
            gap = max(0.0, discrete - eq)
            dd_pct = -round(((hwm - eq) / hwm) * 100.0, 2) if hwm > 0 else 0.0
            margin = round(float(row.get("peak_initial_margin", 0.0)), 2)
            t_beijing = to_beijing_str(row["timestamp"])

            points.append(
                {
                    "time": t_beijing[:16],
                    "mtm": round(eq, 2),
                    "discrete": round(discrete, 2),
                    "gap": round(gap, 2),
                    "drawdown_pct": dd_pct,
                    "margin": margin,
                    "hwm": round(hwm, 2),
                    "unrealized": round(float(row.get("unrealized_pnl", 0.0)), 2),
                }
            )

    if len(points) <= max_points:
        return points

    step = max(1, len(points) // max_points)
    sampled = points[::step]
    if points[-1] != sampled[-1]:
        sampled.append(points[-1])
    return sampled


def generate_three_track_data(
    equity_series: list[dict],
    profile_equity_csv: Path | None = None,
    base_capital: float = 1000.0,
    max_points: int = 350,
) -> list[dict]:
    """Generate longitudinal three-track comparison curves.

    Requires authentic evaluation curves. If profile_equity_csv is not provided
    or does not contain evaluations, returns only the baseline curve with None
    for uncomputed tracks, never synthesizing artificial scaled paths.
    """
    if profile_equity_csv and profile_equity_csv.exists():
        import csv

        with open(profile_equity_csv, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if rows:
            pts = []
            for r in rows:
                t_bj = to_beijing_str(r["timestamp"])[:16]
                base_eq = round(
                    base_capital + float(r.get("baseline_cumulative_pnl_usdt", 0.0)), 2
                )
                best_raw = r.get("D_cumulative_pnl_usdt")
                rec_raw = r.get("F_cumulative_pnl_usdt")
                best_eq = (
                    round(base_capital + float(best_raw), 2)
                    if best_raw is not None
                    else None
                )
                rec_eq = (
                    round(base_capital + float(rec_raw), 2)
                    if rec_raw is not None
                    else None
                )
                pts.append(
                    {
                        "time": t_bj,
                        "baseline": base_eq,
                        "recommended": rec_eq,
                        "daily_best": best_eq,
                    }
                )
            if len(pts) <= max_points:
                return pts
            step = max(1, len(pts) // max_points)
            sampled = pts[::step]
            if pts[-1] != sampled[-1]:
                sampled.append(pts[-1])
            return sampled

    if not equity_series:
        return []

    # If no separate profile equity csv is available, output baseline only
    # with None for other tracks (do not synthesize fake curves)
    # DO NOT synthesize fake upward curves using progress = max(0.0, ...)
    three_track = []
    for p in equity_series:
        three_track.append(
            {
                "time": p["time"],
                "baseline": p["equity"],
                "recommended": None,
                "daily_best": None,
            }
        )
    return three_track


def load_optimization_groups(opt_dir: Path | None = None) -> dict[str, Any]:
    """Load the 2 streamlined optimization groups: Margin <= 280U and Unconstrained."""
    if opt_dir is None:
        opt_dir = (
            ROOT_DIR / "local_optimization/data/optimization_all_collected_20260919"
        )

    cached_json = opt_dir / "opt_groups.json"
    if cached_json.exists():
        try:
            return json.loads(cached_json.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Warning: failed to load {cached_json}: {e}")

    grid_csv = opt_dir / "grid_results.csv"
    if grid_csv.exists():
        try:
            from local_optimization.build_opt_groups import generate_opt_groups_from_run

            groups = generate_opt_groups_from_run(opt_dir)
            cached_json.write_text(
                json.dumps(groups, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            return groups
        except Exception as e:
            print(f"Warning: failed to generate opt groups: {e}")

    # If neither cached JSON nor grid CSV can produce valid groups, return empty dict
    return {}


def render_dashboard(
    output_path: Path,
    report_data: dict,
    equity_series: list[dict],
    live_dir: Path | None = None,
    replay_dir: Path | None = None,
    opt_dir: Path | None = None,
) -> None:
    """Load template and render HTML dashboard with complete visual charts."""
    template_path = SCRIPT_DIR / "templates" / "dashboard_template.html"
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found at: {template_path}")

    template_str = template_path.read_text(encoding="utf-8")

    # Generate data streams
    profile_csv = opt_dir / "profile_equity_series.csv" if opt_dir else None
    three_track_series = generate_three_track_data(
        equity_series, profile_equity_csv=profile_csv
    )
    live_accounts = sample_live_account_series(live_dir, replay_dir=replay_dir)
    reconciliation_data = load_per_symbol_reconciliation(
        live_dir, replay_dir=replay_dir
    )

    equity_csv_file = (
        ROOT_DIR / "local_optimization/reports/baseline_15s_mtm_equity_series.csv"
    )
    discrete_vs_mtm = sample_discrete_vs_mtm_equity(equity_csv_file, max_points=350)
    opt_groups = load_optimization_groups(opt_dir)

    # Replace basic placeholders
    rendered = template_str.replace(
        "{{ date }}", str(report_data.get("date", "2026-09-18"))
    )
    rendered = rendered.replace(
        "{{ n_candidates }}", f"{report_data.get('n_candidates', 25200):,}"
    )

    # Embed data for client-side visual charting
    data_payload = json.dumps(
        {
            "report": report_data,
            "equity": equity_series,
            "three_track": three_track_series,
            "live_accounts": live_accounts,
            "reconciliation": reconciliation_data,
            "discrete_vs_mtm": discrete_vs_mtm,
            "opt_groups": opt_groups,
        },
        ensure_ascii=False,
    )
    rendered = rendered.replace(
        "</body>",
        f"<script>window.CML_DASHBOARD_DATA = {data_payload};</script>\n</body>",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    print(f"✅ Dashboard generated successfully: {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate interactive HTML dashboard.")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT_DIR / "local_optimization/reports/dashboard_2026-09-18.html",
        help="Path to output HTML file",
    )
    parser.add_argument(
        "--opt-dir",
        type=Path,
        default=ROOT_DIR
        / "local_optimization/data/optimization_all_collected_20260919",
        help="Path to optimization output directory",
    )
    parser.add_argument(
        "--equity-csv",
        type=Path,
        default=ROOT_DIR
        / "local_optimization/reports/baseline_15s_mtm_equity_series.csv",
        help="Path to 15s MTM equity series CSV",
    )
    parser.add_argument(
        "--live-dir",
        type=Path,
        default=ROOT_DIR / "local_optimization/data/live_20260918",
        help="Path to live accounts directory",
    )
    parser.add_argument(
        "--replay-dir",
        type=Path,
        default=ROOT_DIR / "local_optimization/data/replay_20260918",
        help="Path to replay events directory",
    )
    args = parser.parse_args()

    equity_points = sample_equity_series(args.equity_csv, max_points=300)
    report_data = {
        "date": "2026-09-03 07:19 ~ 2026-09-19 08:00 (全量收集数据)",
        "snapshot": args.opt_dir.name,
        "protocol": "margin280-free-cooldown",
        "n_candidates": 25200,
    }

    render_dashboard(
        args.output,
        report_data,
        equity_points,
        live_dir=args.live_dir,
        replay_dir=args.replay_dir,
        opt_dir=args.opt_dir,
    )

    # Also copy to artifacts directory for inline generative UI
    artifact_dir = Path(
        "/Users/zhangshuai/.gemini/antigravity/brain/8dbad00e-85df-4c95-8247-5ff87471dfa5"
    )
    if artifact_dir.exists():
        artifact_file = artifact_dir / "optimization_dashboard.html"
        render_dashboard(
            artifact_file,
            report_data,
            equity_points,
            live_dir=args.live_dir,
            replay_dir=args.replay_dir,
            opt_dir=args.opt_dir,
        )


if __name__ == "__main__":
    main()
