#!/usr/bin/env python3
"""Reconcile account-fill PnL against the closed-trade reconstruction."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from analyze_live_trading_data import (
    aggregate_fills_by_order,
    as_float,
    load_fills,
    load_intents,
    load_orders,
    reconstruct_trades,
)


def build_reconciliation(input_dir: Path) -> dict[str, float | int | None]:
    fills = load_fills(input_dir / "account_fill_events.csv.gz")
    orders = load_orders(input_dir / "exchange_orders.csv.gz")
    intents = load_intents(input_dir / "order_intents.csv.gz")
    realized_pnl = sum(
        (fill["realized_pnl"] for fill in fills), start=Decimal("0")
    )
    fees = sum((fill["fee"] for fill in fills), start=Decimal("0"))
    order_aggs = aggregate_fills_by_order(fills)
    orders_by_exchange_id = {
        order["exchange_order_id"]: order
        for order in orders
        if order.get("exchange_order_id")
    }
    trades, matched_segments, unmatched_quantity = reconstruct_trades(
        order_aggs,
        orders_by_exchange_id,
        intents,
    )
    reconstructed_net = sum(
        (trade["net_pnl"] for trade in trades), start=Decimal("0")
    )
    return {
        "account_fill_rows": len(fills),
        "account_realized_pnl_before_fees": as_float(realized_pnl),
        "account_fees": as_float(fees),
        "account_net_pnl_after_fees": as_float(realized_pnl - fees),
        "reconstructed_closed_trade_segments": matched_segments,
        "reconstructed_closed_net_pnl_after_fees": as_float(reconstructed_net),
        "account_vs_reconstruction_difference": as_float(
            (realized_pnl - fees) - reconstructed_net
        ),
        "unmatched_or_open_quantity": as_float(unmatched_quantity),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--expected-net-pnl", type=float, default=-256.0)
    parser.add_argument("--tolerance", type=float, default=1.0)
    args = parser.parse_args()
    result = build_reconciliation(args.input_dir)
    result["expected_net_pnl"] = args.expected_net_pnl
    result["difference_from_expected"] = (
        result["account_net_pnl_after_fees"] - args.expected_net_pnl
        if result["account_net_pnl_after_fees"] is not None
        else None
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    actual = result["account_net_pnl_after_fees"]
    if actual is None or abs(actual - args.expected_net_pnl) > args.tolerance:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
