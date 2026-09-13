"""Materialize dashboard API data as replay-compatible paper-account CSVs.

The server dashboard intentionally exposes position history rather than the
internal PostgreSQL export schema.  This adapter keeps the downloaded JSON as
the source of truth and creates the small, stable CSV surface consumed by the
orderflow B0/B1 replay.  Entry fees are reconstructed from the paper profile's
fixed fee rate; the API exposes aggregate position fees, not the entry/exit
split.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

POSITION_FIELDS = [
    "position_id",
    "run_id",
    "entry_fill_id",
    "signal_id",
    "symbol",
    "side",
    "status",
    "opened_at",
    "closed_at",
    "entry_price",
    "exit_price",
    "quantity",
    "entry_notional",
    "entry_fee",
    "exit_fee",
    "last_mark_price",
    "unrealized_pnl",
    "realized_pnl",
    "return_pct",
    "close_reason",
    "updated_at",
]

FILL_FIELDS = [
    "fill_id",
    "candidate_id",
    "signal_id",
    "run_id",
    "symbol",
    "side",
    "status",
    "target_fill_at",
    "filled_at",
    "requested_notional",
    "filled_notional",
    "quantity",
    "reference_midpoint",
    "spread",
    "fill_price",
    "fee",
    "total_cost",
    "cost_bps",
    "reason",
]


def decimal_string(value: Any, default: str = "0") -> str:
    if value is None or value == "":
        return default
    return str(value)


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: Any) -> str:
    if value is None:
        return ""
    return parse_dt(str(value)).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    return value


def materialize_position(
    row: dict[str, Any],
    *,
    run_id: str,
    fee_rate: float,
    updated_at: str,
) -> dict[str, str]:
    notional = decimal_string(row.get("entry_notional"), "100.000000000000000000")
    entry_fee = f"{float(notional) * fee_rate:.18f}"
    aggregate_fees = float(decimal_string(row.get("fees")))
    exit_fee = max(aggregate_fees - float(entry_fee), 0.0)
    closed_at = iso(row.get("closed_at"))
    status = decimal_string(row.get("status"), "closed")
    is_open = status != "closed"
    return {
        "position_id": decimal_string(row.get("position_id")),
        "run_id": run_id,
        "entry_fill_id": "",
        "signal_id": "",
        "symbol": decimal_string(row.get("symbol")),
        "side": decimal_string(row.get("side")),
        "status": status,
        "opened_at": iso(row.get("opened_at")),
        "closed_at": "" if is_open else closed_at,
        "entry_price": decimal_string(row.get("entry_price")),
        "exit_price": decimal_string(row.get("exit_price")),
        "quantity": decimal_string(row.get("quantity")),
        "entry_notional": notional,
        "entry_fee": entry_fee,
        "exit_fee": f"{exit_fee:.18f}",
        "last_mark_price": decimal_string(row.get("last_mark_price")),
        "unrealized_pnl": decimal_string(row.get("unrealized_pnl")),
        "realized_pnl": "" if is_open else decimal_string(row.get("realized_pnl")),
        "return_pct": "" if is_open else decimal_string(row.get("return_pct")),
        "close_reason": "" if is_open else decimal_string(row.get("close_reason")),
        "updated_at": closed_at or updated_at,
    }


def synthetic_fill(
    position: dict[str, str],
    *,
    fee_rate: float,
    suffix: str,
) -> dict[str, str]:
    notional = float(position["entry_notional"])
    fee = notional * fee_rate
    return {
        "fill_id": f"api_{position['position_id']}_{suffix}",
        "candidate_id": "",
        "signal_id": "",
        "run_id": position["run_id"],
        "symbol": position["symbol"],
        "side": position["side"],
        "status": "filled",
        "target_fill_at": position["opened_at"],
        "filled_at": position["opened_at"],
        "requested_notional": position["entry_notional"],
        "filled_notional": position["entry_notional"],
        "quantity": position["quantity"],
        "reference_midpoint": position["entry_price"],
        "spread": "0",
        "fill_price": position["entry_price"],
        "fee": f"{fee:.18f}",
        "total_cost": f"{fee + notional:.18f}",
        "cost_bps": f"{fee / notional * 10000:.12f}" if notional else "0",
        "reason": "dashboard_api_entry_fee_reconstruction",
    }


def read_reference_fills(path: Path | None, run_id: str) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if row.get("run_id") == run_id and row.get("status") == "filled"
        ]


def fill_key(row: dict[str, Any]) -> tuple[str, str, datetime, Decimal]:
    """Normalize CSV/API timestamp and decimal formatting for deduplication."""

    return (
        str(row.get("symbol", "")),
        str(row.get("side", "")),
        parse_dt(str(row.get("filled_at", ""))),
        Decimal(str(row.get("fill_price", "0"))),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--accounts", type=Path, required=True)
    parser.add_argument("--overview", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-server", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--fee-rate", type=float, default=0.0004)
    parser.add_argument("--reference-fills", type=Path)
    args = parser.parse_args()

    history = load_json(args.history)
    current = load_json(args.current)
    accounts = load_json(args.accounts)
    overview = load_json(args.overview)
    closed = history.get("closed_trades") or []
    open_positions = current.get("open_positions") or []
    if not closed:
        raise SystemExit("history contains no closed trades")

    positions = [
        materialize_position(
            row,
            run_id=args.run_id,
            fee_rate=args.fee_rate,
            updated_at=iso(current.get("checkpoint_at")),
        )
        for row in [*closed, *open_positions]
    ]
    positions.sort(key=lambda row: (parse_dt(row["opened_at"]), row["position_id"]))

    output_dir = args.output_dir
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    positions_path = raw_dir / "paper_positions.csv"
    with positions_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=POSITION_FIELDS)
        writer.writeheader()
        writer.writerows(positions)

    reference = read_reference_fills(args.reference_fills, args.run_id)
    reference_keys = Counter(fill_key(row) for row in reference)
    fills = list(reference)
    synthetic_count = 0
    for position in positions:
        key = fill_key(
            {
                "symbol": position["symbol"],
                "side": position["side"],
                "filled_at": position["opened_at"],
                "fill_price": position["entry_price"],
            }
        )
        if reference_keys[key]:
            reference_keys[key] -= 1
            continue
        fills.append(
            synthetic_fill(
                position,
                fee_rate=args.fee_rate,
                suffix=str(synthetic_count),
            )
        )
        synthetic_count += 1
    fills.sort(key=lambda row: (parse_dt(row["filled_at"]), row["fill_id"]))
    fills_path = raw_dir / "paper_fills.csv"
    with fills_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FILL_FIELDS)
        writer.writeheader()
        writer.writerows(fills)

    symbols = sorted({row["symbol"] for row in positions})
    symbols_path = output_dir / "symbols.txt"
    symbols_path.write_text(
        "".join(f"{symbol}\n" for symbol in symbols), encoding="utf-8"
    )

    account = next(
        (
            item
            for item in accounts.get("accounts", [])
            if item.get("run_id") == args.run_id
        ),
        {},
    )
    generated_at = overview.get("generated_at") or current.get("checkpoint_at")
    manifest = {
        "snapshot_at_utc": generated_at,
        "source": {
            "server": args.source_server,
            "base_url": args.base_url,
            "endpoints": {
                "overview": f"{args.base_url}/api/overview",
                "accounts": f"{args.base_url}/api/paper-accounts",
                "current": f"{args.base_url}/api/paper-accounts/{args.run_id}",
                "history": f"{args.base_url}/api/paper-accounts/{args.run_id}/history",
            },
        },
        "run_id": args.run_id,
        "strategy_name": account.get("strategy_name", current.get("strategy_name")),
        "exit_mode": account.get("exit_mode", current.get("exit_mode")),
        "config_hash": account.get("config_hash", current.get("config_hash")),
        "checkpoint_at": current.get("checkpoint_at"),
        "closed_trade_count": len(closed),
        "open_position_count": len(open_positions),
        "position_row_count": len(positions),
        "symbol_count": len(symbols),
        "fee_rate": args.fee_rate,
        "entry_fee_model": (
            "entry_notional multiplied by fixed fee rate; dashboard exposes only "
            "aggregate fees"
        ),
        "reference_fills": str(args.reference_fills) if args.reference_fills else None,
        "reference_fill_rows": len(reference),
        "synthetic_fill_rows": synthetic_count,
        "positions_path": str(positions_path),
        "fills_path": str(fills_path),
        "symbols_path": str(symbols_path),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
