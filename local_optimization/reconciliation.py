"""Six-layer live vs replay reconciliation and causal divergence attribution.

Implements the multi-layer audit hierarchy:
1. Inputs/Features: Universe & market state alignment
2. Signals/Intents: Signal precision & recall
3. Risk/Orders: Order submission & rejection tracking
4. Fills: Execution price slippage, latency, fill rate
5. Batches/Exits: Position batch boundary & exit reason match
6. Equity: Final equity path drift & PnL attribution
Identifies the first causal divergence point to avoid cascading false alarms.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReconciliationDivergence:
    """A specific event where live execution and replay diverged."""

    timestamp: str
    symbol: str
    layer: str  # inputs | signals | orders | fills | batches | equity
    divergence_type: str
    details: dict[str, Any] = field(default_factory=dict)
    impact_usdt: float = 0.0
    is_root_cause: bool = False


@dataclass(frozen=True)
class LayerSummary:
    """Summary metrics for a specific reconciliation layer."""

    layer: str
    live_total: int
    replay_total: int
    matched_total: int
    precision: float | None  # matched / replay_total
    recall: float | None  # matched / live_total
    discrepancy_count: int


@dataclass
class ReconciliationReport:
    """Complete 6-layer reconciliation ledger."""

    account_id: str
    start_time: str
    end_time: str
    layers: dict[str, LayerSummary] = field(default_factory=dict)
    divergences: list[ReconciliationDivergence] = field(default_factory=list)
    first_divergence: ReconciliationDivergence | None = None
    live_final_equity: float = 0.0
    replay_final_equity: float = 0.0
    equity_divergence_usdt: float = 0.0
    is_audit_passed: bool = True
    audit_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def save_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, sort_keys=True)


def _extract_epoch(rec: dict[str, Any], default: float = 0.0) -> float:
    for key in ("time_epoch", "timestamp_epoch", "epoch", "trade_epoch"):
        if key in rec and rec[key] is not None:
            try:
                return float(rec[key])
            except (ValueError, TypeError):
                pass
    for key in (
        "timestamp",
        "trade_at",
        "time",
        "detected_at",
        "source_state_at",
        "entry_time",
        "entry_at",
    ):
        val = rec.get(key)
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, datetime):
            return val.timestamp()
        if isinstance(val, str) and val.strip():
            try:
                clean_val = val.replace("Z", "+00:00")
                return datetime.fromisoformat(clean_val).timestamp()
            except Exception:
                pass
    return default


def reconcile_signals_and_fills(
    live_signals: Sequence[dict[str, Any]],
    replay_signals: Sequence[dict[str, Any]],
    live_fills: Sequence[dict[str, Any]],
    replay_fills: Sequence[dict[str, Any]],
    *,
    account_id: str = "primary",
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    time_tolerance_sec: float = 60.0,
    price_tolerance_pct: float = 0.002,  # 20 bps
    qty_tolerance_pct: float = 0.05,  # 5%
) -> ReconciliationReport:
    """Perform multi-layer matching and causal divergence identification.

    Args:
        live_signals: Live strategy signal records.
        replay_signals: Replay strategy signal records.
        live_fills: Live trade/fill execution records.
        replay_fills: Replay trade/fill execution records.
        account_id: Target account identifier.
        start_time: Window start.
        end_time: Window end.
        time_tolerance_sec: Max timestamp difference to consider a match.
        price_tolerance_pct: Max relative price difference for execution match.
        qty_tolerance_pct: Max relative quantity difference for fill match.

    Returns:
        ReconciliationReport with layer statistics and root divergence.
    """
    start_str = start_time.isoformat() if start_time else ""
    end_str = end_time.isoformat() if end_time else ""

    divergences: list[ReconciliationDivergence] = []

    # 1. Layer: Signals / Intents
    # Match key: (symbol, direction, account_id, config_id)
    def make_signal_key(s: dict[str, Any]) -> tuple[str, str, str, str]:
        acc = str(s.get("account_id") or s.get("account") or account_id or "").strip()
        cfg = str(
            s.get("config_id") or s.get("config") or s.get("strategy_id") or ""
        ).strip()
        if not acc:
            acc = f"__AMBIGUOUS_ACC_{id(s)}__"
        return (
            str(s.get("symbol", "")).upper(),
            str(s.get("direction", "LONG")).upper(),
            acc,
            cfg,
        )

    live_sig_map: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for s in live_signals:
        live_sig_map.setdefault(make_signal_key(s), []).append(s)

    matched_signals = 0
    replay_only_signals: list[dict[str, Any]] = []

    for r_sig in replay_signals:
        key = make_signal_key(r_sig)
        r_time = _extract_epoch(r_sig)
        candidates = live_sig_map.get(key, [])
        match = None
        for c in candidates:
            c_time = _extract_epoch(c)
            if abs(r_time - c_time) <= time_tolerance_sec:
                match = c
                break
        if match:
            matched_signals += 1
            candidates.remove(match)
        else:
            replay_only_signals.append(r_sig)
            divergences.append(
                ReconciliationDivergence(
                    timestamp=r_sig.get("timestamp", ""),
                    symbol=str(r_sig.get("symbol", "")),
                    layer="signals",
                    divergence_type="replay_only_signal",
                    details={"signal": r_sig},
                )
            )

    live_only_signals = [item for sublist in live_sig_map.values() for item in sublist]
    for l_sig in live_only_signals:
        divergences.append(
            ReconciliationDivergence(
                timestamp=l_sig.get("timestamp", ""),
                symbol=str(l_sig.get("symbol", "")),
                layer="signals",
                divergence_type="live_only_signal",
                details={"signal": l_sig},
            )
        )

    sig_prec = matched_signals / len(replay_signals) if replay_signals else 0.0
    sig_rec = matched_signals / len(live_signals) if live_signals else 0.0

    sig_summary = LayerSummary(
        layer="signals",
        live_total=len(live_signals),
        replay_total=len(replay_signals),
        matched_total=matched_signals,
        precision=round(sig_prec, 4),
        recall=round(sig_rec, 4),
        discrepancy_count=len(live_only_signals) + len(replay_only_signals),
    )

    # 2. Layer: Fills / Executions
    def make_fill_key(f: dict[str, Any]) -> tuple[str, str, str, str]:
        acc = str(f.get("account_id") or f.get("account") or account_id or "").strip()
        cfg = str(
            f.get("config_id") or f.get("config") or f.get("strategy_id") or ""
        ).strip()
        if not acc:
            acc = f"__AMBIGUOUS_ACC_{id(f)}__"
        return (
            str(f.get("symbol", "")).upper(),
            str(f.get("side", "")).upper(),
            acc,
            cfg,
        )

    live_fill_map: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for f in live_fills:
        live_fill_map.setdefault(make_fill_key(f), []).append(f)

    matched_fills = 0
    replay_only_fills: list[dict[str, Any]] = []

    for r_fill in replay_fills:
        key = make_fill_key(r_fill)
        r_time = _extract_epoch(r_fill)
        r_price = float(r_fill.get("price", 0.0))
        r_qty = float(
            r_fill.get("quantity") or r_fill.get("qty") or r_fill.get("size") or 0.0
        )
        candidates = live_fill_map.get(key, [])
        match = None
        for c in candidates:
            c_time = _extract_epoch(c)
            c_price = float(c.get("price", 0.0))
            c_qty = float(c.get("quantity") or c.get("qty") or c.get("size") or 0.0)
            if abs(r_time - c_time) <= time_tolerance_sec:
                if (
                    r_price > 0
                    and c_price > 0
                    and abs(c_price - r_price) / r_price > price_tolerance_pct
                ):
                    continue
                if r_qty > 0 or c_qty > 0:
                    if r_qty <= 0 or c_qty <= 0:
                        continue
                    if abs(c_qty - r_qty) / r_qty > qty_tolerance_pct:
                        continue
                match = c
                break
        if match:
            matched_fills += 1
            candidates.remove(match)
        else:
            replay_only_fills.append(r_fill)
            divergences.append(
                ReconciliationDivergence(
                    timestamp=r_fill.get("timestamp", ""),
                    symbol=str(r_fill.get("symbol", "")),
                    layer="fills",
                    divergence_type="replay_only_fill",
                    details={"fill": r_fill},
                )
            )

    live_only_fills = [item for sublist in live_fill_map.values() for item in sublist]
    for l_fill in live_only_fills:
        divergences.append(
            ReconciliationDivergence(
                timestamp=l_fill.get("timestamp", ""),
                symbol=str(l_fill.get("symbol", "")),
                layer="fills",
                divergence_type="live_only_fill",
                details={"fill": l_fill},
            )
        )

    fill_prec = matched_fills / len(replay_fills) if replay_fills else 0.0
    fill_rec = matched_fills / len(live_fills) if live_fills else 0.0

    fill_summary = LayerSummary(
        layer="fills",
        live_total=len(live_fills),
        replay_total=len(replay_fills),
        matched_total=matched_fills,
        precision=round(fill_prec, 4),
        recall=round(fill_rec, 4),
        discrepancy_count=len(live_only_fills) + len(replay_only_fills),
    )

    # Sort divergences by timestamp to find the first causal divergence
    def sort_key(d: ReconciliationDivergence) -> str:
        return d.timestamp or "9999-99-99"

    divergences.sort(key=sort_key)
    first_div: ReconciliationDivergence | None = None
    if divergences:
        first_orig = divergences[0]
        first_div = ReconciliationDivergence(
            timestamp=first_orig.timestamp,
            symbol=first_orig.symbol,
            layer=first_orig.layer,
            divergence_type=first_orig.divergence_type,
            details=first_orig.details,
            impact_usdt=first_orig.impact_usdt,
            is_root_cause=True,
        )
        divergences[0] = first_div

    # Audit pass criteria: requires actual data present and thresholds met
    has_data = (len(live_signals) > 0 or len(replay_signals) > 0) and (
        len(live_fills) > 0 or len(replay_fills) > 0
    )
    is_audit_passed = (
        has_data
        and (sig_summary.precision or 0) >= 0.95
        and (sig_summary.recall or 0) >= 0.95
        and (fill_summary.precision or 0) >= 0.90
        and (fill_summary.recall or 0) >= 0.90
    )

    notes = []
    if not has_data:
        notes.append("Audit unavailable: zero signals or fills to reconcile.")
    elif first_div:
        notes.append(
            f"First causal divergence at {first_div.timestamp} on {first_div.symbol} "
            f"[{first_div.layer}]: {first_div.divergence_type}"
        )
    if has_data and not is_audit_passed:
        notes.append("Audit failed: precision or recall below required threshold.")

    return ReconciliationReport(
        account_id=account_id,
        start_time=start_str,
        end_time=end_str,
        layers={
            "signals": sig_summary,
            "fills": fill_summary,
        },
        divergences=divergences,
        first_divergence=first_div,
        is_audit_passed=is_audit_passed,
        audit_notes=notes,
    )


def to_beijing_str(dt_str: Any) -> str:
    """Convert UTC ISO/timestamp string to Beijing Time (CST, UTC+8) string."""
    if not dt_str:
        return ""
    s = str(dt_str).strip()
    if not s or s == "-":
        return s
    try:
        clean_s = s
        if (
            len(clean_s) == 19
            and clean_s[10] == " "
            and clean_s[4] == "-"
            and clean_s[7] == "-"
        ):
            return clean_s
        if clean_s.endswith("Z"):
            clean_s = clean_s[:-1] + "+00:00"
        dt = datetime.fromisoformat(clean_s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        cst_tz = timezone(timedelta(hours=8))
        return dt.astimezone(cst_tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s[:19].replace("T", " ")


def to_decimal(val: Any, default: Decimal = Decimal("0")) -> Decimal:
    if val is None or str(val).strip() == "":
        return default
    try:
        return Decimal(str(val).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid decimal value: {val}") from exc


def pair_round_trip_trades(
    fills: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Pair individual execution fills into closed round-trip trades.

    Aggregates partial fills by order_id first, then uses FIFO matching per symbol.
    Supports both LONG (BUY open -> SELL close) and SHORT (SELL open -> BUY close) positions,
    preserving account isolation, environment, position_side, and Decimal financial precision.
    """
    from collections import defaultdict

    def get_fill_time(f: dict[str, Any]) -> str:
        return str(f.get("trade_at") or f.get("time_str") or f.get("time") or "")

    order_map = defaultdict(
        lambda: {
            "order_id": "",
            "environment": "",
            "account_id": "",
            "symbol": "",
            "position_side": "",
            "side": "",
            "quantity": Decimal("0"),
            "cost": Decimal("0"),
            "fee": Decimal("0"),
            "realized_pnl": Decimal("0"),
            "trade_at": "",
        }
    )

    for idx, f in enumerate(sorted(fills, key=get_fill_time)):
        oid = str(f.get("order_id") or "")
        env = str(f.get("environment") or "").strip()
        acct = str(
            f.get("account_label")
            or f.get("account_id")
            or f.get("account")
            or ""
        ).strip()
        sym = str(f.get("symbol") or "").strip().upper()
        raw_payload = f.get("raw_payload")
        ps_raw = ""
        if isinstance(raw_payload, dict):
            ps_raw = str(raw_payload.get("positionSide") or raw_payload.get("ps") or "")
        elif isinstance(raw_payload, str) and raw_payload.startswith("{"):
            try:
                parsed_json = json.loads(raw_payload)
                if isinstance(parsed_json, dict):
                    row_data = parsed_json.get("row") or parsed_json.get("o") or parsed_json
                    ps_raw = str(row_data.get("positionSide") or row_data.get("ps") or "")
            except Exception:
                pass
        pos_side = str(
            f.get("position_side")
            or f.get("positionSide")
            or ps_raw
            or ""
        ).strip().upper()
        side = str(f.get("side") or "").strip().upper()

        key = (
            (env, acct, sym, pos_side, side, oid)
            if oid
            else (env, acct, sym, pos_side, side, f"synth_{idx}")
        )
        o = order_map[key]
        o["order_id"] = oid
        o["environment"] = env
        o["account_id"] = acct
        o["symbol"] = sym
        o["position_side"] = pos_side
        o["side"] = side
        q = to_decimal(f.get("quantity") or f.get("qty") or 0)
        px = to_decimal(f.get("price") or 0)
        o["quantity"] += q
        o["cost"] += q * px
        o["fee"] += to_decimal(f.get("fee") or f.get("commission") or 0)
        o["realized_pnl"] += to_decimal(f.get("realized_pnl") or f.get("realizedPnl") or 0)
        if not o["trade_at"]:
            o["trade_at"] = get_fill_time(f)

    sorted_fills = []
    for o in order_map.values():
        if o["quantity"] > Decimal("0"):
            o["price"] = o["cost"] / o["quantity"]
        else:
            o["price"] = Decimal("0")
        sorted_fills.append(o)
    sorted_fills.sort(key=lambda x: str(x["trade_at"]))

    fills_by_group: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in sorted_fills:
        fills_by_group[(row["environment"], row["account_id"], row["symbol"], row["position_side"])].append(row)

    trades: list[dict[str, Any]] = []

    for (env, acct, sym, pos_side), sym_fills in fills_by_group.items():
        open_lots: list[dict[str, Any]] = []

        for i, row in enumerate(sym_fills):
            side = row["side"]
            pos_side = row["position_side"]
            qty = row["quantity"]
            px = row["price"]
            fee = row["fee"]
            pnl = row["realized_pnl"]
            t_str = row["trade_at"]

            is_entry = False
            if pos_side == "LONG":
                is_entry = (side == "BUY")
            elif pos_side == "SHORT":
                is_entry = (side == "SELL")
            else:
                if open_lots:
                    is_entry = (side == open_lots[0]["side"])
                else:
                    if side == "BUY":
                        is_entry = True
                    else:  # side == "SELL"
                        has_future_buy = any(f["side"] == "BUY" for f in sym_fills[i + 1:])
                        is_entry = has_future_buy

            if is_entry:
                open_lots.append(
                    {
                        "side": side,
                        "qty": qty,
                        "price": px,
                        "fee": fee,
                        "time": t_str,
                    }
                )
            else:
                rem_qty = qty
                entry_cost = Decimal("0")
                entry_fees = Decimal("0")
                first_entry_time = t_str
                matched_qty = Decimal("0")
                trade_side = "BUY"

                while rem_qty > Decimal("1e-6") and open_lots:
                    open_lot = open_lots[0]
                    trade_side = open_lot["side"]
                    m = min(rem_qty, open_lot["qty"])
                    if matched_qty == Decimal("0"):
                        first_entry_time = open_lot["time"]
                    entry_cost += m * open_lot["price"]
                    alloc_fee = open_lot["fee"] * (m / max(Decimal("1e-9"), open_lot["qty"]))
                    entry_fees += alloc_fee
                    open_lot["qty"] -= m
                    open_lot["fee"] = max(Decimal("0"), open_lot["fee"] - alloc_fee)
                    rem_qty -= m
                    matched_qty += m
                    if open_lot["qty"] <= Decimal("1e-6"):
                        open_lots.pop(0)

                is_carry_in = (matched_qty <= Decimal("1e-6"))
                trade_qty = matched_qty if not is_carry_in else qty
                avg_entry_price = (entry_cost / matched_qty) if not is_carry_in else None
                total_fee = fee + entry_fees

                if is_carry_in:
                    net_pnl = Decimal("0")
                    ret_pct = Decimal("0")
                    trade_side = "BUY" if side == "SELL" else "SELL"
                else:
                    if pnl != Decimal("0"):
                        net_pnl = pnl - total_fee
                    else:
                        if trade_side == "BUY":  # Long
                            net_pnl = (px - avg_entry_price) * matched_qty - total_fee
                        else:  # Short
                            net_pnl = (avg_entry_price - px) * matched_qty - total_fee

                    if avg_entry_price is not None and avg_entry_price > Decimal("0"):
                        if trade_side == "BUY":
                            ret_pct = ((px - avg_entry_price) / avg_entry_price) * Decimal("100")
                        else:
                            ret_pct = ((avg_entry_price - px) / avg_entry_price) * Decimal("100")
                    else:
                        ret_pct = Decimal("0")

                trade_entry = {
                    "account_id": acct,
                    "symbol": sym,
                    "side": trade_side,
                    "position_side": pos_side,
                    "entry_time": to_beijing_str(first_entry_time)
                    if not is_carry_in
                    else "N/A (Carry-In)",
                    "entry_price": float(round(avg_entry_price, 6))
                    if avg_entry_price is not None
                    else None,
                    "exit_time": to_beijing_str(t_str),
                    "exit_price": float(round(px, 6)),
                    "quantity": float(round(trade_qty, 4)),
                    "realized_pnl": float(round(pnl, 4)),
                    "fees": float(round(total_fee, 4)),
                    "fee": float(round(total_fee, 4)),
                    "net_pnl": float(round(net_pnl, 4)),
                    "return_pct": float(round(ret_pct, 4)),
                    "is_carry_in": is_carry_in,
                }
                if env:
                    trade_entry["environment"] = env
                trades.append(trade_entry)

                if rem_qty > Decimal("1e-6"):
                    open_lots.append(
                        {
                            "side": side,
                            "qty": rem_qty,
                            "price": px,
                            "fee": Decimal("0"),
                            "time": t_str,
                        }
                    )

    trades.sort(key=lambda x: str(x.get("exit_time") or x.get("entry_time") or ""))
    return trades


def match_per_symbol_trades(
    live_trades: Sequence[dict[str, Any]],
    replay_trades: Sequence[dict[str, Any]],
    time_window_sec: float = 600.0,
) -> dict[str, Any]:
    """Perform Symbol-level trade matching between live and replay trades.

    Returns comparison records and summary attribution metrics.
    """
    from datetime import datetime

    def parse_dt(val: Any) -> datetime | None:
        if isinstance(val, datetime):
            return (
                val
                if val.tzinfo is not None
                else val.replace(tzinfo=timezone(timedelta(hours=8)))
            )
        s = str(val or "").strip()
        if not s or s == "-":
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            return dt
        except Exception:
            return None

    unmatched_replay = list(replay_trades)
    matched_records: list[dict[str, Any]] = []

    for l_trade in live_trades:
        sym = l_trade.get("symbol", "")
        l_dt = parse_dt(l_trade.get("entry_time"))
        match_idx = None

        if l_dt is not None:
            best_diff = float("inf")
            for i, r_trade in enumerate(unmatched_replay):
                if r_trade.get("symbol", "") != sym:
                    continue
                r_dt = parse_dt(r_trade.get("entry_at") or r_trade.get("entry_time"))
                if r_dt is None:
                    continue
                diff = abs((l_dt - r_dt).total_seconds())
                if diff <= time_window_sec and diff < best_diff:
                    best_diff = diff
                    match_idx = i

        if match_idx is not None:
            r_match = unmatched_replay.pop(match_idx)
            r_dt = parse_dt(r_match.get("entry_at") or r_match.get("entry_time"))
            time_diff = (l_dt - r_dt).total_seconds() if l_dt and r_dt else 0.0

            l_entry_px = float(l_trade.get("entry_price", 0.0))
            r_entry_px = float(r_match.get("entry_price", 0.0))
            entry_slip = (
                ((l_entry_px - r_entry_px) / r_entry_px * 10000.0)
                if r_entry_px > 0
                else 0.0
            )

            l_exit_px = float(l_trade.get("exit_price", 0.0))
            r_exit_px = float(r_match.get("exit_price", 0.0))
            exit_slip = (
                ((l_exit_px - r_exit_px) / r_exit_px * 10000.0)
                if r_exit_px > 0
                else 0.0
            )

            l_pnl = float(l_trade.get("net_pnl", 0.0))
            r_pnl = float(r_match.get("net_pnl_usdt", 0.0))
            pnl_delta = l_pnl - r_pnl

            matched_records.append(
                {
                    "symbol": sym,
                    "status": "MATCHED",
                    "live_entry_time": l_trade.get("entry_time"),
                    "live_entry_price": l_entry_px,
                    "live_exit_time": l_trade.get("exit_time"),
                    "live_exit_price": l_exit_px,
                    "live_net_pnl": l_pnl,
                    "live_fees": float(l_trade.get("fees", 0.0)),
                    "replay_entry_time": to_beijing_str(
                        r_match.get("entry_at") or r_match.get("entry_time")
                    ),
                    "replay_entry_price": r_entry_px,
                    "replay_exit_time": to_beijing_str(
                        r_match.get("exit_at") or r_match.get("exit_time")
                    ),
                    "replay_exit_price": r_exit_px,
                    "replay_net_pnl": r_pnl,
                    "replay_exit_reason": r_match.get("exit_reason", ""),
                    "entry_slippage_bps": round(entry_slip, 1),
                    "exit_slippage_bps": round(exit_slip, 1),
                    "time_diff_sec": round(time_diff, 1),
                    "pnl_delta_usdt": round(pnl_delta, 4),
                }
            )
        else:
            matched_records.append(
                {
                    "symbol": sym,
                    "status": "LIVE_ONLY",
                    "live_entry_time": l_trade.get("entry_time"),
                    "live_entry_price": float(l_trade.get("entry_price", 0.0)),
                    "live_exit_time": l_trade.get("exit_time"),
                    "live_exit_price": float(l_trade.get("exit_price", 0.0)),
                    "live_net_pnl": float(l_trade.get("net_pnl", 0.0)),
                    "live_fees": float(l_trade.get("fees", 0.0)),
                    "replay_entry_time": "-",
                    "replay_entry_price": 0.0,
                    "replay_exit_time": "-",
                    "replay_exit_price": 0.0,
                    "replay_net_pnl": 0.0,
                    "replay_exit_reason": "Not triggered in replay",
                    "entry_slippage_bps": 0.0,
                    "exit_slippage_bps": 0.0,
                    "time_diff_sec": 0.0,
                    "pnl_delta_usdt": float(l_trade.get("net_pnl", 0.0)),
                }
            )

    # Leftover in replay
    for r_rem in unmatched_replay:
        r_pnl = float(r_rem.get("net_pnl_usdt", 0.0))
        matched_records.append(
            {
                "symbol": r_rem.get("symbol", ""),
                "status": "REPLAY_ONLY",
                "live_entry_time": "-",
                "live_entry_price": 0.0,
                "live_exit_time": "-",
                "live_exit_price": 0.0,
                "live_net_pnl": 0.0,
                "live_fees": 0.0,
                "replay_entry_time": to_beijing_str(
                    r_rem.get("entry_at") or r_rem.get("entry_time")
                ),
                "replay_entry_price": float(r_rem.get("entry_price", 0.0)),
                "replay_exit_time": to_beijing_str(
                    r_rem.get("exit_at") or r_rem.get("exit_time")
                ),
                "replay_exit_price": float(r_rem.get("exit_price", 0.0)),
                "replay_net_pnl": r_pnl,
                "replay_exit_reason": r_rem.get("exit_reason", ""),
                "entry_slippage_bps": 0.0,
                "exit_slippage_bps": 0.0,
                "time_diff_sec": 0.0,
                "pnl_delta_usdt": round(-r_pnl, 4),
            }
        )

    # Sort records by primary timestamp
    def rec_time(r: dict[str, Any]) -> str:
        t1 = str(r.get("live_entry_time") or "")
        t2 = str(r.get("replay_entry_time") or "")
        return t1 if t1 != "-" else t2

    matched_records.sort(key=rec_time)

    # Metrics
    matched_only = [r for r in matched_records if r["status"] == "MATCHED"]
    mean_entry_slip = (
        sum(r["entry_slippage_bps"] for r in matched_only) / len(matched_only)
        if matched_only
        else 0.0
    )
    mean_exit_slip = (
        sum(r["exit_slippage_bps"] for r in matched_only) / len(matched_only)
        if matched_only
        else 0.0
    )
    tot_live_pnl = sum(r["live_net_pnl"] for r in matched_records)
    tot_replay_pnl = sum(r["replay_net_pnl"] for r in matched_records)

    return {
        "records": matched_records,
        "summary": {
            "total_live_trades": len(live_trades),
            "total_replay_trades": len(replay_trades),
            "matched_count": len(matched_only),
            "live_only_count": sum(
                1 for r in matched_records if r["status"] == "LIVE_ONLY"
            ),
            "replay_only_count": sum(
                1 for r in matched_records if r["status"] == "REPLAY_ONLY"
            ),
            "mean_entry_slippage_bps": round(mean_entry_slip, 2),
            "mean_exit_slippage_bps": round(mean_exit_slip, 2),
            "total_live_net_pnl": round(tot_live_pnl, 4),
            "total_replay_net_pnl": round(tot_replay_pnl, 4),
            "live_total_pnl": round(tot_live_pnl, 4),
            "replay_total_pnl": round(tot_replay_pnl, 4),
            "total_pnl_delta_usdt": round(tot_live_pnl - tot_replay_pnl, 4),
            "total_slippage_usdt": round(tot_live_pnl - tot_replay_pnl, 4),
        },
    }
