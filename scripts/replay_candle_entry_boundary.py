"""Replay B1 and live long exits with or without the entry 15m candle.

This script is intentionally self-contained so it can run inside the server's
strategy container.  It reads only the two requested accounts and their
symbols from PostgreSQL, fetches official Binance 15m OHLC bars, uses the
server's stored 15s bid quotes for executable exits, and emits JSON on stdout.
The caller can then build local CSV/HTML artifacts without transferring the
full runtime market-state table.
"""

from __future__ import annotations

import asyncio
import bisect
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import asyncpg
import httpx

B1_RUN_ID = "paper-account-12-orderflow-b1-long-candle15m-v1"
LIVE_ACCOUNT = "primary"
TARGET_PCT = Decimal("0.0088")
B1_FEE_RATE = Decimal("0.0004")
LIVE_FEE_RATE = Decimal("0.0005")
LIVE_MAKER_FEE_RATE = Decimal("0.0002")
BINANCE_SYMBOL_ALIASES = {"龙虾USDT": "LONGXIAUSDT"}


@dataclass(frozen=True, slots=True)
class Candle:
    symbol: str
    start: datetime
    end: datetime
    open: Decimal
    close: Decimal
    high: Decimal | None = None
    low: Decimal | None = None


@dataclass(frozen=True, slots=True)
class Quote:
    at: datetime
    bid: Decimal | None
    ask: Decimal | None


async def fetch_official_candles(
    symbols: list[str],
    start: datetime,
    end: datetime,
) -> tuple[dict[str, list[Candle]], dict[str, str]]:
    """Fetch official Binance USD-M 15m OHLC bars for the replay window."""

    semaphore = asyncio.Semaphore(8)
    timeout = httpx.Timeout(30.0, connect=10.0)
    limits = httpx.Limits(max_connections=12, max_keepalive_connections=12)
    errors: dict[str, str] = {}
    output: dict[str, list[Candle]] = {}
    request_start = floor_15m(start)
    request_end = floor_15m(end)

    async with httpx.AsyncClient(
        base_url="https://fapi.binance.com",
        timeout=timeout,
        limits=limits,
        trust_env=False,
    ) as client:

        async def one(symbol: str) -> tuple[str, list[Candle]]:
            api_symbol = BINANCE_SYMBOL_ALIASES.get(symbol, symbol)
            cursor = request_start
            rows: dict[int, list[Any]] = {}
            async with semaphore:
                while cursor < request_end:
                    params = {
                        "symbol": api_symbol,
                        "interval": "15m",
                        "startTime": int(cursor.timestamp() * 1000),
                        "endTime": int(request_end.timestamp() * 1000) - 1,
                        "limit": 1500,
                    }
                    last_error: Exception | None = None
                    page: list[Any] | None = None
                    for attempt in range(4):
                        try:
                            response = await client.get(
                                "/fapi/v1/klines", params=params
                            )
                            response.raise_for_status()
                            payload = response.json()
                            if not isinstance(payload, list):
                                raise ValueError(
                                    f"Binance kline response is not a list: {payload}"
                                )
                            page = payload
                            last_error = None
                            break
                        except (
                            httpx.ConnectError,
                            httpx.ReadError,
                            httpx.RemoteProtocolError,
                            httpx.TimeoutException,
                            httpx.HTTPStatusError,
                        ) as exc:
                            last_error = exc
                            if attempt < 3:
                                await asyncio.sleep(0.5 * (attempt + 1))
                    if last_error is not None or page is None:
                        raise RuntimeError(
                            f"failed to load Binance 15m candles for {api_symbol}"
                        ) from last_error
                    if not page:
                        break
                    for raw in page:
                        if not isinstance(raw, list) or len(raw) < 7:
                            continue
                        try:
                            open_ms = int(raw[0])
                        except (TypeError, ValueError):
                            continue
                        rows[open_ms] = raw
                    valid_starts = [
                        int(raw[0])
                        for raw in page
                        if isinstance(raw, list) and raw
                    ]
                    if not valid_starts:
                        break
                    next_cursor = datetime.fromtimestamp(
                        (max(valid_starts) + 900_000) / 1000,
                        tz=UTC,
                    )
                    if next_cursor <= cursor:
                        raise RuntimeError(
                            f"Binance 15m pagination stalled for {api_symbol}"
                        )
                    cursor = next_cursor
                    if len(page) < 1500:
                        break
            candles = []
            for open_ms, raw in sorted(rows.items()):
                candle_start = datetime.fromtimestamp(open_ms / 1000, tz=UTC)
                candle_end = datetime.fromtimestamp(
                    (int(raw[6]) + 1) / 1000,
                    tz=UTC,
                )
                if not request_start <= candle_start < request_end:
                    continue
                if candle_end > request_end:
                    continue
                candles.append(
                    Candle(
                        symbol=symbol,
                        start=candle_start,
                        end=candle_end,
                        open=Decimal(str(raw[1])),
                        close=Decimal(str(raw[4])),
                        high=Decimal(str(raw[2])),
                        low=Decimal(str(raw[3])),
                    )
                )
            return symbol, candles

        tasks = [asyncio.create_task(one(symbol)) for symbol in symbols]
        for index, task in enumerate(asyncio.as_completed(tasks), start=1):
            try:
                symbol, candles = await task
                output[symbol] = candles
                print(
                    f"downloaded {index}/{len(symbols)} {symbol} "
                    f"({len(candles)} 15m candles)",
                    file=sys.stderr,
                )
            except Exception as exc:  # noqa: BLE001
                message = f"{type(exc).__name__}: {exc}"
                errors[f"task_{index}"] = message
                print(
                    f"kline download failed {index}/{len(symbols)}: {message}",
                    file=sys.stderr,
                )
    return output, errors


def floor_15m(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    return value.replace(
        minute=value.minute - value.minute % 15,
        second=0,
        microsecond=0,
    )


def iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def json_value(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return iso(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return value


def decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def close_pnl(
    *,
    entry: Decimal,
    quantity: Decimal,
    entry_fee: Decimal,
    exit_price: Decimal,
    fee_rate: Decimal,
) -> Decimal:
    gross = (exit_price - entry) * quantity
    exit_fee = abs(exit_price * quantity) * fee_rate
    return gross - entry_fee - exit_fee


def fee_rate_for_fill(
    position: dict[str, Any],
    base_rate: Decimal,
    *,
    maker: bool,
) -> Decimal:
    if maker and position.get("account") == "live":
        return LIVE_MAKER_FEE_RATE
    return base_rate


def build_candles(states: list[dict[str, Any]]) -> dict[str, list[Candle]]:
    # The runtime state repeats the latest closed 1m candle on each 15s row.
    # Keep one value per minute, then require all 15 minutes for a closed bar.
    minutes: dict[tuple[str, datetime], tuple[Decimal, Decimal]] = {}
    for row in states:
        minute = row.get("closed_kline_1m_open_time")
        open_price = decimal(row.get("closed_kline_1m_open_price"))
        close_price = decimal(row.get("closed_kline_1m_close_price"))
        if minute is None or open_price is None or close_price is None:
            continue
        minutes[(row["symbol"], minute.astimezone(UTC))] = (
            open_price,
            close_price,
        )
    grouped: dict[str, list[Candle]] = defaultdict(list)
    by_bar: dict[
        tuple[str, datetime], dict[datetime, tuple[Decimal, Decimal]]
    ] = defaultdict(dict)
    for (symbol, minute), values in minutes.items():
        by_bar[(symbol, floor_15m(minute))][minute] = values
    for (symbol, start), values in by_bar.items():
        expected = {
            start + timedelta(minutes=offset) for offset in range(15)
        }
        if set(values) != expected:
            continue
        grouped[symbol].append(
            Candle(
                symbol=symbol,
                start=start,
                end=start + timedelta(minutes=15),
                open=values[start][0],
                close=values[start + timedelta(minutes=14)][1],
            )
        )
    for candles in grouped.values():
        candles.sort(key=lambda candle: candle.end)
    return grouped


def build_quotes(states: list[dict[str, Any]]) -> dict[str, list[Quote]]:
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for row in states:
        bid = decimal(row.get("last_bid_price"))
        ask = decimal(row.get("last_ask_price"))
        if bid is None and ask is None:
            continue
        grouped[row["symbol"]].append(
            Quote(at=row["bucket_end"].astimezone(UTC), bid=bid, ask=ask)
        )
    for quotes in grouped.values():
        quotes.sort(key=lambda quote: quote.at)
    return grouped


def first_adverse(
    candles: list[Candle],
    *,
    opened_at: datetime,
    skip_entry_candle: bool,
) -> Candle | None:
    if skip_entry_candle:
        first_allowed = floor_15m(opened_at) + timedelta(minutes=15)
        candidates = [candle for candle in candles if candle.start >= first_allowed]
    else:
        candidates = [candle for candle in candles if candle.end > opened_at]
    for candle in candidates:
        if candle.close < candle.open:
            return candle
    return None


def replay_position(
    position: dict[str, Any],
    *,
    candles: list[Candle],
    quotes: list[Quote],
    fee_rate: Decimal,
    skip_entry_candle: bool,
) -> dict[str, Any]:
    row = dict(position)
    prefix = "skip" if skip_entry_candle else "current"
    reverse = first_adverse(
        candles,
        opened_at=position["opened_at"],
        skip_entry_candle=skip_entry_candle,
    )
    if reverse is None:
        row.update(
            {
                f"{prefix}_status": "no_reverse_candle",
                f"{prefix}_pnl": None,
                f"{prefix}_closed_at": None,
                f"{prefix}_exit_price": None,
                f"{prefix}_reverse_candle_end": None,
                f"{prefix}_fill_kind": None,
            }
        )
        return row
    entry = position["entry_price"]
    quantity = position["quantity"]
    target = entry * (Decimal("1") + TARGET_PCT)
    deadline = reverse.end + timedelta(minutes=15)
    times = [quote.at for quote in quotes]
    signal_index = bisect.bisect_left(times, reverse.end)
    signal_quote = quotes[signal_index] if signal_index < len(quotes) else None
    signal_price = signal_quote.bid if signal_quote is not None else None
    if signal_price is None:
        signal_price = reverse.close
    if signal_price >= target:
        pnl = close_pnl(
            entry=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=signal_price,
            fee_rate=fee_rate,
        )
        row.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_closed_at": signal_quote.at if signal_quote else reverse.end,
                f"{prefix}_exit_price": signal_price,
                f"{prefix}_target_price": target,
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_fill_kind": "immediate_marketable",
            }
        )
        return row
    for quote in quotes[signal_index + 1 :]:
        if quote.at > deadline:
            break
        if quote.bid is None or quote.bid < target:
            continue
        pnl = close_pnl(
            entry=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=target,
            fee_rate=fee_rate,
        )
        row.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_closed_at": quote.at,
                f"{prefix}_exit_price": target,
                f"{prefix}_target_price": target,
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_fill_kind": "limit_favorable_quote_touch",
            }
        )
        return row
    timeout_index = bisect.bisect_left(times, deadline)
    timeout_quote = (
        quotes[timeout_index]
        if timeout_index < len(quotes) and quotes[timeout_index].at == deadline
        else None
    )
    exit_price = timeout_quote.bid if timeout_quote is not None else None
    fill_kind = "timeout_market_quote"
    if exit_price is None:
        deadline_candle = next(
            (candle for candle in candles if candle.end == deadline),
            None,
        )
        if deadline_candle is None:
            row.update(
                {
                    f"{prefix}_status": "unresolved",
                    f"{prefix}_pnl": None,
                    f"{prefix}_closed_at": None,
                    f"{prefix}_exit_price": None,
                    f"{prefix}_target_price": target,
                    f"{prefix}_reverse_candle_end": reverse.end,
                    f"{prefix}_fill_kind": None,
                }
            )
            return row
        exit_price = deadline_candle.close
        fill_kind = "timeout_official_close_fallback"
    pnl = close_pnl(
        entry=entry,
        quantity=quantity,
        entry_fee=position["entry_fee"],
        exit_price=exit_price,
        fee_rate=fee_rate,
    )
    row.update(
        {
            f"{prefix}_status": "closed",
            f"{prefix}_pnl": pnl,
            f"{prefix}_closed_at": deadline,
            f"{prefix}_exit_price": exit_price,
            f"{prefix}_target_price": target,
            f"{prefix}_reverse_candle_end": reverse.end,
            f"{prefix}_fill_kind": fill_kind,
        }
    )
    return row


def replay_position_official(
    position: dict[str, Any],
    *,
    candles: list[Candle],
    fee_rate: Decimal,
    quotes: list[Quote] | None = None,
    skip_entry_candle: bool,
) -> dict[str, Any]:
    """Replay the live/B1 B1 rule from official 15m OHLC bars.

    When the server bid-quote stream is available, the favorable limit target
    is considered filled on the first bid touch; otherwise the next 15m bar's
    high is used as a conservative OHLC fallback.  This is deliberately paired
    for both variants, so the comparison isolates only the entry-candle
    eligibility boundary.
    """

    row = dict(position)
    prefix = "skip" if skip_entry_candle else "current"
    reverse = first_adverse(
        candles,
        opened_at=position["opened_at"],
        skip_entry_candle=skip_entry_candle,
    )
    if reverse is None:
        row.update(
            {
                f"{prefix}_status": "no_reverse_candle",
                f"{prefix}_pnl": None,
                f"{prefix}_closed_at": None,
                f"{prefix}_exit_price": None,
                f"{prefix}_target_price": position["entry_price"]
                * (Decimal("1") + TARGET_PCT),
                f"{prefix}_reverse_candle_end": None,
                f"{prefix}_fill_kind": None,
            }
        )
        return row
    entry = position["entry_price"]
    quantity = position["quantity"]
    target = entry * (Decimal("1") + TARGET_PCT)
    next_bar = next(
        (candle for candle in candles if candle.start == reverse.end),
        None,
    )
    quote_rows = quotes or []
    if quote_rows:
        quote_times = [quote.at for quote in quote_rows]
        signal_index = bisect.bisect_left(quote_times, reverse.end)
        signal_quote = (
            quote_rows[signal_index]
            if signal_index < len(quote_rows)
            else None
        )
        signal_price = (
            signal_quote.bid
            if signal_quote is not None and signal_quote.bid is not None
            else reverse.close
        )
        if signal_price >= target:
            pnl = close_pnl(
                entry=entry,
                quantity=quantity,
                entry_fee=position["entry_fee"],
                exit_price=signal_price,
                fee_rate=fee_rate,
            )
            row.update(
                {
                    f"{prefix}_status": "closed",
                    f"{prefix}_pnl": pnl,
                    f"{prefix}_closed_at": (
                        signal_quote.at
                        if signal_quote is not None
                        else reverse.end
                    ),
                    f"{prefix}_exit_price": signal_price,
                    f"{prefix}_target_price": target,
                    f"{prefix}_reverse_candle_end": reverse.end,
                    f"{prefix}_fill_kind": "immediate_marketable_quote",
                }
            )
            return row
        deadline = reverse.end + timedelta(minutes=15)
        for quote in quote_rows[signal_index + 1 :]:
            if quote.at > deadline:
                break
            if quote.bid is None or quote.bid < target:
                continue
            pnl = close_pnl(
                entry=entry,
                quantity=quantity,
                entry_fee=position["entry_fee"],
                exit_price=target,
                fee_rate=fee_rate_for_fill(position, fee_rate, maker=True),
            )
            row.update(
                {
                    f"{prefix}_status": "closed",
                    f"{prefix}_pnl": pnl,
                    f"{prefix}_closed_at": quote.at,
                    f"{prefix}_exit_price": target,
                    f"{prefix}_target_price": target,
                    f"{prefix}_reverse_candle_end": reverse.end,
                    f"{prefix}_fill_kind": "limit_target_quote_touch",
                }
            )
            return row
        timeout_index = bisect.bisect_left(quote_times, deadline)
        timeout_quote = (
            quote_rows[timeout_index]
            if timeout_index < len(quote_rows)
            and quote_rows[timeout_index].at == deadline
            else None
        )
        if timeout_quote is not None and timeout_quote.bid is not None:
            exit_price = timeout_quote.bid
            fill_kind = "timeout_market_quote"
        elif next_bar is not None:
            exit_price = next_bar.close
            fill_kind = "timeout_market_at_grace_close"
        else:
            row.update(
                {
                    f"{prefix}_status": "unresolved",
                    f"{prefix}_pnl": None,
                    f"{prefix}_closed_at": None,
                    f"{prefix}_exit_price": None,
                    f"{prefix}_target_price": target,
                    f"{prefix}_reverse_candle_end": reverse.end,
                    f"{prefix}_fill_kind": None,
                }
            )
            return row
        pnl = close_pnl(
            entry=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=exit_price,
            fee_rate=fee_rate_for_fill(
                position,
                fee_rate,
                maker=fill_kind.startswith("limit"),
            ),
        )
        row.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_closed_at": deadline,
                f"{prefix}_exit_price": exit_price,
                f"{prefix}_target_price": target,
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_fill_kind": fill_kind,
            }
        )
        return row
    if reverse.close >= target:
        pnl = close_pnl(
            entry=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=reverse.close,
            fee_rate=fee_rate,
        )
        row.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_closed_at": reverse.end,
                f"{prefix}_exit_price": reverse.close,
                f"{prefix}_target_price": target,
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_fill_kind": "immediate_marketable_close",
            }
        )
        return row
    if next_bar is None or next_bar.high is None:
        row.update(
            {
                f"{prefix}_status": "unresolved",
                f"{prefix}_pnl": None,
                f"{prefix}_closed_at": None,
                f"{prefix}_exit_price": None,
                f"{prefix}_target_price": target,
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_fill_kind": None,
            }
        )
        return row
    if next_bar.high >= target:
        exit_price = target
        closed_at = next_bar.end
        fill_kind = "limit_target_touched_in_grace_bar"
    else:
        exit_price = next_bar.close
        closed_at = next_bar.end
        fill_kind = "timeout_market_at_grace_close"
    pnl = close_pnl(
        entry=entry,
        quantity=quantity,
        entry_fee=position["entry_fee"],
        exit_price=exit_price,
        fee_rate=fee_rate_for_fill(
            position,
            fee_rate,
            maker=fill_kind.startswith("limit"),
        ),
    )
    row.update(
        {
            f"{prefix}_status": "closed",
            f"{prefix}_pnl": pnl,
            f"{prefix}_closed_at": closed_at,
            f"{prefix}_exit_price": exit_price,
            f"{prefix}_target_price": target,
            f"{prefix}_reverse_candle_end": reverse.end,
            f"{prefix}_fill_kind": fill_kind,
        }
    )
    return row


def load_live_positions(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reconstruct one row per filled live entry order, then allocate exits FIFO.

    ``account_fill_events`` is an exchange *fill* ledger, not a strategy-trade
    ledger.  A single MARKET entry order is commonly split into several fills,
    so treating every fill as a separate lot inflates the live trade count.  We
    first aggregate all BUY fills by ``order_id`` (one strategy entry), using a
    quantity-weighted entry price and the sum of entry fees.  SELL fills are
    then allocated FIFO across those aggregated entry orders.  The resulting
    rows therefore answer "how many live entry orders were opened?" rather
    than "how many exchange execution reports were received?".
    """

    buy_orders: dict[tuple[str, str], dict[str, Any]] = {}
    sell_fills: list[dict[str, Any]] = []
    for fill in fills:
        symbol = str(fill["symbol"])
        side = str(fill["side"]).upper()
        if side == "SELL":
            sell_fills.append(fill)
            continue
        if side != "BUY":
            continue

        # Binance order IDs are stable across all partial fills of one order.
        # The trade ID fallback keeps the reconstruction deterministic if an
        # old/imported row has no order ID.
        order_id = str(fill.get("order_id") or fill.get("trade_id"))
        key = (symbol, order_id)
        price = decimal(fill["price"]) or Decimal("0")
        quantity = decimal(fill["quantity"]) or Decimal("0")
        fee = decimal(fill["fee"]) or Decimal("0")
        trade_at = fill["trade_at"].astimezone(UTC)
        group = buy_orders.setdefault(
            key,
            {
                "position_id": f"live:{symbol}:order:{order_id}",
                "entry_order_id": order_id,
                "symbol": symbol,
                "side": "long",
                "opened_at": trade_at,
                "entry_price": Decimal("0"),
                "quantity": Decimal("0"),
                "entry_fee": Decimal("0"),
                "entry_notional": Decimal("0"),
                "actual_pnl": Decimal("0"),
                "actual_closed_at": None,
                "actual_exit_price": None,
                "actual_exit_notional": Decimal("0"),
                "actual_exit_quantity": Decimal("0"),
                "actual_reason": None,
            },
        )
        group["opened_at"] = min(group["opened_at"], trade_at)
        group["quantity"] += quantity
        group["entry_notional"] += price * quantity
        group["entry_fee"] += fee

    lots: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for group in sorted(
        buy_orders.values(), key=lambda row: (row["opened_at"], row["position_id"])
    ):
        quantity = group["quantity"]
        group["entry_price"] = (
            group["entry_notional"] / quantity if quantity else Decimal("0")
        )
        group["remaining_quantity"] = quantity
        lots[group["symbol"]].append(group)

    for fill in sorted(
        sell_fills,
        key=lambda row: (
            row["trade_at"].astimezone(UTC),
            str(row.get("trade_id") or ""),
        ),
    ):
        symbol = str(fill["symbol"])
        price = decimal(fill["price"]) or Decimal("0")
        quantity = decimal(fill["quantity"]) or Decimal("0")
        remaining = quantity
        actual_pnl = decimal(fill.get("realized_pnl")) or Decimal("0")
        trade_at = fill["trade_at"].astimezone(UTC)
        while remaining > 0 and lots[symbol]:
            lot = lots[symbol][0]
            take = min(remaining, lot["remaining_quantity"])
            portion = take / quantity if quantity else Decimal("0")
            lot["remaining_quantity"] -= take
            lot["actual_exit_quantity"] += take
            lot["actual_exit_notional"] += price * take
            lot["actual_pnl"] += actual_pnl * portion
            lot["actual_closed_at"] = trade_at
            lot["actual_reason"] = "exchange_fill"
            if lot["remaining_quantity"] <= 0:
                lots[symbol].pop(0)
            remaining -= take

    output: list[dict[str, Any]] = []
    for group in buy_orders.values():
        row = dict(group)
        exit_quantity = row.pop("actual_exit_quantity")
        exit_notional = row.pop("actual_exit_notional")
        row.pop("remaining_quantity")
        row["actual_exit_price"] = (
            exit_notional / exit_quantity if exit_quantity else None
        )
        output.append(row)
    output.sort(key=lambda row: (row["opened_at"], row["position_id"]))
    return output


async def load_and_replay() -> dict[str, Any]:
    database_url = os.environ["CML_DATABASE_URL"].replace("+asyncpg", "")
    conn = await asyncpg.connect(database_url)
    try:
        b1_rows = await conn.fetch(
            """
            select position_id, symbol, side, opened_at, closed_at,
                   entry_price, quantity, entry_notional, entry_fee,
                   exit_price, realized_pnl, close_reason
            from paper_positions
            where run_id = $1 and side = 'long'
            order by opened_at, position_id
            """,
            B1_RUN_ID,
        )
        live_fills = await conn.fetch(
            """
            select symbol, order_id, trade_id, side, price, quantity,
                   realized_pnl, fee, trade_at
            from account_fill_events
            where environment = 'live' and account_label = $1
            order by trade_at, trade_id
            """,
            LIVE_ACCOUNT,
        )
        b1_positions = [
            {
                "account": "b1",
                "position_id": row["position_id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "opened_at": row["opened_at"].astimezone(UTC),
                "entry_price": row["entry_price"],
                "quantity": row["quantity"],
                "entry_notional": row["entry_notional"],
                "entry_fee": row["entry_fee"],
                "actual_closed_at": (
                    row["closed_at"].astimezone(UTC)
                    if row["closed_at"]
                    else None
                ),
                "actual_exit_price": row["exit_price"],
                "actual_pnl": row["realized_pnl"],
                "actual_reason": row["close_reason"],
            }
            for row in b1_rows
        ]
        live_positions = [
            {"account": "live", **row} for row in load_live_positions(live_fills)
        ]
        positions = b1_positions + live_positions
        if not positions:
            return {"status": "empty", "rows": []}
        symbols = sorted({row["symbol"] for row in positions})
        state_max = await conn.fetchval(
            """
            select max(bucket_end) from runtime_market_states_15s
            where environment = 'research' and symbol = any($1::text[])
            """,
            symbols,
        )
        if state_max is None:
            return {"status": "no_states", "rows": []}
        start = min(row["opened_at"] for row in positions) - timedelta(minutes=30)
        cutoff = floor_15m(state_max)
        candles_by_symbol, candle_errors = await fetch_official_candles(
            symbols,
            start,
            cutoff,
        )
        output: list[dict[str, Any]] = []
        positions_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for position in positions:
            positions_by_symbol[position["symbol"]].append(position)
        quote_rows_count = 0

        def replay_symbol(symbol: str, quote_rows: list[Quote]) -> None:
            nonlocal quote_rows_count
            quote_rows_count += len(quote_rows)
            symbol_candles = candles_by_symbol.get(symbol, [])
            for position in positions_by_symbol[symbol]:
                fee_rate = (
                    B1_FEE_RATE
                    if position["account"] == "b1"
                    else LIVE_FEE_RATE
                )
                result = replay_position_official(
                    position,
                    candles=symbol_candles,
                    fee_rate=fee_rate,
                    quotes=quote_rows,
                    skip_entry_candle=False,
                )
                result = replay_position_official(
                    result,
                    candles=symbol_candles,
                    fee_rate=fee_rate,
                    quotes=quote_rows,
                    skip_entry_candle=True,
                )
                output.append(result)

        async with conn.transaction():
            cursor = conn.cursor(
                """
                select symbol, bucket_end, last_bid_price, last_ask_price
                from runtime_market_states_15s
                where environment = 'research'
                  and symbol = any($1::text[])
                  and bucket_end >= $2
                  and bucket_end <= $3
                order by symbol, bucket_end
                """,
                symbols,
                start,
                state_max,
                prefetch=10000,
            )
            current_symbol: str | None = None
            current_quotes: list[Quote] = []
            seen_quote_symbols: set[str] = set()
            async for record in cursor:
                symbol = str(record["symbol"])
                bid = decimal(record["last_bid_price"])
                ask = decimal(record["last_ask_price"])
                if bid is None and ask is None:
                    continue
                if current_symbol is not None and symbol != current_symbol:
                    replay_symbol(current_symbol, current_quotes)
                    current_quotes = []
                current_symbol = symbol
                seen_quote_symbols.add(symbol)
                current_quotes.append(
                    Quote(
                        at=record["bucket_end"].astimezone(UTC),
                        bid=bid,
                        ask=ask,
                    )
                )
            if current_symbol is not None:
                replay_symbol(current_symbol, current_quotes)
            for symbol in symbols:
                if symbol not in seen_quote_symbols:
                    replay_symbol(symbol, [])
    finally:
        await conn.close()
    return {
        "status": "ok",
        "generated_at": datetime.now(UTC),
        "b1_position_rows": len(b1_positions),
        "live_entry_order_rows": len(live_positions),
        # Kept as a compatibility alias for consumers of the first report;
        # unlike the old version it now has the corrected entry-order count.
        "live_reconstructed_rows": len(live_positions),
        "candle_source": "binance_fapi_15m",
        "candle_cutoff": cutoff,
        "candle_count": sum(len(values) for values in candles_by_symbol.values()),
        "candle_symbols_loaded": len(candles_by_symbol),
        "candle_errors": candle_errors,
        "runtime_quote_rows": quote_rows_count,
        "runtime_state_max": state_max,
        "symbol_count": len(symbols),
        "rows": output,
    }


def main() -> None:
    try:
        result = asyncio.run(load_and_replay())
    except Exception as exc:
        print(json.dumps({"status": "error", "error": repr(exc)}), file=sys.stderr)
        raise
    print(json.dumps(json_value(result), ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
