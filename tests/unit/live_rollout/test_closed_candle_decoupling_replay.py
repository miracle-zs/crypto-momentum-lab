"""Behavioral replay unit tests for S04 and S05 scenarios.

Verifies:
1. S04: IMX 07:15 candle deferred by position sync wait, other symbols evaluate
   without blocking; at 07:18:44 facts advance, IMX re-evaluates at 07:18:44 with
   fresh candidate identity and valid allocation, submitting successfully without
   expired candidate and zero duplicate POST.
2. S04 Expiration: Re-evaluation after strategy timeliness window (>= candle_end + 15m)
   fails with explicit closed_candle_evaluation_expired, notifies operator, clears
   pending event without silent drop or duplicate POST.
3. S05: Stale projection version conflict yields pending_live_context, other symbols
   unblocked; fresh context arrives with updated Book quantity, re-evaluates with new
   candidate identity and new quantity, zero old-quantity POST, zero duplicate POST.
4. Transient network error backoff for one symbol does not block evaluation of other symbols,
   and fact updates do not clear network backoff.
"""

from collections.abc import AsyncIterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio
import pytest

from crypto_momentum_lab.domain.strategy.position_exit import ClosedCandle15m
from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime
from crypto_momentum_lab.live_rollout.exit_failure_policy import ORDER_IDENTITY_CONFLICT_REASON


def _make_candle_event(
    symbol: str,
    candle_start: datetime,
    candle_end: datetime,
    received_at: datetime,
) -> ClosedCandle15mEvent:
    candle = ClosedCandle15m(
        symbol=symbol,
        candle_start=candle_start,
        candle_end=candle_end,
        open_price=Decimal("10.0"),
        close_price=Decimal("9.5"),
    )
    return ClosedCandle15mEvent(
        candle=candle,
        exchange_event_at=candle_end,
        received_at=received_at,
    )


async def test_s04_replay_imx_deferred_at_0715_and_executed_at_071844():
    """S04 replay: IMX candle at 07:15 deferred by position sync wait, BTC evaluates

    immediately; at 07:18:44 facts arrive, IMX re-evaluates with fresh candidate identity
    and succeeds without expired candidate and zero repeat POST.
    """
    t_0700 = datetime(2026, 8, 4, 7, 0, 0, tzinfo=UTC)
    t_0715 = datetime(2026, 8, 4, 7, 15, 0, tzinfo=UTC)
    t_071844 = datetime(2026, 8, 4, 7, 18, 44, tzinfo=UTC)

    imx_event = _make_candle_event("IMXUSDT", t_0700, t_0715, t_0715)
    btc_event = _make_candle_event("BTCUSDT", t_0700, t_0715, t_0715)

    imx_facts_ready = False
    current_time = t_0715
    submissions: list[dict[str, object]] = []
    failures: list[tuple[str, str | None]] = []
    first_eval_done = asyncio.Event()

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            symbol = event.candle.symbol
            if symbol == "IMXUSDT":
                if not imx_facts_ready:
                    first_eval_done.set()
                    return "pending_live_positions:IMXUSDT"
                # Facts ready at 07:18:44: evaluate with fresh candidate
                is_reval = current_time > event.candle.candle_end
                candidate_id = f"exit:{symbol}:{int(event.candle.candle_start.timestamp())}"
                if is_reval:
                    candidate_id += f":reval:{int(current_time.timestamp())}"
                expires_at = current_time + timedelta(seconds=60)
                # Ensure candidate has NOT expired
                assert expires_at > current_time
                submissions.append({
                    "symbol": symbol,
                    "candidate_id": candidate_id,
                    "evaluated_at": current_time,
                    "expires_at": expires_at,
                    "is_reval": is_reval,
                })
                return None
            elif symbol == "BTCUSDT":
                submissions.append({
                    "symbol": symbol,
                    "candidate_id": f"exit:{symbol}:{int(event.candle.candle_start.timestamp())}",
                    "evaluated_at": current_time,
                    "expires_at": current_time + timedelta(seconds=60),
                    "is_reval": False,
                })
                return None
            return None

    def on_failure(symbol, reason):
        failures.append((symbol, reason))

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
        on_exit_failure=on_failure,
    )

    async def source():
        yield imx_event
        yield btc_event

    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        # Wait until initial evaluation completes
        await asyncio.wait_for(first_eval_done.wait(), 1.0)
        await asyncio.sleep(0.05)

        # BTC evaluated immediately at 07:15; IMX deferred
        assert len(submissions) == 1
        assert submissions[0]["symbol"] == "BTCUSDT"
        assert ("IMXUSDT", t_0700) in runtime._pending_candles

        # Advance facts and clock to 07:18:44
        imx_facts_ready = True
        current_time = t_071844
        runtime.note_account_facts_changed(("IMXUSDT",))

        # Channel should finish processing both events
        await asyncio.wait_for(task, 1.0)

        # IMX evaluated and submitted exactly once
        assert len(submissions) == 2
        imx_submission = submissions[1]
        assert imx_submission["symbol"] == "IMXUSDT"
        assert imx_submission["is_reval"] is True
        assert ":reval:1785827924" in imx_submission["candidate_id"]
        assert imx_submission["evaluated_at"] == t_071844
        assert imx_submission["expires_at"] == t_071844 + timedelta(seconds=60)
        assert not runtime._pending_candles
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_s04_replay_imx_expires_after_15m_timeliness_window():
    """S04 replay: IMX candle deferred at 07:15, facts do not arrive until 07:31 (>15m window).

    Re-evaluation is rejected under strategy timeliness rules, operator is notified,
    pending event is cleared, and zero orders are submitted.
    """
    t_0700 = datetime(2026, 8, 4, 7, 0, 0, tzinfo=UTC)
    t_0715 = datetime(2026, 8, 4, 7, 15, 0, tzinfo=UTC)
    t_0731 = datetime(2026, 8, 4, 7, 31, 0, tzinfo=UTC)

    imx_event = _make_candle_event("IMXUSDT", t_0700, t_0715, t_0715)
    current_time = t_0715
    submissions = []
    failures: list[tuple[str, str | None]] = []
    first_eval_done = asyncio.Event()

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            symbol = event.candle.symbol
            if current_time >= event.candle.candle_end + timedelta(minutes=15):
                return f"closed_candle_evaluation_expired:{symbol}"
            first_eval_done.set()
            return f"pending_live_positions:{symbol}"

    def on_failure(symbol, reason):
        failures.append((symbol, reason))

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
        on_exit_failure=on_failure,
    )

    async def source():
        yield imx_event

    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(first_eval_done.wait(), 1.0)
        assert ("IMXUSDT", t_0700) in runtime._pending_candles

        # Advance past 15m window to 07:31:00
        current_time = t_0731
        runtime.note_account_facts_changed(("IMXUSDT",))

        await asyncio.wait_for(task, 1.0)
        assert not runtime._pending_candles
        assert submissions == []
        assert failures == [("IMXUSDT", "closed_candle_evaluation_expired:IMXUSDT")]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_s05_replay_marscoin_stale_projection_conflict_then_fresh_allocation():
    """S05 replay: MARSCOIN encounters stale projection conflict pv1, deferred without

    blocking other symbols; fresh context arrives with pv2 and updated quantity,
    re-evaluation uses new candidate identity and fresh quantity, zero duplicate POST.
    """
    t_0700 = datetime(2026, 8, 4, 7, 0, 0, tzinfo=UTC)
    t_0715 = datetime(2026, 8, 4, 7, 15, 0, tzinfo=UTC)

    marscoin_event = _make_candle_event("MARSCOINUSDT", t_0700, t_0715, t_0715)
    sol_event = _make_candle_event("SOLUSDT", t_0700, t_0715, t_0715)

    projection_version = "pv1"
    submissions: list[dict[str, object]] = []
    conflict_detected = asyncio.Event()

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            symbol = event.candle.symbol
            if symbol == "MARSCOINUSDT":
                if projection_version == "pv1":
                    conflict_detected.set()
                    return "pending_live_context:MARSCOINUSDT"
                # Fresh context with pv2 arrived: allocate updated quantity
                submissions.append({
                    "symbol": symbol,
                    "projection_version": projection_version,
                    "quantity": Decimal("50"),
                    "candidate_id": f"exit:{symbol}:{projection_version}",
                })
                return None
            elif symbol == "SOLUSDT":
                submissions.append({
                    "symbol": symbol,
                    "projection_version": "pv_sol_current",
                    "quantity": Decimal("10"),
                    "candidate_id": f"exit:{symbol}:pv_sol_current",
                })
                return None
            return None

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
    )

    async def source():
        yield marscoin_event
        yield sol_event

    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(conflict_detected.wait(), 1.0)
        await asyncio.sleep(0.05)

        # SOL evaluated immediately without delay; MARSCOIN pending
        assert len(submissions) == 1
        assert submissions[0]["symbol"] == "SOLUSDT"
        assert ("MARSCOINUSDT", t_0700) in runtime._pending_candles

        # Advance MARSCOIN projection to pv2
        projection_version = "pv2"
        runtime.note_account_facts_changed(("MARSCOINUSDT",))

        await asyncio.wait_for(task, 1.0)

        # MARSCOIN evaluated with pv2 and new quantity 50; pv1 had ZERO posts
        assert len(submissions) == 2
        marscoin_submission = submissions[1]
        assert marscoin_submission["symbol"] == "MARSCOINUSDT"
        assert marscoin_submission["projection_version"] == "pv2"
        assert marscoin_submission["quantity"] == Decimal("50")
        assert not runtime._pending_candles
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_symbol_isolation_during_transient_network_backoff():
    """Transient network error backoff on BTCUSDT must not block ETHUSDT closed candle evaluation,

    and facts changed on BTCUSDT must not clear its network backoff.
    """
    t_0700 = datetime(2026, 8, 4, 7, 0, 0, tzinfo=UTC)
    t_0715 = datetime(2026, 8, 4, 7, 15, 0, tzinfo=UTC)

    btc_event = _make_candle_event("BTCUSDT", t_0700, t_0715, t_0715)
    eth_event = _make_candle_event("ETHUSDT", t_0700, t_0715, t_0715)

    btc_attempts = 0
    eth_evaluated = False
    btc_first_failed = asyncio.Event()

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            nonlocal btc_attempts, eth_evaluated
            symbol = event.candle.symbol
            if symbol == "BTCUSDT":
                btc_attempts += 1
                if btc_attempts == 1:
                    btc_first_failed.set()
                    raise ConnectionResetError("network glitch")
                return None
            elif symbol == "ETHUSDT":
                eth_evaluated = True
                return None
            return None

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda err: isinstance(err, ConnectionResetError),
    )

    async def source():
        yield btc_event
        await btc_first_failed.wait()
        yield eth_event

    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(btc_first_failed.wait(), 1.0)
        # Verify BTC entered retry backoff
        assert "BTCUSDT" in runtime._candle_retries

        # ETH should be evaluated immediately even while BTC is in network backoff
        for _ in range(20):
            if eth_evaluated:
                break
            await asyncio.sleep(0.05)
        assert eth_evaluated is True

        # Facts changed on BTCUSDT must NOT clear its network backoff
        runtime.note_account_facts_changed(("BTCUSDT",))
        assert "BTCUSDT" in runtime._candle_retries

        # Wait for the network retry backoff to elapse and BTC to succeed
        await asyncio.wait_for(task, 2.5)
        assert btc_attempts == 2
        assert not runtime._pending_candles
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
