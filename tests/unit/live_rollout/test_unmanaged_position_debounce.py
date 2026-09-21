"""Tests for unmanaged position debounce and context cache short-TTL recovery."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    _context_cache_can_be_reused,
)
from tests.unit.live_rollout.test_daemon import (
    PlanAwareExchange,
    _daemon,
    _runtime_context,
)


def _market_state(
    symbol: str = "BTCUSDT",
    bucket_start: datetime | None = None,
) -> MarketState15s:
    start = bucket_start or datetime(2026, 8, 4, 0, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol=symbol,
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=None,
        low_price=None,
        close_price=Decimal("100"),
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("50"),
        aggressive_sell_notional=Decimal("50"),
        last_bid_price=Decimal("99"),
        last_ask_price=Decimal("101"),
        spread=Decimal("2"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=start,
        last_received_at=start,
    )


def test_context_cache_reused_normally_for_clean_context() -> None:
    now = datetime(2026, 8, 4, 0, 0, 10, tzinfo=UTC)
    state = _market_state(bucket_start=datetime(2026, 8, 4, 0, 0, 0, tzinfo=UTC))
    clean_context = replace(
        _runtime_context(),
        now=now,
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
    )
    # 10s age, max 30s -> reused
    assert _context_cache_can_be_reused(
        state=state,
        cached_bucket_start=state.bucket_start,
        cached_loaded_at=now - timedelta(seconds=10),
        now=now,
        max_age_seconds=30,
        cached_context=clean_context,
    )


def test_context_cache_abnormal_positions_enforce_short_debounce() -> None:
    now = datetime(2026, 8, 4, 0, 0, 10, tzinfo=UTC)
    state = _market_state(bucket_start=datetime(2026, 8, 4, 0, 0, 0, tzinfo=UTC))
    abnormal_context = replace(
        _runtime_context(),
        now=now,
        pending_position_symbols=frozenset({"BTCUSDT"}),
        unmanaged_position_symbols=frozenset(),
    )
    # 0.2s age < 0.5s abnormal TTL -> can be reused for microsecond debounce
    assert _context_cache_can_be_reused(
        state=state,
        cached_bucket_start=state.bucket_start,
        cached_loaded_at=now - timedelta(seconds=0.2),
        now=now,
        max_age_seconds=30,
        cached_context=abnormal_context,
        abnormal_max_age_seconds=0.5,
    )
    # 1.0s age >= 0.5s abnormal TTL -> REJECTED to force fresh DB reload
    assert not _context_cache_can_be_reused(
        state=state,
        cached_bucket_start=state.bucket_start,
        cached_loaded_at=now - timedelta(seconds=1.0),
        now=now,
        max_age_seconds=30,
        cached_context=abnormal_context,
        abnormal_max_age_seconds=0.5,
    )


def test_context_cache_same_bucket_never_bypasses_max_age() -> None:
    now = datetime(2026, 8, 4, 0, 2, 0, tzinfo=UTC)
    state = _market_state(bucket_start=datetime(2026, 8, 4, 0, 0, 0, tzinfo=UTC))
    clean_context = replace(
        _runtime_context(),
        now=now,
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
    )
    # Age is 120s, max age is 30s. Even though state is same bucket, must NOT reuse.
    assert not _context_cache_can_be_reused(
        state=state,
        cached_bucket_start=state.bucket_start,
        cached_loaded_at=now - timedelta(seconds=120),
        now=now,
        max_age_seconds=30,
        cached_context=clean_context,
    )


@pytest.mark.asyncio
async def test_market_loop_debounces_unmanaged_position_and_recovers() -> None:
    """When an unmanaged position appears briefly and clears, daemon does not halt."""
    now = datetime(2026, 8, 4, 0, 0, 0, tzinfo=UTC)
    exchange = PlanAwareExchange()
    call_count = 0

    async def dynamic_context(state: object) -> LiveDaemonRuntimeContext:
        nonlocal call_count
        call_count += 1
        del state
        # First call has unmanaged position, second call clears it
        unmanaged = frozenset({"ETHUSDT"}) if call_count == 1 else frozenset()
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"ETHUSDT"}) if unmanaged else frozenset(),
            unmanaged_position_symbols=unmanaged,
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=dynamic_context,
        unmanaged_halt_debounce_seconds=15.0,
    )

    async def states() -> AsyncIterator[MarketState15s]:
        # State 1 at t=0
        yield _market_state("ETHUSDT", bucket_start=now)
        # State 2 at t=15s
        yield _market_state("ETHUSDT", bucket_start=now + timedelta(seconds=15))

    result = await daemon.run(states())

    # Daemon processed both states without halting because unmanaged position resolved!
    assert result.processed_state_count == 2
    assert result.halt_reason is None
    # Entries were blocked during debounce on state 1, resumed on state 2
    assert exchange.calls == ["submit"]
