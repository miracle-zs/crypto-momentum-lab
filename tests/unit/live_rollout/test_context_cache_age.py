"""Context cache age limits for clean and pending position facts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    _context_cache_can_be_reused,
)
from tests.unit.live_rollout.test_daemon import (
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
