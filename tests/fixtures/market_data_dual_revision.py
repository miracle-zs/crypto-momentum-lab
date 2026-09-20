"""Market data dual-revision fixture: decision-visible vs canonical revision.

Provides structured market data revisions for identical time buckets:
1. Decision-Visible Revision (v1): The exact snapshot observed at real-time decision time.
2. Canonical Revision (v2): The authoritative repaired/materialized revision in Parquet storage.

Used to test:
- Decision revision tagging and auditability;
- Multi-mode replay (reproducing live observations vs evaluating canonical datasets);
- Prevention of lookahead leakage in backtests.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s


@dataclass(frozen=True, slots=True)
class MarketDataRevisionEnvelope:
    """Carries market state along with its revision, progress, and lineage."""

    revision_id: str
    symbol: str
    bucket_start: datetime
    bucket_end: datetime
    state: MarketState15s
    is_canonical: bool
    progress_stage: str  # "observed" | "accepted_durable" | "materialized"
    quality_notes: str


def build_dual_revision_dataset(
    *,
    symbol: str = "BTCUSDT",
    base_time: datetime | None = None,
) -> tuple[MarketDataRevisionEnvelope, MarketDataRevisionEnvelope]:
    """Generate a dual-revision pair for a single 15-second bucket.

    Bucket: [T, T+15s]
    - Decision-Visible (v1): Received at T+15s + 50ms. Missing 2 late-arriving ticks; close=65000.
    - Canonical (v2): Materialized at T+15s + 2s. Repaired with late ticks; close=65020.
    """
    t0 = base_time or datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=15)

    v1_state = MarketState15s(
        schema_version=1,
        environment="live",
        exchange="binance",
        symbol=symbol,
        bucket_start=t0,
        bucket_end=t1,
        open_price=Decimal("64980.00"),
        high_price=Decimal("65010.00"),
        low_price=Decimal("64975.00"),
        close_price=Decimal("65000.00"),
        trade_count=142,
        trade_notional=Decimal("500000.00"),
        aggressive_buy_notional=Decimal("260000.00"),
        aggressive_sell_notional=Decimal("240000.00"),
        last_bid_price=Decimal("64999.50"),
        last_ask_price=Decimal("65000.50"),
        spread=Decimal("1.00"),
        midpoint=Decimal("65000.00"),
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=Decimal("65000.10"),
        closed_kline_count=0,
        source_event_count=150,
        first_received_at=t0 + timedelta(milliseconds=100),
        last_received_at=t1 + timedelta(milliseconds=45),
        data_complete=False,
        missing_agg_trade_count=2,
        is_backfill=False,
    )

    v1_envelope = MarketDataRevisionEnvelope(
        revision_id=f"{symbol}:{t0.isoformat()}:v1_observed",
        symbol=symbol,
        bucket_start=t0,
        bucket_end=t1,
        state=v1_state,
        is_canonical=False,
        progress_stage="observed",
        quality_notes="Real-time stream at candle close; 2 late trades pending",
    )

    v2_state = MarketState15s(
        schema_version=1,
        environment="live",
        exchange="binance",
        symbol=symbol,
        bucket_start=t0,
        bucket_end=t1,
        open_price=Decimal("64980.00"),
        high_price=Decimal("65025.00"),  # Late tick pushed high higher
        low_price=Decimal("64975.00"),
        close_price=Decimal("65020.00"),  # Late tick updated close price
        trade_count=144,
        trade_notional=Decimal("512000.00"),
        aggressive_buy_notional=Decimal("272000.00"),
        aggressive_sell_notional=Decimal("240000.00"),
        last_bid_price=Decimal("65019.50"),
        last_ask_price=Decimal("65020.50"),
        spread=Decimal("1.00"),
        midpoint=Decimal("65020.00"),
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=Decimal("65020.10"),
        closed_kline_count=0,
        source_event_count=152,
        first_received_at=t0 + timedelta(milliseconds=100),
        last_received_at=t1 + timedelta(seconds=2),
        data_complete=True,
        missing_agg_trade_count=0,
        is_backfill=True,
    )

    v2_envelope = MarketDataRevisionEnvelope(
        revision_id=f"{symbol}:{t0.isoformat()}:v2_canonical",
        symbol=symbol,
        bucket_start=t0,
        bucket_end=t1,
        state=v2_state,
        is_canonical=True,
        progress_stage="materialized",
        quality_notes="Authoritative Parquet materialization with gap repair",
    )

    return v1_envelope, v2_envelope
