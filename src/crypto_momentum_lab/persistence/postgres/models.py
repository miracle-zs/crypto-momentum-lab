from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from crypto_momentum_lab.persistence.postgres.base import Base


class ContractMetadataRow(Base):
    __tablename__ = "contract_metadata"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    effective_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    contract_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    quote_asset: Mapped[str] = mapped_column(String(16))
    margin_asset: Mapped[str] = mapped_column(String(16))
    onboard_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class DailyOpenRow(Base):
    __tablename__ = "daily_open_prices"

    utc_day: Mapped[date] = mapped_column(Date, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    open_price: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UniverseSnapshotRow(Base):
    __tablename__ = "universe_snapshots"

    snapshot_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        unique=True,
    )
    utc_day: Mapped[date] = mapped_column(Date)
    config_hash: Mapped[str] = mapped_column(String(64))
    activated: Mapped[bool] = mapped_column(Boolean)


class UniverseEntryRow(Base):
    __tablename__ = "universe_entries"

    snapshot_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("universe_snapshots.snapshot_id", ondelete="CASCADE"),
        primary_key=True,
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    open_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    price_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    utc_day_return: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    gainer_rank: Mapped[int | None] = mapped_column(Integer)
    loser_rank: Mapped[int | None] = mapped_column(Integer)
    is_target: Mapped[bool] = mapped_column(Boolean)
    exclusion_reason: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_universe_entries_snapshot_target", "snapshot_id", "is_target"),
    )


class MonitoringMembershipRow(Base):
    __tablename__ = "monitoring_memberships"

    snapshot_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("universe_snapshots.snapshot_id", ondelete="CASCADE"),
        primary_key=True,
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    status: Mapped[str] = mapped_column(String(16))
    side: Mapped[str | None] = mapped_column(String(16))
    left_target_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )


class RawArchiveManifestRow(Base):
    __tablename__ = "raw_archive_manifests"

    manifest_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
    )
    relative_path: Mapped[str] = mapped_column(Text, unique=True)
    sha256: Mapped[str] = mapped_column(String(64))
    schema_version: Mapped[int] = mapped_column(Integer)
    exchange: Mapped[str] = mapped_column(String(32))
    environment: Mapped[str] = mapped_column(String(32))
    route: Mapped[str] = mapped_column(String(16))
    stream: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str | None] = mapped_column(String(32))
    utc_date: Mapped[date] = mapped_column(Date)
    utc_hour: Mapped[int] = mapped_column(Integer)
    connection_session_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    subscription_generation_min: Mapped[int] = mapped_column(Integer)
    subscription_generation_max: Mapped[int] = mapped_column(Integer)
    row_count: Mapped[int] = mapped_column(Integer)
    compressed_bytes: Mapped[int] = mapped_column(Integer)
    first_exchange_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    last_exchange_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    first_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    capture_version: Mapped[str] = mapped_column(String(64))
    recovery_status: Mapped[str] = mapped_column(String(32))
    known_gap_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MarketDataQualityEventRow(Base):
    __tablename__ = "market_data_quality_events"

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    category: Mapped[str] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    route: Mapped[str | None] = mapped_column(String(16))
    stream: Mapped[str | None] = mapped_column(String(32))
    symbol: Mapped[str | None] = mapped_column(String(32))
    connection_session_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True)
    )
    local_sequence: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[dict[str, object]] = mapped_column(JSONB)


class MarketDataProcessStateRow(Base):
    __tablename__ = "market_data_process_states"

    state_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[str] = mapped_column(String(16))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str | None] = mapped_column(Text)


class RuntimeMarketState15sRow(Base):
    __tablename__ = "runtime_market_states_15s"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    bucket_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    schema_version: Mapped[int] = mapped_column(Integer)
    exchange: Mapped[str] = mapped_column(String(32))
    bucket_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    open_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    high_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    low_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    trade_count: Mapped[int] = mapped_column(Integer)
    trade_notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    aggressive_buy_notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    aggressive_sell_notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    last_bid_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    last_ask_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    midpoint: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    liquidation_count: Mapped[int] = mapped_column(Integer)
    liquidation_notional: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    mark_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    closed_kline_count: Mapped[int] = mapped_column(Integer)
    source_event_count: Mapped[int] = mapped_column(Integer)
    first_received_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    last_received_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source_watermark_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True)
    )
    closure_reason: Mapped[str] = mapped_column(String(32))
    input_sequence_min: Mapped[int | None] = mapped_column(Integer)
    input_sequence_max: Mapped[int | None] = mapped_column(Integer)

    __table_args__ = (
        Index(
            "ix_runtime_market_states_15s_polling",
            "environment",
            "bucket_start",
            "symbol",
        ),
        Index(
            "ix_runtime_market_states_15s_symbol_time",
            "environment",
            "symbol",
            "bucket_start",
        ),
        Index(
            "ix_runtime_market_states_15s_created",
            "environment",
            "created_at",
        ),
    )


class StrategyRunRow(Base):
    __tablename__ = "strategy_runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    run_mode: Mapped[str] = mapped_column(String(16))
    code_commit: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    schema_version: Mapped[int] = mapped_column(Integer)
    source_paths: Mapped[list[str]] = mapped_column(JSONB)
    source_description: Mapped[str] = mapped_column(Text)
    execution_config: Mapped[dict[str, object]] = mapped_column(JSONB)
    input_state_count: Mapped[int] = mapped_column(Integer)
    processed_symbol_count: Mapped[int] = mapped_column(Integer)
    signal_count: Mapped[int] = mapped_column(Integer)
    candidate_count: Mapped[int] = mapped_column(Integer)
    fill_count: Mapped[int] = mapped_column(Integer)
    pending_candidate_count: Mapped[int] = mapped_column(Integer)
    rejection_summary: Mapped[dict[str, object]] = mapped_column(JSONB)
    summary_counts: Mapped[dict[str, object]] = mapped_column(JSONB)
    fill_summary: Mapped[dict[str, object]] = mapped_column(JSONB)


class StrategySignalRow(Base):
    __tablename__ = "strategy_signals"

    signal_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
    )
    strategy_name: Mapped[str] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source_state_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(Text)
    features: Mapped[dict[str, object]] = mapped_column(JSONB)
    reference_prices: Mapped[dict[str, object]] = mapped_column(JSONB)

    __table_args__ = (
        Index(
            "ix_strategy_signals_run_time_symbol",
            "run_id",
            "detected_at",
            "symbol",
        ),
        Index("ix_strategy_signals_run_symbol", "run_id", "symbol"),
    )


class OrderIntentCandidateRow(Base):
    __tablename__ = "order_intent_candidates"

    candidate_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    signal_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_signals.signal_id", ondelete="CASCADE"),
    )
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
    )
    strategy_name: Mapped[str] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    entry_type: Mapped[str] = mapped_column(String(16))
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    desired_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    reduce_only: Mapped[bool] = mapped_column(Boolean)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(Text)
    features: Mapped[dict[str, object]] = mapped_column(JSONB)

    __table_args__ = (
        Index(
            "ix_order_intent_candidates_run_created_symbol",
            "run_id",
            "created_at",
            "symbol",
        ),
        Index("ix_order_intent_candidates_run_symbol", "run_id", "symbol"),
    )


class PaperFillRow(Base):
    __tablename__ = "paper_fills"

    fill_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    candidate_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("order_intent_candidates.candidate_id", ondelete="CASCADE"),
    )
    signal_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_signals.signal_id", ondelete="CASCADE"),
    )
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
    )
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    target_fill_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    requested_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    filled_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    reference_midpoint: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    fill_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    fee: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    total_cost: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    cost_bps: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    reason: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index(
            "ix_paper_fills_run_target_symbol",
            "run_id",
            "target_fill_at",
            "symbol",
        ),
        Index("ix_paper_fills_run_status", "run_id", "status"),
    )


class StrategyCheckpointRow(Base):
    __tablename__ = "strategy_checkpoints"

    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_processed_at_by_symbol: Mapped[dict[str, object]] = mapped_column(JSONB)
    warmup_buckets_by_symbol: Mapped[dict[str, int]] = mapped_column(JSONB)
    cooldown_buckets_remaining_by_symbol: Mapped[dict[str, int]] = (
        mapped_column(JSONB)
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    saved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class StrategyRuntimeEventRow(Base):
    __tablename__ = "strategy_runtime_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    symbol: Mapped[str | None] = mapped_column(String(32))
    bucket_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict[str, object]] = mapped_column(JSONB)

    __table_args__ = (
        Index(
            "ix_strategy_runtime_events_run_time",
            "run_id",
            "occurred_at",
        ),
        Index(
            "ix_strategy_runtime_events_type_time",
            "event_type",
            "occurred_at",
        ),
    )


class StrategyRuntimeCheckpointRow(Base):
    __tablename__ = "strategy_runtime_checkpoints"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    last_processed_at_by_symbol: Mapped[dict[str, object]] = mapped_column(JSONB)
    warmup_buckets_by_symbol: Mapped[dict[str, int]] = mapped_column(JSONB)
    cooldown_buckets_remaining_by_symbol: Mapped[dict[str, int]] = (
        mapped_column(JSONB)
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    saved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
