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
