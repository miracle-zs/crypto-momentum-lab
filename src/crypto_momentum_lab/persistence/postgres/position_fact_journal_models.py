"""Database rows for versioned account position facts and recovery checkpoints."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crypto_momentum_lab.persistence.postgres.base import Base


class PositionFactJournalEventRow(Base):
    __tablename__ = "position_fact_journal_events"

    event_record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    environment: Mapped[str] = mapped_column(String(32), nullable=False)
    account_label: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    position_side: Mapped[str] = mapped_column(String(16), nullable=False)
    stream_id: Mapped[str] = mapped_column(String(256), nullable=False)
    stream_epoch: Mapped[str] = mapped_column(String(256), nullable=False)
    event_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    event_id: Mapped[str] = mapped_column(String(256), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        Index(
            "ix_position_fact_events_scope_occurred",
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
            "occurred_at",
        ),
        Index(
            "ix_position_fact_events_scope_recorded",
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
            "recorded_at",
        ),
    )


class PositionRecoveryCheckpointRow(Base):
    __tablename__ = "position_recovery_checkpoints"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    position_side: Mapped[str] = mapped_column(String(16), primary_key=True)
    stream_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    stream_epoch: Mapped[str] = mapped_column(String(256), primary_key=True)
    checkpoint_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    event_cut: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    facts_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        Index(
            "ix_position_recovery_checkpoints_latest",
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
            "event_cut",
        ),
    )
