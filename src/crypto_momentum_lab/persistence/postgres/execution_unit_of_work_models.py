"""Durable state rows used by execution and decision commit transactions."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crypto_momentum_lab.persistence.postgres.base import Base


class ExecutionBookHeadRow(Base):
    """Single CAS head for a position, recording its currently adopted stream."""

    __tablename__ = "execution_book_heads"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    position_side: Mapped[str] = mapped_column(String(8), primary_key=True)
    stream_id: Mapped[str] = mapped_column(String(256), nullable=False)
    stream_epoch: Mapped[str] = mapped_column(String(256), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    projection_version: Mapped[str] = mapped_column(String(64), nullable=False)
    state_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ExecutionEvidenceReceiptRow(Base):
    """Idempotency receipt for one source evidence envelope."""

    __tablename__ = "execution_evidence_receipts"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    position_side: Mapped[str] = mapped_column(String(8), primary_key=True)
    stream_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    stream_epoch: Mapped[str] = mapped_column(String(256), primary_key=True)
    evidence_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    payload_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ExecutionRetiredStreamRow(Base):
    """Permanent fence: a superseded execution epoch cannot be adopted again."""

    __tablename__ = "execution_retired_streams"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    position_side: Mapped[str] = mapped_column(String(8), primary_key=True)
    stream_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    stream_epoch: Mapped[str] = mapped_column(String(256), primary_key=True)
    retired_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    receipts_archived_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )


class ExecutionTradeIdentityRow(Base):
    """Stable account-position identity of a trade across stream epochs."""

    __tablename__ = "execution_trade_identities"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    position_side: Mapped[str] = mapped_column(String(8), primary_key=True)
    trade_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    payload_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_execution_trade_quantity"),
        CheckConstraint("price > 0", name="ck_execution_trade_price"),
    )


class ExecutionOrderWatermarkRow(Base):
    """Monotonic cumulative watermark retained across source stream epochs."""

    __tablename__ = "execution_order_watermarks"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    position_side: Mapped[str] = mapped_column(String(8), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    cumulative_quantity: Mapped[Decimal] = mapped_column(
        Numeric(38, 18), nullable=False
    )
    cumulative_quote: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class DurablePolicyStateRow(Base):
    """CAS-protected durable policy state for one runtime strategy identity."""

    __tablename__ = "durable_policy_states"

    policy_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    policy_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    state_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    state_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    last_decision_id: Mapped[str] = mapped_column(String(128), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class DurablePolicyCommitRow(Base):
    """Immutable idempotency record for one committed policy transition."""

    __tablename__ = "durable_policy_commits"

    decision_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    policy_key: Mapped[str] = mapped_column(String(256), nullable=False)
    # commit_digest already commits to both state digests, trace, dependencies,
    # and accepted exit. Keep the permanent decision identity and this digest;
    # redundant hex digests are not additional idempotency protection.
    commit_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class DurableDecisionExitRow(Base):
    """Recoverable outbox for accepted exits committed with policy and trace."""

    __tablename__ = "durable_decision_exits"

    decision_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    policy_key: Mapped[str] = mapped_column(String(256), nullable=False)
    command_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    command_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dispatch_receipt: Mapped[str | None] = mapped_column(Text)
    disposition_reason: Mapped[str | None] = mapped_column(Text)
