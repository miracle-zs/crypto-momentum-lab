from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    String,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from crypto_momentum_lab.persistence.postgres.base import Base


class LivePositionBatchRow(Base):
    """活跃持仓批次快照表 (支持多账户隔离与同标的多批次重叠共存)"""

    __tablename__ = "live_position_batches"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(64), primary_key=True)

    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    position_side: Mapped[str] = mapped_column(
        String(8), nullable=False
    )  # LONG / SHORT
    quantity: Mapped[Decimal] = mapped_column(
        Numeric(28, 10), nullable=False
    )  # 批次持仓绝对数量 (严格非负)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(28, 10), nullable=False)
    entry_bar_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )  # 独立的 15m K线入场锁定时间
    exit_order_submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False
    )  # ACTIVE / EXITING / CLOSED
    realized_pnl: Mapped[Decimal] = mapped_column(
        Numeric(28, 10), nullable=False, default=Decimal("0")
    )
    exiting_quantity: Mapped[Decimal] = mapped_column(
        Numeric(28, 10), nullable=False, default=Decimal("0")
    )  # 保持 Lot 预留不变量
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint("quantity >= 0", name="live_batches_quantity_non_negative"),
        CheckConstraint(
            "exiting_quantity >= 0", name="live_batches_exiting_qty_non_negative"
        ),
        CheckConstraint(
            "status IN ('ACTIVE', 'EXITING', 'CLOSED')",
            name="live_batches_status_check",
        ),
        Index(
            "idx_live_batches_acc_sym_status",
            "environment",
            "account_label",
            "symbol",
            "status",
        ),
    )


class LiveOrderRow(Base):
    """统一订单生命周期表 (结构化列，逻辑关联与字符长度约束)"""

    __tablename__ = "live_orders"

    environment: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_label: Mapped[str] = mapped_column(String(64), primary_key=True)
    client_order_id: Mapped[str] = mapped_column(
        String(36), primary_key=True
    )  # 严格硬限制 <= 36 字符

    exchange_order_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)  # BUY / SELL
    position_side: Mapped[str] = mapped_column(
        String(8), nullable=False, default="BOTH"
    )  # BOTH / LONG / SHORT
    order_type: Mapped[str] = mapped_column(
        String(16), nullable=False
    )  # LIMIT / MARKET
    quantity: Mapped[Decimal] = mapped_column(Numeric(28, 10), nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric(28, 10), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    filled_quantity: Mapped[Decimal] = mapped_column(
        Numeric(28, 10), nullable=False, default=Decimal("0")
    )
    average_price: Mapped[Decimal] = mapped_column(
        Numeric(28, 10), nullable=False, default=Decimal("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            "length(client_order_id) <= 36",
            name="live_orders_client_id_len_check",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'SUBMITTED', 'OPEN', 'PARTIALLY_FILLED', "
            "'FILLED', 'CANCELED', 'EXPIRED', 'REJECTED', 'RECONCILING')",
            name="live_orders_status_check",
        ),
        Index(
            "idx_live_orders_acc_sym_status",
            "environment",
            "account_label",
            "symbol",
            "status",
        ),
        Index(
            "idx_live_orders_acc_batch",
            "environment",
            "account_label",
            "batch_id",
        ),
    )
