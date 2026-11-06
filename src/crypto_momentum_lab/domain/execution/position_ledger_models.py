"""Position identity and batch models used by execution-head migrations.

Provides strict identity types:
- ``PositionKey``: Fully qualified identity across environment,
  account, symbol, and side;
- ``PositionLedgerBatch``: An entry lot within a specific position episode;
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide


class PositionHealthStatus(StrEnum):
    """
    Authoritative trading health status for a PositionKey per architecture RFC
    2026-09-25.
    """

    READY = "READY"
    CATCHING_UP = "CATCHING_UP"
    INCOMPLETE = "INCOMPLETE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class PositionKey:
    """Explicit identity key for a futures position.

    Prevents callers from relying on ambiguous symbol strings or omitting
    account, environment, or position_side dimensions.
    """

    environment: str
    account_label: str
    symbol: str
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if any(
            ":" in value
            for value in (self.environment, self.account_label, self.symbol)
        ):
            raise ValueError("position key fields must not contain ':'")


@dataclass(frozen=True, slots=True)
class PositionLedgerBatch:
    """One discrete entry lot inside a position episode."""

    batch_id: str
    episode_id: str
    quantity: Decimal
    original_quantity: Decimal
    entry_price: Decimal
    opened_at: datetime
    exit_order_submitted_at: datetime | None = None
    # Ordered distinct opening identities; empty means count evidence is absent.
    entry_order_ids: tuple[str, ...] = ()
    entry_client_order_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if not self.episode_id.strip():
            raise ValueError("episode_id must not be empty")
        if self.quantity < 0:
            raise ValueError("quantity must be non-negative")
        if self.original_quantity <= 0:
            raise ValueError("original_quantity must be positive")
        if self.quantity > self.original_quantity:
            raise ValueError("quantity must not exceed original_quantity")
        if self.entry_price <= 0:
            raise ValueError("entry_price must be positive")
        for identities in (self.entry_order_ids, self.entry_client_order_ids):
            if len(set(identities)) != len(identities) or any(
                not v.strip() for v in identities
            ):
                raise ValueError("entry order identities must be unique and nonempty")
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        if (
            self.exit_order_submitted_at is not None
            and self.exit_order_submitted_at.tzinfo is None
        ):
            raise ValueError("exit_order_submitted_at must be timezone-aware")

