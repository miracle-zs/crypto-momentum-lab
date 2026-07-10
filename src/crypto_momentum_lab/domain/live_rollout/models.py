from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import JsonValue

LIVE_APPROVAL_CONFIRMATION = "ENABLE SMALL LIVE TRADING"


class LiveGateStatus(StrEnum):
    APPROVED = "approved"
    BLOCKED = "blocked"


class LiveSessionState(StrEnum):
    PREFLIGHT = "preflight"
    SHADOW_PREFLIGHT = "shadow_preflight"
    LIVE_ENABLED = "live_enabled"
    DRAINING = "draining"
    HALTED = "halted"
    RECONCILING = "reconciling"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class LiveOperatorApproval:
    approval_id: str
    account_label: str
    strategy_name: str
    strategy_config_hash: str
    risk_config_hash: str
    git_commit_hash: str
    database_migration_revision: str
    approved_notional_cap: Decimal
    approved_max_open_positions: int
    approved_max_daily_loss: Decimal
    approver_name: str
    approval_text: str
    expires_at: datetime
    created_at: datetime

    def __post_init__(self) -> None:
        if self.approval_text != LIVE_APPROVAL_CONFIRMATION:
            raise ValueError("approval_text does not match confirmation phrase")
        if self.approved_notional_cap <= 0:
            raise ValueError("approved_notional_cap must be positive")
        if self.approved_max_open_positions <= 0:
            raise ValueError("approved_max_open_positions must be positive")
        if self.approved_max_daily_loss <= 0:
            raise ValueError("approved_max_daily_loss must be positive")
        _require_aware(self.created_at, "created_at")
        _require_aware(self.expires_at, "expires_at")
        if self.expires_at <= self.created_at:
            raise ValueError("approval expiration must be after creation")


@dataclass(frozen=True, slots=True)
class LiveGateDecision:
    status: LiveGateStatus
    reasons: tuple[str, ...]

    @property
    def approved(self) -> bool:
        return self.status is LiveGateStatus.APPROVED


@dataclass(frozen=True, slots=True)
class LiveSessionTransition:
    transition_id: str
    session_id: str
    state: LiveSessionState
    occurred_at: datetime
    operator: str
    strategy_config_hash: str
    risk_config_hash: str
    reason: str | None
    details: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class RollbackCommand:
    command_id: str
    command_type: str
    requested_by: str
    confirmation_text: str
    requested_at: datetime
    idempotency_key: str
    account_label: str
    strategy_name: str
    session_id: str
    status: str
    completed_at: datetime | None
    failure_reason: str | None


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
