from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.strategy import deterministic_config_hash


class TradingLeaseState(StrEnum):
    ACTIVE = "active"
    RELEASED = "released"
    EXPIRED = "expired"


class RiskDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    HALTED = "halted"


class StrategyLiveState(StrEnum):
    ACTIVE = "active"
    DRAINING = "draining"
    HALTED = "halted"


@dataclass(frozen=True, slots=True)
class StrategyLiveStateRecord:
    environment: str
    account_label: str
    strategy_name: str
    state: StrategyLiveState
    changed_at: datetime
    reason: str | None

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.strategy_name, "strategy_name")
        if not isinstance(self.state, StrategyLiveState):
            raise ValueError("state must be a StrategyLiveState")
        _require_aware(self.changed_at, "changed_at")


@dataclass(frozen=True, slots=True)
class TradingLease:
    lease_id: str
    environment: str
    account_label: str
    strategy_name: str
    owner: str
    state: TradingLeaseState
    acquired_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.lease_id, "lease_id")
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.strategy_name, "strategy_name")
        _require_non_empty(self.owner, "owner")
        if not isinstance(self.state, TradingLeaseState):
            raise ValueError("state must be a TradingLeaseState")
        _require_aware(self.acquired_at, "acquired_at")
        _require_aware(self.expires_at, "expires_at")
        if self.expires_at <= self.acquired_at:
            raise ValueError("expires_at must be after acquired_at")


@dataclass(frozen=True, slots=True)
class RiskConfigSnapshot:
    environment: str
    account_label: str
    max_order_notional: Decimal | None
    max_gross_notional: Decimal | None
    max_daily_loss: Decimal | None
    max_open_positions: int | None
    max_market_state_age_seconds: float
    max_account_state_age_seconds: float
    allow_reduce_only_while_draining: bool
    created_at: datetime

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        for field_name in ("max_order_notional", "max_gross_notional"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                raise ValueError(f"{field_name} must be positive")
        if self.max_daily_loss is not None and self.max_daily_loss <= 0:
            raise ValueError("max_daily_loss must be positive")
        if self.max_open_positions is not None and self.max_open_positions < 0:
            raise ValueError("max_open_positions must be non-negative")
        if self.max_market_state_age_seconds <= 0:
            raise ValueError("max_market_state_age_seconds must be positive")
        if self.max_account_state_age_seconds <= 0:
            raise ValueError("max_account_state_age_seconds must be positive")
        _require_aware(self.created_at, "created_at")

    @property
    def config_hash(self) -> str:
        values = asdict(self)
        values.pop("created_at")
        return deterministic_config_hash(values)


@dataclass(frozen=True, slots=True)
class RiskHalt:
    halt_id: str
    environment: str
    account_label: str
    reason: str
    active: bool
    created_at: datetime
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.halt_id, "halt_id")
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.reason, "reason")
        _require_aware(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class RiskEvaluation:
    evaluation_id: str
    candidate_id: str
    decision: RiskDecision
    reason: str
    evaluated_at: datetime
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.evaluation_id, "evaluation_id")
        _require_non_empty(self.candidate_id, "candidate_id")
        if not isinstance(self.decision, RiskDecision):
            raise ValueError("decision must be a RiskDecision")
        _require_non_empty(self.reason, "reason")
        _require_aware(self.evaluated_at, "evaluated_at")


def _require_common(environment: str, account_label: str) -> None:
    _require_non_empty(environment, "environment")
    _require_non_empty(account_label, "account_label")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
