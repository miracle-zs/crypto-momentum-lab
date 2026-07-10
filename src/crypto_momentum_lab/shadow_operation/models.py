from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.market.models import JsonValue


@dataclass(frozen=True, slots=True)
class ShadowSession:
    run_id: str
    account_label: str
    strategy_name: str
    strategy_config_hash: str
    state: str
    account_readiness: str
    started_at: datetime
    ended_at: datetime | None
    details: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class ShadowOrderPlan:
    order_plan_id: str
    run_id: str
    order_intent_id: str
    symbol: str
    decision_state: str
    account_readiness: str
    market_freshness: str
    risk_result: str
    state_closed_at: datetime
    created_at: datetime
    order_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        if not self.order_payload:
            raise ValueError("order_payload must not be empty")
        _require_aware(self.state_closed_at, "state_closed_at")
        _require_aware(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class ShadowDecisionMetric:
    metric_id: str
    run_id: str
    symbol: str | None
    category: str
    reason: str | None
    occurred_at: datetime
    details: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class ShadowDrillResult:
    drill_result_id: str
    run_id: str
    drill_name: str
    outcome: str
    occurred_at: datetime
    details: dict[str, JsonValue]


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
