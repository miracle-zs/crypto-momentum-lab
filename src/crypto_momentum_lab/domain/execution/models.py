from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import JsonValue


class ExecutionRunMode(StrEnum):
    PAPER = "paper"
    PAPER_DAEMON = "paper_daemon"
    SHADOW = "shadow"
    LIVE = "live"


@dataclass(frozen=True, slots=True)
class ShadowSuppressionEvent:
    order_plan_id: str
    client_order_id: str
    suppressed_at: datetime
    reason: str
    order_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.order_plan_id, "order_plan_id"),
            (self.client_order_id, "client_order_id"),
            (self.reason, "reason"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.suppressed_at.tzinfo is None or self.suppressed_at.utcoffset() is None:
            raise ValueError("suppressed_at must be timezone-aware")
