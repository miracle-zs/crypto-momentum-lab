from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.strategy import deterministic_config_hash


@dataclass(frozen=True, slots=True)
class RiskConfigSnapshot:
    environment: str
    account_label: str
    max_order_notional: Decimal | None
    max_gross_notional: Decimal | None
    max_daily_loss: Decimal | None
    max_open_positions: int | None
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
        _require_aware(self.created_at, "created_at")

    @property
    def config_hash(self) -> str:
        values = asdict(self)
        values.pop("created_at")
        return deterministic_config_hash(values)


def _require_common(environment: str, account_label: str) -> None:
    _require_non_empty(environment, "environment")
    _require_non_empty(account_label, "account_label")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
