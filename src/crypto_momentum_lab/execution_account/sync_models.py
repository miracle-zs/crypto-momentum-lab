"""Small account reconciliation context and fill identity types."""

from dataclasses import dataclass
from datetime import datetime

type FillKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class AccountReconciliationContext:
    environment: str
    account_label: str
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
