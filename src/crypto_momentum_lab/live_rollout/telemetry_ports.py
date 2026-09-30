"""Small telemetry capabilities consumed by independent runtime publishers."""

from datetime import datetime
from typing import Protocol


class ConsumerHealthSink(Protocol):
    def consumer_health(
        self,
        *,
        consumer: str,
        available: bool,
        occurred_at: datetime,
        reason: str | None = None,
        recovery: bool = False,
        lag: bool = False,
        sequence: int | None = None,
    ) -> None: ...

