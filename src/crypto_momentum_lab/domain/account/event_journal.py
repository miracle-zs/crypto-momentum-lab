"""Raw account event receipts for durable storage; no exchange payload interpretation."""

from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.market.models import JsonValue


@dataclass(frozen=True, slots=True)
class AccountEventReceipt:
    event_type: str
    event_at: datetime
    received_at: datetime
    payload: dict[str, JsonValue]
    event_id: str
    # ``received_at`` is a local transport timestamp.  Binance's event or
    # transaction timestamp is the only ordering evidence available on the
    # user-data stream for event types that do not expose a sequence number.
    exchange_event_at: datetime | None = None
    exchange_update_id: int | None = None
    exchange_previous_update_id: int | None = None

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type must not be empty")
        if self.event_at.tzinfo is None or self.event_at.utcoffset() is None:
            raise ValueError("event_at must be timezone-aware")
        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        if len(self.event_id) != 64:
            raise ValueError("event_id must be a SHA-256 hex digest")
        if self.exchange_event_at is not None and (
            self.exchange_event_at.tzinfo is None
            or self.exchange_event_at.utcoffset() is None
        ):
            raise ValueError("exchange_event_at must be timezone-aware")
        for value, field_name in (
            (self.exchange_update_id, "exchange_update_id"),
            (self.exchange_previous_update_id, "exchange_previous_update_id"),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{field_name} must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class AccountEventJournalEntry:
    sequence: int
    environment: str
    account_label: str
    receiver_session_id: str
    stream_token: int | None
    event: AccountEventReceipt
