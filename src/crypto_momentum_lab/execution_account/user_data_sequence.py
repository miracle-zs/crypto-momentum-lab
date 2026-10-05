"""Pure exchange user-data sequence validation without watermark ownership."""

from datetime import datetime

from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.user_data_models import UserDataStateError


def validate_exchange_update_watermark(
    event: BinanceUserDataEvent,
    last_update_id: int | None,
) -> None:
    update_id = event.exchange_update_id
    if update_id is None:
        return
    if last_update_id is None:
        return
    previous_update_id = event.exchange_previous_update_id
    if previous_update_id is not None and previous_update_id != last_update_id:
        raise UserDataStateError("exchange user-data update sequence is not contiguous")
    if update_id <= last_update_id:
        raise UserDataStateError("exchange user-data update watermark moved backwards")


def stale_user_data_reason(
    event: BinanceUserDataEvent,
    *,
    last_exchange_event_at: datetime | None,
) -> str | None:
    """Reject events that regress the exchange event-time watermark."""
    if event.exchange_event_at is not None:
        if (
            last_exchange_event_at is not None
            and event.exchange_event_at < last_exchange_event_at
        ):
            return "stale_exchange_event"
    return None


def positive_exchange_milliseconds(value: object) -> int | None:
    """Accept exchange integer timestamps only; zero denotes no evidence."""
    if type(value) is int and value > 0:
        return value
    return None
