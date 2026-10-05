"""User-data merge field conversion and evidence payload construction."""

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.user_data_models import UserDataStateError


def require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise UserDataStateError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def require_mapping_list(
    value: object,
    field_name: str,
) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise UserDataStateError(f"{field_name} must be an array")
    return tuple(require_mapping(item, field_name) for item in value)


def required_text(value: object, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise UserDataStateError(f"{field_name} must not be empty")
    return text


def parse_decimal(value: object, field_name: str) -> Decimal:
    if value is None:
        raise UserDataStateError(f"{field_name} is missing")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise UserDataStateError(f"{field_name} is not numeric") from error


def parse_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise UserDataStateError(f"{field_name} must be a boolean")


def parse_timestamp(
    value: object,
    *,
    field_name: str,
) -> datetime:
    if type(value) is not int or value <= 0:
        raise UserDataStateError(f"{field_name} must be a positive integer timestamp")
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise UserDataStateError(f"{field_name} is not a valid timestamp") from error


def event_raw_payload(
    event: BinanceUserDataEvent,
    section: str,
    row: Mapping[str, object],
) -> dict[str, JsonValue]:
    return {
        "source": "user_data_stream",
        "section": section,
        "event_id": event.event_id,
        "event": event.payload,
        "row": {str(key): json_value(item) for key, item in row.items()},
    }


def json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [json_value(item) for item in value]
    return str(value)


def initial_mark_price(
    *,
    entry_price: Decimal,
    position_amt: Decimal,
    unrealized_pnl: Decimal,
) -> Decimal:
    """Build a safe provisional mark for a position-only account update.

    Binance does not include mark price in ``ACCOUNT_UPDATE``.  Deriving it
    from unrealized PnL is exact when PnL is non-zero; entry price is the
    conservative provisional mark at a flat PnL boundary.  The next REST
    snapshot replaces this value with the exchange mark.
    """
    if position_amt != 0 and unrealized_pnl != 0:
        derived = entry_price + unrealized_pnl / position_amt
        if derived > 0:
            return derived
    return entry_price if entry_price > 0 else Decimal("0")
