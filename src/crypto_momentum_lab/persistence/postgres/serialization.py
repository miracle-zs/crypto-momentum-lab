from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import JsonValue


def jsonable(value: object) -> JsonValue:
    """Convert persistence payload values to JSON-compatible primitives."""
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return (
            value.astimezone(UTC).isoformat()
            if value.tzinfo is not None and value.utcoffset() is not None
            else value.isoformat()
        )
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)
