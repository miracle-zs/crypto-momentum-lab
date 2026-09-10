from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast

from crypto_momentum_lab.domain.market.models import JsonValue


def jsonable(value: object) -> JsonValue:
    """Convert strategy reports and checkpoints to JSON-compatible values."""
    if is_dataclass(value) and not isinstance(value, type):
        return jsonable(asdict(cast(Any, value)))
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)
