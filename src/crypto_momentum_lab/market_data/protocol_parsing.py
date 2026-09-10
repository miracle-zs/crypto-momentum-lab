from collections.abc import Callable, Mapping
from datetime import datetime


def require_string(
    payload: Mapping[str, object],
    name: str,
    *,
    error: Callable[[str], Exception],
) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise error(f"{name} must be a non-empty string")
    return value


def require_datetime(
    payload: Mapping[str, object],
    name: str,
    *,
    error: Callable[[str], Exception],
    value_message: str = "must be an ISO datetime",
    invalid_message: str = "must be an ISO datetime",
    timezone_message: str = "must be timezone-aware",
) -> datetime:
    value = payload.get(name)
    if not isinstance(value, str):
        raise error(f"{name} {value_message}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as cause:
        raise error(f"{name} {invalid_message}") from cause
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise error(f"{name} {timezone_message}")
    return parsed
