"""Interpret received Binance HTTP responses without issuing requests."""

import math

import httpx


def exchange_error_code(exc: httpx.HTTPStatusError) -> int | None:
    try:
        payload = exc.response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    code = payload.get("code")
    return int(code) if code is not None else None


def retry_after_seconds(response: httpx.Response) -> float | None:
    raw_value = response.headers.get("Retry-After")
    if raw_value is None:
        return None
    try:
        value = float(raw_value)
    except ValueError:
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return value


def is_invalid_leverage_rejection(exc: httpx.HTTPStatusError) -> bool:
    """Return whether Binance rejected only the requested leverage level."""
    return exchange_error_code(exc) == -4028


def exchange_error_message(exc: httpx.HTTPStatusError) -> str:
    try:
        payload = exc.response.json()
    except ValueError:
        return f"Binance rejected order with HTTP {exc.response.status_code}"
    if isinstance(payload, dict) and payload.get("msg"):
        return str(payload["msg"])
    return f"Binance rejected order with HTTP {exc.response.status_code}"
