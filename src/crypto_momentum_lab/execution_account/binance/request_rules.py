"""Pure parameter and configuration rules for Binance requests."""

from collections.abc import Iterable


def normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({symbol.strip().upper() for symbol in symbols}))
    if any(not symbol for symbol in normalized):
        raise ValueError("fill symbols must not be empty")
    if any("/" in symbol or "\\" in symbol for symbol in normalized):
        raise ValueError("fill symbols must be valid Binance symbols")
    return normalized


def entry_leverage_candidates(requested: int) -> tuple[int, ...]:
    return tuple(dict.fromkeys(max(1, requested - offset) for offset in range(3)))


def require_margin_type(value: str) -> str:
    if value not in {"CROSSED", "ISOLATED"}:
        raise ValueError("margin_type must be CROSSED or ISOLATED")
    return value
