"""Pure parameter and configuration rules for Binance requests."""

from collections.abc import Iterable, Mapping


def normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({symbol.strip().upper() for symbol in symbols}))
    if any(not symbol for symbol in normalized):
        raise ValueError("fill symbols must not be empty")
    if any("/" in symbol or "\\" in symbol for symbol in normalized):
        raise ValueError("fill symbols must be valid Binance symbols")
    return normalized


def normalize_fill_cursors(
    cursors: Mapping[str, int] | None,
) -> dict[str, int]:
    if cursors is None:
        return {}
    normalized: dict[str, int] = {}
    for raw_symbol, raw_cursor in cursors.items():
        symbol = str(raw_symbol).strip().upper()
        if not symbol or "/" in symbol or "\\" in symbol:
            raise ValueError("fill cursor symbols must be valid Binance symbols")
        if isinstance(raw_cursor, bool) or not isinstance(raw_cursor, int):
            raise ValueError("fill cursors must be integer values")
        if raw_cursor < 0:
            raise ValueError("fill cursors must be non-negative")
        normalized[symbol] = raw_cursor
    return normalized


def entry_leverage_candidates(requested: int) -> tuple[int, ...]:
    return tuple(dict.fromkeys(max(1, requested - offset) for offset in range(3)))


def require_margin_type(value: str) -> str:
    if value not in {"CROSSED", "ISOLATED"}:
        raise ValueError("margin_type must be CROSSED or ISOLATED")
    return value
