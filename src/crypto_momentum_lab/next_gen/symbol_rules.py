from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Any

import structlog

log = structlog.get_logger("symbol_rules")


@dataclass(frozen=True)
class SymbolFilter:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    price_multiplier_up: Decimal = Decimal("0")
    price_multiplier_down: Decimal = Decimal("0")


class SymbolRulesRegistry:
    """标的交易精度与过滤器注册表"""

    def __init__(self, custom_filters: dict[str, SymbolFilter] | None = None) -> None:
        self._filters = dict(custom_filters) if custom_filters is not None else {}

    def update_from_exchange_info(self, exchange_info: dict[str, Any]) -> None:
        """Load exact exchange filters; missing fields are configuration errors."""
        parsed = {}
        for row in exchange_info["symbols"]:
            if not isinstance(row, dict):
                raise TypeError("exchange symbol entries must be mappings")
            filters: dict[Any, Any] = {}
            for item in row.get("filters", ()):
                if not isinstance(item, dict):
                    raise TypeError("exchange filter entries must be mappings")
                filters[item.get("filterType")] = item
            price = filters.get("PRICE_FILTER")
            lot = filters.get("LOT_SIZE")
            notional = filters.get("MIN_NOTIONAL")
            if price is None or lot is None or notional is None:
                # Delisted/settling symbols can omit one of the filters. They
                # are simply not tradable here; do not fail the whole startup.
                log.warning(
                    "symbol_rules_missing_filters",
                    symbol=row.get("symbol"),
                )
                continue
            percent = filters.get("PERCENT_PRICE")
            multiplier_up = (
                Decimal(percent.get("multiplierUp", "0"))
                if percent is not None
                else Decimal("0")
            )
            multiplier_down = (
                Decimal(percent.get("multiplierDown", "0"))
                if percent is not None
                else Decimal("0")
            )
            rule = SymbolFilter(
                symbol=row["symbol"],
                tick_size=Decimal(price["tickSize"]),
                step_size=Decimal(lot["stepSize"]),
                min_qty=Decimal(lot["minQty"]),
                min_notional=Decimal(notional["notional"]),
                price_multiplier_up=multiplier_up,
                price_multiplier_down=multiplier_down,
            )
            values = (rule.tick_size, rule.step_size, rule.min_qty, rule.min_notional)
            if any(not value.is_finite() or value <= 0 for value in values):
                raise ValueError(f"Invalid exchange filters for {rule.symbol}")
            parsed[rule.symbol] = rule
        self._filters = parsed

    def has_filter(self, symbol: str) -> bool:
        return symbol.upper() in self._filters

    def get_filter(self, symbol: str) -> SymbolFilter:
        try:
            return self._filters[symbol.upper()]
        except KeyError as error:
            raise ValueError(f"Missing exchange filters for {symbol}") from error

    def round_quantity(self, symbol: str, raw_qty: Decimal) -> Decimal:
        """根据标的 stepSize 向下取整截断 (Floor)，确保不会超额开仓或超过可用数量"""
        flt = self.get_filter(symbol)
        step = flt.step_size
        units = (raw_qty / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
        rounded = (units * step).normalize()
        # 避免 normalize 产生科学计数法 (如 1E+2)
        return Decimal(f"{rounded:f}")

    def round_price(self, symbol: str, raw_price: Decimal) -> Decimal:
        """根据标的 tickSize 四舍五入对齐价格"""
        flt = self.get_filter(symbol)
        tick = flt.tick_size
        units = (raw_price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        rounded = (units * tick).normalize()
        return Decimal(f"{rounded:f}")

    def price_allowed(
        self, symbol: str, price: Decimal, reference_price: Decimal | None
    ) -> bool:
        """Check Binance PERCENT_PRICE against a mark/reference price.

        A missing filter or reference is permissive; the exchange remains the
        final authority.  When present, reject locally instead of sending an
        order that the exchange will refuse after a fast move.
        """
        if reference_price is None or reference_price <= 0 or price <= 0:
            return True
        flt = self.get_filter(symbol)
        if flt.price_multiplier_up <= 0 or flt.price_multiplier_down <= 0:
            return True
        return (
            reference_price * flt.price_multiplier_down
            <= price
            <= reference_price * flt.price_multiplier_up
        )

    def validate_order(
        self,
        symbol: str,
        qty: Decimal,
        price: Decimal,
        reference_price: Decimal | None = None,
    ) -> tuple[bool, str]:
        """验证订单是否满足最小数量、最小名义价值与价格带"""
        flt = self.get_filter(symbol)
        if qty < flt.min_qty:
            return False, f"数量 {qty} 低于标的最小下单量 {flt.min_qty}"
        notional = qty * price
        if notional < flt.min_notional:
            return (
                False,
                f"名义价值 {notional} 低于标的最小名义价值 {flt.min_notional}",
            )
        if not self.price_allowed(symbol, price, reference_price):
            return False, "价格超出币安 PERCENT_PRICE 价格带"
        return True, ""
