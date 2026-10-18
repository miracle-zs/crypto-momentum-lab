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


# 预置主流标的默认过滤规则 (以防启动时网络瞬断未能拉取 exchangeInfo)
DEFAULT_SYMBOL_FILTERS: dict[str, SymbolFilter] = {
    "BTCUSDT": SymbolFilter(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5.0"),
    ),
    "ETHUSDT": SymbolFilter(
        symbol="ETHUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5.0"),
    ),
    "SOLUSDT": SymbolFilter(
        symbol="SOLUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.01"),
        min_qty=Decimal("0.01"),
        min_notional=Decimal("5.0"),
    ),
    "BNBUSDT": SymbolFilter(
        symbol="BNBUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.01"),
        min_qty=Decimal("0.01"),
        min_notional=Decimal("5.0"),
    ),
    "DOGEUSDT": SymbolFilter(
        symbol="DOGEUSDT",
        tick_size=Decimal("0.00001"),
        step_size=Decimal("1"),
        min_qty=Decimal("1"),
        min_notional=Decimal("5.0"),
    ),
    "1000PEPEUSDT": SymbolFilter(
        symbol="1000PEPEUSDT",
        tick_size=Decimal("0.0000001"),
        step_size=Decimal("1"),
        min_qty=Decimal("1"),
        min_notional=Decimal("5.0"),
    ),
}


class SymbolRulesRegistry:
    """标的交易精度与过滤器注册表"""

    def __init__(self, custom_filters: dict[str, SymbolFilter] | None = None) -> None:
        self._filters: dict[str, SymbolFilter] = dict(DEFAULT_SYMBOL_FILTERS)
        if custom_filters:
            self._filters.update(custom_filters)

    def update_from_exchange_info(self, exchange_info: dict[str, Any]) -> None:
        """从 Binance exchangeInfo 接口响应中动态更新所有交易对的精度规则"""
        if not isinstance(exchange_info, dict):
            return
        symbols_list = exchange_info.get("symbols")
        if not isinstance(symbols_list, list):
            return
        for sym_data in symbols_list:
            if not isinstance(sym_data, dict):
                continue
            symbol = sym_data.get("symbol", "").upper()
            tick_size = Decimal("0.01")
            step_size = Decimal("0.001")
            min_qty = Decimal("0.001")
            min_notional = Decimal("5.0")

            for f in sym_data.get("filters", []):
                filter_type = f.get("filterType")
                if filter_type == "PRICE_FILTER":
                    if "tickSize" in f and Decimal(f["tickSize"]) > 0:
                        tick_size = Decimal(f["tickSize"])
                elif filter_type == "LOT_SIZE":
                    if "stepSize" in f and Decimal(f["stepSize"]) > 0:
                        step_size = Decimal(f["stepSize"])
                    if "minQty" in f and Decimal(f["minQty"]) > 0:
                        min_qty = Decimal(f["minQty"])
                elif filter_type == "MIN_NOTIONAL":
                    if "notional" in f and Decimal(f["notional"]) > 0:
                        min_notional = Decimal(f["notional"])

            self._filters[symbol] = SymbolFilter(
                symbol=symbol,
                tick_size=tick_size,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
            )

    def get_filter(self, symbol: str) -> SymbolFilter:
        sym = symbol.upper()
        if sym in self._filters:
            return self._filters[sym]
        return SymbolFilter(
            symbol=sym,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5.0"),
        )

    def round_quantity(self, symbol: str, raw_qty: Decimal) -> Decimal:
        """根据标的 stepSize 向下取整截断 (Floor)，确保不会超额开仓或超过可用数量"""
        flt = self.get_filter(symbol)
        step = flt.step_size
        if step <= Decimal("0"):
            return raw_qty
        units = (raw_qty / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
        rounded = (units * step).normalize()
        # 避免 normalize 产生科学计数法 (如 1E+2)
        return Decimal(f"{rounded:f}")

    def round_price(self, symbol: str, raw_price: Decimal) -> Decimal:
        """根据标的 tickSize 四舍五入对齐价格"""
        flt = self.get_filter(symbol)
        tick = flt.tick_size
        if tick <= Decimal("0"):
            return raw_price
        units = (raw_price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        rounded = (units * tick).normalize()
        return Decimal(f"{rounded:f}")

    def validate_order(
        self, symbol: str, qty: Decimal, price: Decimal
    ) -> tuple[bool, str]:
        """验证订单是否满足最小数量与最小名义价值"""
        flt = self.get_filter(symbol)
        if qty < flt.min_qty:
            return False, f"数量 {qty} 低于标的最小下单量 {flt.min_qty}"
        notional = qty * price
        if notional < flt.min_notional:
            return (
                False,
                f"名义价值 {notional} 低于标的最小名义价值 {flt.min_notional}",
            )
        return True, ""
