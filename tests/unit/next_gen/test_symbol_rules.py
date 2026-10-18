from __future__ import annotations

from decimal import Decimal

from crypto_momentum_lab.next_gen.symbol_rules import (
    SymbolRulesRegistry,
)


def test_symbol_rules_defaults() -> None:
    registry = SymbolRulesRegistry()
    flt = registry.get_filter("BTCUSDT")
    assert flt.step_size == Decimal("0.001")
    assert flt.tick_size == Decimal("0.10")
    assert flt.min_qty == Decimal("0.001")
    assert flt.min_notional == Decimal("5.0")

    doge = registry.get_filter("DOGEUSDT")
    assert doge.step_size == Decimal("1")
    assert doge.tick_size == Decimal("0.00001")


def test_symbol_rules_round_quantity_floored() -> None:
    registry = SymbolRulesRegistry()
    # BTCUSDT step size = 0.001
    assert registry.round_quantity("BTCUSDT", Decimal("0.1239")) == Decimal("0.123")
    assert registry.round_quantity("BTCUSDT", Decimal("0.0011")) == Decimal("0.001")
    assert registry.round_quantity("BTCUSDT", Decimal("0.0009")) == Decimal("0")

    # DOGEUSDT step size = 1
    assert registry.round_quantity("DOGEUSDT", Decimal("150.99")) == Decimal("150")


def test_symbol_rules_round_price_half_up() -> None:
    registry = SymbolRulesRegistry()
    # BTCUSDT tick size = 0.10
    assert registry.round_price("BTCUSDT", Decimal("65000.14")) == Decimal("65000.1")
    assert registry.round_price("BTCUSDT", Decimal("65000.16")) == Decimal("65000.2")

    # DOGEUSDT tick size = 0.00001
    assert registry.round_price("DOGEUSDT", Decimal("0.123456")) == Decimal("0.12346")


def test_symbol_rules_validation() -> None:
    registry = SymbolRulesRegistry()
    # 满足要求
    valid, msg = registry.validate_order("BTCUSDT", Decimal("0.001"), Decimal("65000"))
    assert valid is True
    assert msg == ""

    # 低于 min_qty
    valid, msg = registry.validate_order("BTCUSDT", Decimal("0.0005"), Decimal("65000"))
    assert valid is False
    assert "最小下单量" in msg

    # 低于 min_notional
    valid, msg = registry.validate_order("DOGEUSDT", Decimal("1"), Decimal("0.1"))
    assert valid is False
    assert "最小名义价值" in msg


def test_symbol_rules_dynamic_update_from_exchange_info() -> None:
    registry = SymbolRulesRegistry()
    mock_info = {
        "symbols": [
            {
                "symbol": "CUSTOMUSDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.05"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.5"},
                    {"filterType": "MIN_NOTIONAL", "notional": "10.0"},
                ],
            }
        ]
    }
    registry.update_from_exchange_info(mock_info)
    flt = registry.get_filter("CUSTOMUSDT")
    assert flt.tick_size == Decimal("0.05")
    assert flt.step_size == Decimal("0.1")
    assert flt.min_qty == Decimal("0.5")
    assert flt.min_notional == Decimal("10.0")


def test_symbol_rules_defensive_on_malformed_exchange_info() -> None:
    registry = SymbolRulesRegistry()
    # 传入非法格式时不崩溃
    registry.update_from_exchange_info({})
    registry.update_from_exchange_info({"symbols": None})  # type: ignore[dict-item]
    registry.update_from_exchange_info({"symbols": ["not_a_dict"]})  # type: ignore[list-item]
    assert registry.get_filter("UNKNOWNUSDT").step_size == Decimal("0.001")
