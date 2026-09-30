from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pytest

from crypto_momentum_lab.execution_account.binance.rest_parser import (
    account_fill_from_trade_item,
    decimal_value,
    json_mapping,
)


def trade_row():
    return {
        "symbol": " btcusdt ",
        "id": 1,
        "orderId": 10,
        "side": "BUY",
        "price": "50000.00",
        "qty": "0.010",
        "realizedPnl": "-0.2",
        "commission": "0.1",
        "commissionAsset": "USDT",
        "time": 1790812800123,
    }


def parse(row):
    return account_fill_from_trade_item(
        row, environment="live", account_label="primary", fallback_symbol="BTCUSDT"
    )


def test_bounded_trade_parser_preserves_fields_precision_and_raw_payload():
    row = trade_row()
    result = parse(row)
    assert result.symbol == "BTCUSDT"
    assert result.trade_id == "1" and result.order_id == "10"
    assert result.side == "BUY" and result.fee_asset == "USDT"
    assert str(result.price) == "50000.00" and str(result.quantity) == "0.010"
    assert result.realized_pnl == Decimal("-0.2") and result.fee == Decimal("0.1")
    assert result.trade_at == datetime(2026, 10, 1, 0, 0, 0, 123000, tzinfo=UTC)
    assert result.environment == "live" and result.account_label == "primary"
    assert result.raw_payload == row
    assert result.raw_payload is not row
    assert row == trade_row()


def test_missing_optional_trade_values_keep_zero_epoch_and_fallback_symbol():
    result = parse(
        {"id": "1", "orderId": "10", "side": "BUY", "commissionAsset": "USDT"}
    )
    assert result.symbol == "BTCUSDT"
    assert result.price == result.quantity == result.fee == result.realized_pnl == 0
    assert result.trade_at == datetime(1970, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize("symbol", ["ETHUSDT", "", None])
def test_wrong_symbol_is_rejected_before_malformed_numeric_fields(symbol):
    with pytest.raises(ValueError, match="contained another symbol"):
        parse({"symbol": symbol, "price": "broken"})


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("price", "broken", InvalidOperation),
        ("qty", "broken", InvalidOperation),
        ("time", "broken", ValueError),
        ("commission", "-1", ValueError),
        ("id", "", ValueError),
        ("commissionAsset", "", ValueError),
    ],
)
def test_trade_conversion_and_domain_validation_keep_errors(field, value, error):
    row = trade_row()
    row[field] = value
    with pytest.raises(error):
        parse(row)


@pytest.mark.parametrize(
    "value,expected",
    [("0.010", "0.010"), (0, "0"), (1.25, "1.25"), (Decimal("1.00"), "1.00")],
)
def test_decimal_conversion_preserves_existing_text_semantics(value, expected):
    assert str(decimal_value(value)) == expected


def test_rest_json_conversion_keeps_nested_values_and_stringifies_unsupported_types():
    raw = {
        "nested": [{"v": Decimal("1.00")}, None, True],
        "tuple": (1, 2),
        "text": "value",
    }
    assert json_mapping(raw) == {
        "nested": [{"v": "1.00"}, None, True],
        "tuple": "(1, 2)",
        "text": "value",
    }
    assert isinstance(raw["nested"][0]["v"], Decimal)
    assert raw["tuple"] == (1, 2)
