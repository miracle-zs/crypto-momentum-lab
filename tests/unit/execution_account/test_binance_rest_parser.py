from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pytest

from crypto_momentum_lab.execution_account.binance.rest_parser import (
    account_fill_from_trade_item,
    balances_from_response,
    decimal_value,
    json_mapping,
    order_snapshot_from_response,
    positions_from_response,
    rest_optional_int,
    rest_optional_str,
    rest_require_mapping,
    rest_require_sequence_of_mappings,
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


def test_rest_object_and_array_checks_preserve_row_identity():
    row = {"id": 1}
    assert rest_require_mapping(row) is row
    rows = rest_require_sequence_of_mappings([row, {}])
    assert isinstance(rows, tuple) and rows[0] is row and rows[1] == {}
    assert rest_require_sequence_of_mappings([]) == ()


@pytest.mark.parametrize("value", [None, [], "{}", 1])
def test_rest_mapping_rejects_non_dict(value):
    with pytest.raises(ValueError, match="expected JSON object"):
        rest_require_mapping(value)


@pytest.mark.parametrize("value", [None, {}, (), "[]"])
def test_rest_rows_reject_non_list(value):
    with pytest.raises(ValueError, match="expected JSON array"):
        rest_require_sequence_of_mappings(value)


@pytest.mark.parametrize("value", [[{}, None], [{}, []]])
def test_rest_rows_validate_every_element(value):
    with pytest.raises(ValueError, match="expected JSON object"):
        rest_require_sequence_of_mappings(value)


@pytest.mark.parametrize(
    "value,expected", [(None, None), ("", None), (" ", " "), (0, "0"), (False, "False")]
)
def test_rest_optional_text_keeps_empty_and_whitespace_distinction(value, expected):
    assert rest_optional_str(value) == expected


@pytest.mark.parametrize(
    "value,expected", [(None, None), (0, 0), (" 2 ", 2), ("-1", -1)]
)
def test_rest_optional_integer_keeps_zero_negative_and_whitespace(value, expected):
    assert rest_optional_int(value) == expected


@pytest.mark.parametrize("value", [True, "1.5"])
def test_rest_optional_integer_preserves_conversion_errors(value):
    with pytest.raises(ValueError):
        rest_optional_int(value)


@pytest.mark.parametrize(
    "quantity,average,quote,expected",
    [
        ("2", "0", "10", "5"),
        ("2", "-1", "10", "5"),
        ("2", "7", "10", "7"),
        ("0", "0", "10", "0"),
        ("2", "0", "0", "0"),
        ("2", "0", "-1", "0"),
    ],
)
def test_order_response_average_price_fallback(quantity, average, quote, expected):
    observed_at = datetime(2026, 10, 1, tzinfo=UTC)
    data = {
        "clientOrderId": "entry",
        "orderId": 42,
        "status": "NEW",
        "executedQty": quantity,
        "avgPrice": average,
        "cumQuote": quote,
    }
    result = order_snapshot_from_response(
        data, observed_at=observed_at, entry_leverage=5
    )
    assert result.average_price == Decimal(expected)
    assert result.executed_quantity == Decimal(quantity)
    assert result.client_order_id == "entry" and result.exchange_order_id == "42"
    assert result.observed_at is observed_at and result.entry_leverage == 5
    assert data["avgPrice"] == average


def test_order_response_optional_numeric_defaults():
    result = order_snapshot_from_response(
        {"clientOrderId": "entry", "orderId": 42, "status": "NEW"},
        observed_at=datetime(2026, 10, 1, tzinfo=UTC),
    )
    assert result.average_price == result.executed_quantity == 0
    assert result.entry_leverage is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "UNKNOWN"),
        ("clientOrderId", ""),
        ("executedQty", "-1"),
        ("avgPrice", "-1"),
    ],
)
def test_order_response_keeps_status_and_domain_validation(field, value):
    data = {"clientOrderId": "entry", "orderId": 42, "status": "NEW"}
    data[field] = value
    with pytest.raises(ValueError):
        order_snapshot_from_response(
            data, observed_at=datetime(2026, 10, 1, tzinfo=UTC)
        )


def account_scope():
    return {
        "environment": "live",
        "account_label": "primary",
        "observed_at": datetime(2026, 10, 1, tzinfo=UTC),
    }


def test_balance_response_keeps_order_values_and_raw_payload():
    rows = [
        {
            "asset": "USDT",
            "balance": "100.00",
            "availableBalance": "80",
            "crossUnPnl": "-2",
        },
        {"asset": "BTC"},
    ]
    result = balances_from_response(rows, **account_scope())
    assert [item.asset for item in result] == ["USDT", "BTC"]
    assert str(result[0].wallet_balance) == "100.00"
    assert result[0].available_balance == 80 and result[0].unrealized_pnl == -2
    assert (
        result[1].wallet_balance
        == result[1].available_balance
        == result[1].unrealized_pnl
        == 0
    )
    assert result[0].raw_payload == rows[0] and result[0].raw_payload is not rows[0]
    assert result[0].observed_at == account_scope()["observed_at"]
    assert result[0].environment == "live" and result[0].account_label == "primary"


def test_position_response_keeps_flat_rows_order_and_optional_fields():
    rows = [
        {
            "symbol": "ETHUSDT",
            "positionSide": "SHORT",
            "positionAmt": "-2",
            "entryPrice": "10.00",
            "markPrice": "11",
            "unRealizedProfit": "-2",
            "notional": "-22",
            "leverage": "5",
            "marginType": "cross",
        },
        {"symbol": "BTCUSDT"},
    ]
    result = positions_from_response(rows, **account_scope())
    assert [item.symbol for item in result] == ["ETHUSDT", "BTCUSDT"]
    assert result[0].position_side == "SHORT" and result[0].position_amt == -2
    assert str(result[0].entry_price) == "10.00"
    assert (
        result[0].mark_price == 11
        and result[0].unrealized_pnl == -2
        and result[0].notional == -22
    )
    assert result[0].leverage == 5 and result[0].margin_type == "cross"
    assert result[0].raw_payload == rows[0]
    assert result[1].position_side == "BOTH" and result[1].position_amt == 0
    assert result[1].leverage is None and result[1].margin_type is None
    assert result[1].observed_at == account_scope()["observed_at"]


@pytest.mark.parametrize("parse", [balances_from_response, positions_from_response])
def test_account_response_empty_rows_stay_empty(parse):
    assert parse([], **account_scope()) == ()


@pytest.mark.parametrize("parse", [balances_from_response, positions_from_response])
def test_account_response_shape_validation_is_shared(parse):
    with pytest.raises(ValueError, match="expected JSON array"):
        parse({}, **account_scope())


@pytest.mark.parametrize(
    "parse,row",
    [
        (balances_from_response, {"asset": "USDT", "balance": "-1"}),
        (positions_from_response, {"symbol": "BTCUSDT", "leverage": "-1"}),
    ],
)
def test_account_response_keeps_domain_validation(parse, row):
    with pytest.raises(ValueError):
        parse([row], **account_scope())
