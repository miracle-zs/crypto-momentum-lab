import pytest

from crypto_momentum_lab.execution_account.binance.request_rules import (
    normalize_fill_cursors,
    normalize_symbols,
)


def test_symbols_are_normalized_sorted_deduplicated_without_mutating_input():
    symbols = [" ethusdt ", "BTCUSDT", "btcUsdt"]
    assert normalize_symbols(iter(symbols)) == ("BTCUSDT", "ETHUSDT")
    assert symbols == [" ethusdt ", "BTCUSDT", "btcUsdt"]
    assert normalize_symbols(()) == ()


@pytest.mark.parametrize(
    "symbols,error",
    [
        ([" "], "fill symbols must not be empty"),
        (["BTC/USDT"], "fill symbols must be valid Binance symbols"),
        (["BTC\\USDT"], "fill symbols must be valid Binance symbols"),
        (["BTC/USDT", " "], "fill symbols must not be empty"),
    ],
)
def test_invalid_symbols_preserve_error_priority(symbols, error):
    with pytest.raises(ValueError, match=error):
        normalize_symbols(symbols)


def test_cursor_keys_normalize_and_later_collision_overwrites_without_mutation():
    cursors = {" btcusdt ": 0, "BTCUSDT": 42, "ethusdt": 1700000000000}
    assert normalize_fill_cursors(cursors) == {"BTCUSDT": 42, "ETHUSDT": 1700000000000}
    assert cursors == {" btcusdt ": 0, "BTCUSDT": 42, "ethusdt": 1700000000000}
    assert normalize_fill_cursors(None) == normalize_fill_cursors({}) == {}


@pytest.mark.parametrize(
    "cursors,error",
    [
        ({" ": -1}, "fill cursor symbols must be valid Binance symbols"),
        ({"BTC/USDT": 0}, "fill cursor symbols must be valid Binance symbols"),
        ({"BTC\\USDT": 0}, "fill cursor symbols must be valid Binance symbols"),
        ({"BTCUSDT": True}, "fill cursors must be integer values"),
        ({"BTCUSDT": "1"}, "fill cursors must be integer values"),
        ({"BTCUSDT": 1.5}, "fill cursors must be integer values"),
        ({"BTCUSDT": -1}, "fill cursors must be non-negative"),
    ],
)
def test_invalid_cursor_symbol_type_and_sign_keep_existing_errors(cursors, error):
    with pytest.raises(ValueError, match=error):
        normalize_fill_cursors(cursors)
