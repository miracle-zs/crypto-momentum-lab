import pytest

from crypto_momentum_lab.execution_account.binance.request_rules import (
    entry_leverage_candidates,
    normalize_fill_cursors,
    normalize_margin_type,
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


@pytest.mark.parametrize(
    "value,expected",
    [
        ("CROSS", "CROSSED"),
        ("CROSSED", "CROSSED"),
        ("ISOLATED", "ISOLATED"),
        (" cross ", "CROSSED"),
        (" crossed ", "CROSSED"),
        (" isolated ", "ISOLATED"),
    ],
)
def test_margin_type_aliases_and_whitespace(value, expected):
    assert normalize_margin_type(value) == expected


@pytest.mark.parametrize("value", ["", " ", "portfolio", "cross-margin"])
def test_margin_type_errors_keep_allowed_values_and_cause(value):
    with pytest.raises(
        ValueError, match="margin_type must be one of: CROSSED, ISOLATED"
    ) as caught:
        normalize_margin_type(value)
    assert isinstance(caught.value.__cause__, KeyError)


@pytest.mark.parametrize(
    "requested,steps,expected",
    [
        (5, 2, (5, 4, 3)),
        (2, 2, (2, 1)),
        (1, 5, (1,)),
        (5, 0, (5,)),
        (5, -1, ()),
        (0, 2, (1,)),
        (-2, 2, (1,)),
    ],
)
def test_leverage_candidates_keep_order_floor_deduplication_and_step_semantics(
    requested, steps, expected
):
    assert entry_leverage_candidates(requested, max_steps=steps) == expected


def test_leverage_candidates_default_to_two_fallback_steps():
    assert entry_leverage_candidates(5) == (5, 4, 3)
