import pytest

from crypto_momentum_lab.execution_account.binance.request_rules import (
    entry_leverage_candidates,
    normalize_symbols,
    require_margin_type,
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


@pytest.mark.parametrize(
    "value",
    ["CROSSED", "ISOLATED"],
)
def test_margin_type_accepts_exchange_enums(value):
    assert require_margin_type(value) == value


@pytest.mark.parametrize(
    "value",
    ["CROSS", "cross", " cross ", " crossed ", " isolated ", "", "portfolio"],
)
def test_margin_type_rejects_aliases_and_whitespace(value):
    with pytest.raises(ValueError, match="margin_type must be CROSSED or ISOLATED"):
        require_margin_type(value)


@pytest.mark.parametrize(
    "requested,expected",
    [
        (5, (5, 4, 3)),
        (2, (2, 1)),
        (1, (1,)),
    ],
)
def test_leverage_candidates_keep_the_configured_strategy(requested, expected):
    assert entry_leverage_candidates(requested) == expected
