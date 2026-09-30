import httpx
import pytest

from crypto_momentum_lab.execution_account.binance.response_rules import (
    exchange_error_code,
    exchange_error_message,
    is_invalid_leverage_rejection,
    retry_after_seconds,
)


def error(payload, *, malformed=False, status=400):
    request = httpx.Request("GET", "https://example.invalid/order")
    response = httpx.Response(
        status,
        request=request,
        **({"content": b"broken"} if malformed else {"json": payload}),
    )
    return httpx.HTTPStatusError("rejected", request=request, response=response)


@pytest.mark.parametrize(
    "header,expected",
    [
        (None, None),
        ("0", 0.0),
        ("1.5", 1.5),
        (" 2 ", 2.0),
        ("-1", None),
        ("nan", None),
        ("inf", None),
        ("-inf", None),
        ("broken", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", None),
    ],
)
def test_retry_after_preserves_seconds_only_finite_nonnegative_policy(header, expected):
    response = httpx.Response(
        429, headers={} if header is None else {"Retry-After": header}
    )
    assert retry_after_seconds(response) == expected


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({}, None),
        ({"code": None}, None),
        ([], None),
        ({"code": -4028}, -4028),
        ({"code": "-4046"}, -4046),
        ({"code": 0}, 0),
    ],
)
def test_exchange_error_code_preserves_optional_and_integer_conversion(
    payload, expected
):
    assert exchange_error_code(error(payload)) == expected


def test_invalid_json_keeps_code_absent_and_status_fallback():
    rejection = error(None, malformed=True, status=503)
    assert exchange_error_code(rejection) is None
    assert exchange_error_message(rejection) == "Binance rejected order with HTTP 503"


@pytest.mark.parametrize("code", ["broken", []])
def test_malformed_present_error_code_keeps_conversion_exception(code):
    with pytest.raises((ValueError, TypeError)):
        exchange_error_code(error({"code": code}))


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"msg": "reason"}, "reason"),
        ({"msg": 42}, "42"),
        ({"msg": ""}, "Binance rejected order with HTTP 400"),
        ({}, "Binance rejected order with HTTP 400"),
        ([], "Binance rejected order with HTTP 400"),
    ],
)
def test_error_message_preserves_truthy_text_and_status_fallback(payload, expected):
    assert exchange_error_message(error(payload)) == expected


@pytest.mark.parametrize(
    "code,expected", [(-4028, True), ("-4028", True), (-4046, False), (None, False)]
)
def test_only_invalid_leverage_code_allows_leverage_fallback(code, expected):
    assert is_invalid_leverage_rejection(error({"code": code})) is expected
