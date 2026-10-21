import asyncio
import hashlib
import hmac
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import DecimalException
from urllib.parse import urlencode

import httpx
import structlog

from crypto_momentum_lab.domain.account.models import (
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeOrderQueryUnknownError,
    ExchangeOrderRejectedError,
    ExchangeSubmissionTimeoutError,
    LiveSubmissionDisabledError,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.execution_account.binance.response_rules import (
    exchange_error_code,
    exchange_error_message,
    retry_after_seconds,
)
from crypto_momentum_lab.execution_account.binance.rest_parser import (
    account_open_order_from_item,
    order_snapshot_from_response,
    positions_from_response,
    rest_require_mapping,
    rest_require_sequence_of_mappings,
)

log = structlog.get_logger(__name__)

# Binance USD-M Futures private endpoints used by this operations client:
# /fapi/v3/positionRisk and /fapi/v1/openOrders.

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 5.0
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
_DEFAULT_POOL_TIMEOUT_SECONDS = 5.0
_MISSING = object()


def _json_value_type(value: object) -> str:
    """Return the JSON token type without coercing the received value."""

    if value is _MISSING:
        return "missing"
    if value is None:
        return "null"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


class BinanceRateLimitError(httpx.HTTPStatusError):
    """A Binance REST rate-limit response that callers should back off from."""

    def __init__(self, response: httpx.Response) -> None:
        super().__init__(
            f"Binance REST request rate limited with HTTP {response.status_code}",
            request=response.request,
            response=response,
        )
        self.retry_after_seconds = retry_after_seconds(response)


class _AsyncRequestPacer:
    """Keep request starts at least the configured interval apart."""

    def __init__(self, min_interval_seconds: float) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must not be negative")
        self._min_interval_seconds = min_interval_seconds
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def wait(self) -> None:
        if self._min_interval_seconds == 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            delay = max(0.0, self._next_allowed_at - loop.time())
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_allowed_at = loop.time() + self._min_interval_seconds


class BinanceUsdMPrivateReadClient:
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        environment: str,
        account_label: str,
        base_url: str = "https://fapi.binance.com",
        http_client: httpx.AsyncClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        recv_window_ms: int = 10000,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
        connect_timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        pool_timeout_seconds: float = _DEFAULT_POOL_TIMEOUT_SECONDS,
    ) -> None:
        if not api_key.strip():
            raise ValueError("api_key must not be empty")
        if not api_secret.strip():
            raise ValueError("api_secret must not be empty")
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if recv_window_ms <= 0:
            raise ValueError("recv_window_ms must be positive")
        if request_interval_seconds < 0:
            raise ValueError("request_interval_seconds must not be negative")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if pool_timeout_seconds <= 0:
            raise ValueError("pool_timeout_seconds must be positive")
        self._api_key = api_key
        self._api_secret = api_secret
        self._environment = environment
        self._account_label = account_label
        self._clock = clock
        self._recv_window_ms = recv_window_ms
        self._request_pacer = _AsyncRequestPacer(request_interval_seconds)
        self._client = http_client or httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(
                request_timeout_seconds,
                connect=connect_timeout_seconds,
                pool=pool_timeout_seconds,
            ),
            trust_env=False,
        )

    async def fetch_positions(
        self, *, include_flat: bool = False
    ) -> tuple[AccountPositionSnapshot, ...]:
        """Read actual position rows; V2 retains explicit flat rows for bootstrap.

        V3 omits symbols without positions/open orders. Absence in that response
        must never be converted into an invented zero-position observation.
        """
        path = "/fapi/v2/positionRisk" if include_flat else "/fapi/v3/positionRisk"
        payload = await self._signed_get(path)
        observed_at = self._now()
        return positions_from_response(
            payload,
            environment=self._environment,
            account_label=self._account_label,
            observed_at=observed_at,
        )

    async def fetch_open_orders(
        self,
        symbol: str | None = None,
    ) -> tuple[AccountOpenOrderSnapshot, ...]:
        params: dict[str, str | int | float | bool | None] | None = None
        expected_symbol = None
        if symbol is not None:
            if not symbol.strip():
                raise ValueError("symbol must not be empty")
            expected_symbol = symbol.strip().upper()
            params = {"symbol": expected_symbol}
        payload = await self._signed_get("/fapi/v1/openOrders", params)
        observed_at = self._now()
        return tuple(
            account_open_order_from_item(
                item,
                environment=self._environment,
                account_label=self._account_label,
                observed_at=observed_at,
                expected_symbol=expected_symbol,
            )
            for item in rest_require_sequence_of_mappings(payload)
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _signed_get(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None] | None = None,
    ) -> object:
        await self._request_pacer.wait()
        signed_params = self._signed_params(params or {})
        response = await self._client.get(
            path,
            params=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        _raise_for_status(response)
        return response.json()

    async def _signed_post(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None],
    ) -> object:
        await self._request_pacer.wait()
        signed_params = self._signed_params(params)
        response = await self._client.post(
            path,
            data=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        _raise_for_status(response)
        return response.json()

    async def _signed_delete(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None],
    ) -> object:
        await self._request_pacer.wait()
        signed_params = self._signed_params(params)
        response = await self._client.delete(
            path,
            params=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        _raise_for_status(response)
        return response.json()


    def _signed_params(
        self,
        params: dict[str, str | int | float | bool | None],
    ) -> dict[str, str | int | float | bool | None]:
        payload = {key: value for key, value in params.items() if value is not None}
        payload.update(
            {
                "timestamp": int(self._now().timestamp() * 1000),
                "recvWindow": self._recv_window_ms,
            }
        )
        query = urlencode(payload)
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {**payload, "signature": signature}

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return now


class BinanceUsdMTradeClient(BinanceUsdMPrivateReadClient):
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        environment: str,
        account_label: str,
        live_submit_enabled: bool,
        base_url: str = "https://fapi.binance.com",
        http_client: httpx.AsyncClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        recv_window_ms: int = 10000,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
        connect_timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        pool_timeout_seconds: float = _DEFAULT_POOL_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            environment=environment,
            account_label=account_label,
            base_url=base_url,
            http_client=http_client,
            clock=clock,
            recv_window_ms=recv_window_ms,
            request_interval_seconds=request_interval_seconds,
            request_timeout_seconds=request_timeout_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
            pool_timeout_seconds=pool_timeout_seconds,
        )
        self._live_submit_enabled = live_submit_enabled

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        if not plan.reduce_only:
            raise OrderPreSubmissionError(
                "legacy Binance trade client only supports reduce-only orders"
            )
        params: dict[str, str | int | float | bool | None] = {
            "symbol": plan.symbol,
            "side": plan.side,
            "type": "MARKET",
            "quantity": format(plan.quantity, "f"),
            "newClientOrderId": plan.client_order_id,
            "newOrderRespType": "RESULT",
        }
        if plan.position_side.value == "BOTH":
            params["reduceOnly"] = str(plan.reduce_only).lower()
        else:
            # Hedge mode: reduceOnly is absent, so the exchange classifies
            # purely from side + positionSide.  A reduce_only plan whose
            # side agrees with its position side would increase exposure.
            if plan.reduce_only:
                _closing_side = {
                    FuturesPositionSide.LONG: "SELL",
                    FuturesPositionSide.SHORT: "BUY",
                }
                if plan.side != _closing_side.get(plan.position_side):
                    raise OrderPreSubmissionError(
                        f"Hedge reduce_only plan {plan.client_order_id} "
                        f"side={plan.side} position_side={plan.position_side.value} "
                        "would increase exposure instead of closing"
                    )
            params["positionSide"] = plan.position_side.value
        try:
            payload = await self._signed_post(
                "/fapi/v1/order",
                params,
            )
        except ValueError as exc:
            # A decode failure escaping the order POST cannot prove rejection.
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit returned an unreadable response; outcome unknown"
            ) from exc
        except httpx.TimeoutException as exc:
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit timed out"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit failed with an unknown outcome"
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                raise ExchangeSubmissionTimeoutError(
                    "Binance order submit returned an unknown server outcome"
                ) from exc
            raise ExchangeOrderRejectedError(exchange_error_message(exc)) from exc
        try:
            snapshot = order_snapshot_from_response(
                rest_require_mapping(payload),
                observed_at=self._now(),
                fill_price_optional=True,
            )
            return snapshot
        except (ValueError, TypeError, KeyError) as parse_exc:
            log.warning(
                "binance_order_submit_response_parse_error",
                client_order_id=plan.client_order_id,
                error=str(parse_exc),
                avg_price_json_type=_json_value_type(
                    payload.get("avgPrice", _MISSING)
                    if isinstance(payload, dict)
                    else _MISSING
                ),
                cumulative_quote_json_type=_json_value_type(
                    payload.get("cumQuote", _MISSING)
                    if isinstance(payload, dict)
                    else _MISSING
                ),
            )
            raise ExchangeSubmissionTimeoutError(
                f"Binance order submitted but response parsing failed: {parse_exc}"
            ) from parse_exc

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        try:
            payload = await self._signed_get(
                "/fapi/v1/order",
                {
                    "symbol": symbol,
                    "origClientOrderId": client_order_id,
                },
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400 and exchange_error_code(exc) == -2013:
                return None
            if exc.response.status_code in {418, 429} or (
                exc.response.status_code >= 500
            ):
                raise ExchangeOrderQueryUnknownError(
                    "Binance order lookup returned a transient HTTP error; "
                    "order state requires reconciliation",
                    retry_after_seconds=retry_after_seconds(exc.response),
                ) from exc
            raise
        except httpx.TimeoutException as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup timed out; order state requires reconciliation"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup failed; order state requires reconciliation"
            ) from exc
        try:
            return order_snapshot_from_response(
                rest_require_mapping(payload), observed_at=self._now()
            )
        except (DecimalException, KeyError, TypeError, ValueError) as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup response was unreadable; "
                "order state must be reconciled"
            ) from exc

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        """Cancel one known order for normal executor lifecycle management."""
        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        try:
            payload = await self._signed_delete(
                "/fapi/v1/order",
                {
                    "symbol": symbol,
                    "origClientOrderId": client_order_id,
                },
            )
        except httpx.TimeoutException as exc:
            raise ExchangeCancellationUnknownError(
                "Binance cancel request timed out; order state must be reconciled"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeCancellationUnknownError(
                "Binance cancel request failed; order state must be reconciled"
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {418, 429}:
                raise ExchangeCancellationUnknownError(
                    "Binance cancel request was rate limited; "
                    "order state must be reconciled",
                    retry_after_seconds=retry_after_seconds(exc.response),
                ) from exc
            if exc.response.status_code >= 500:
                raise ExchangeCancellationUnknownError(
                    "Binance cancel request returned an unknown server outcome"
                ) from exc
            if exchange_error_code(exc) in {-2011, -2013}:
                exchange_message = exchange_error_message(exc)
                try:
                    existing = await self.query_order_by_client_id(
                        symbol,
                        client_order_id,
                    )
                except ExchangeOrderQueryUnknownError as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance cancel result could not be reconciled"
                    ) from query_error
                if existing is not None:
                    return existing
                try:
                    open_orders = await self.fetch_open_orders(symbol=symbol)
                except httpx.TimeoutException as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance open-order check timed out; "
                        "order state must be reconciled"
                    ) from query_error
                except httpx.RequestError as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance open-order check failed; "
                        "order state must be reconciled"
                    ) from query_error
                except httpx.HTTPStatusError as query_error:
                    raise ExchangeCancellationUnknownError(
                        "Binance open-order check returned an error; "
                        "order state must be reconciled",
                        retry_after_seconds=retry_after_seconds(query_error.response),
                    ) from query_error
                if any(
                    order.symbol == symbol and order.client_order_id == client_order_id
                    for order in open_orders
                ):
                    raise ExchangeCancellationUnknownError(
                        "Binance cancel result is inconsistent; "
                        "matching open order still exists"
                    ) from exc
                raise ExchangeOrderAlreadyAbsentError(
                    exchange_message,
                    exchange_code=exchange_error_code(exc),
                    exchange_message=exchange_message,
                    http_status=exc.response.status_code,
                    open_orders_checked=True,
                ) from exc
            raise ExchangeCancellationUnknownError(
                "Binance cancel request was rejected; order state must be reconciled"
            ) from exc
        try:
            return order_snapshot_from_response(
                rest_require_mapping(payload), observed_at=self._now()
            )
        except (DecimalException, KeyError, TypeError, ValueError) as exc:
            raise ExchangeCancellationUnknownError(
                "Binance cancel response was unreadable; "
                "order state must be reconciled"
            ) from exc

def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code in {418, 429}:
        raise BinanceRateLimitError(response)
    response.raise_for_status()
