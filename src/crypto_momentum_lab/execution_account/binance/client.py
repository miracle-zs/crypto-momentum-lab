import hashlib
import hmac
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from urllib.parse import urlencode

import httpx

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.live_rollout import RollbackCommand
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeCancellationUnknownError,
    ExchangeOrderQueryUnknownError,
    ExchangeOrderRejectedError,
    ExchangeSubmissionTimeoutError,
    LiveSubmissionDisabledError,
)
from crypto_momentum_lab.live_rollout.commands import (
    CANCEL_ALL_CONFIRMATION,
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)

# Official Binance USD-M Futures USER_DATA endpoints verified 2026-07-04:
# /fapi/v3/account, /fapi/v3/balance, /fapi/v3/positionRisk,
# /fapi/v1/openOrders, /fapi/v1/userTrades.


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
        clock: Callable[[], datetime] | None = None,
        recv_window_ms: int = 5000,
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
        self._api_key = api_key
        self._api_secret = api_secret
        self._environment = environment
        self._account_label = account_label
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._recv_window_ms = recv_window_ms
        self._client = http_client or httpx.AsyncClient(
            base_url=base_url,
            trust_env=False,
        )

    async def fetch_account_config(self) -> AccountConfigSnapshot:
        payload = await self._signed_get("/fapi/v3/account")
        data = _require_mapping(payload)
        position_mode_payload = await self._signed_get(
            "/fapi/v1/positionSide/dual"
        )
        position_mode = _require_mapping(position_mode_payload)
        hedge_mode = bool(position_mode.get("dualSidePosition", False))
        raw_payload = _json_mapping(data)
        raw_payload["dualSidePosition"] = hedge_mode
        observed_at = self._now()
        return AccountConfigSnapshot(
            environment=self._environment,
            account_label=self._account_label,
            multi_assets_mode=bool(data.get("multiAssetsMargin", False)),
            hedge_mode=hedge_mode,
            can_trade=bool(data.get("canTrade", False)),
            fee_tier=_optional_int(data.get("feeTier")),
            observed_at=observed_at,
            raw_payload=raw_payload,
        )

    async def fetch_balances(self) -> tuple[AccountBalanceSnapshot, ...]:
        payload = await self._signed_get("/fapi/v3/balance")
        observed_at = self._now()
        return tuple(
            AccountBalanceSnapshot(
                environment=self._environment,
                account_label=self._account_label,
                asset=str(item.get("asset", "")),
                wallet_balance=_decimal(item.get("balance", "0")),
                available_balance=_decimal(item.get("availableBalance", "0")),
                unrealized_pnl=_decimal(item.get("crossUnPnl", "0")),
                observed_at=observed_at,
                raw_payload=_json_mapping(item),
            )
            for item in _require_sequence_of_mappings(payload)
        )

    async def fetch_positions(self) -> tuple[AccountPositionSnapshot, ...]:
        payload = await self._signed_get("/fapi/v3/positionRisk")
        observed_at = self._now()
        return tuple(
            AccountPositionSnapshot(
                environment=self._environment,
                account_label=self._account_label,
                symbol=str(item.get("symbol", "")),
                position_side=str(item.get("positionSide", "BOTH")),
                position_amt=_decimal(item.get("positionAmt", "0")),
                entry_price=_decimal(item.get("entryPrice", "0")),
                mark_price=_decimal(item.get("markPrice", "0")),
                unrealized_pnl=_decimal(item.get("unRealizedProfit", "0")),
                notional=_decimal(item.get("notional", "0")),
                leverage=_optional_int(item.get("leverage")),
                margin_type=_optional_str(item.get("marginType")),
                observed_at=observed_at,
                raw_payload=_json_mapping(item),
            )
            for item in _require_sequence_of_mappings(payload)
        )

    async def fetch_open_orders(self) -> tuple[AccountOpenOrderSnapshot, ...]:
        payload = await self._signed_get("/fapi/v1/openOrders")
        observed_at = self._now()
        return tuple(
            AccountOpenOrderSnapshot(
                environment=self._environment,
                account_label=self._account_label,
                symbol=str(item.get("symbol", "")),
                order_id=str(item.get("orderId", "")),
                client_order_id=str(item.get("clientOrderId", "")),
                side=str(item.get("side", "")),
                order_type=str(item.get("type", "")),
                status=str(item.get("status", "")),
                price=_decimal(item.get("price", "0")),
                original_quantity=_decimal(item.get("origQty", "0")),
                executed_quantity=_decimal(item.get("executedQty", "0")),
                reduce_only=bool(item.get("reduceOnly", False)),
                observed_at=observed_at,
                raw_payload=_json_mapping(item),
            )
            for item in _require_sequence_of_mappings(payload)
        )

    async def fetch_recent_fills(
        self,
        symbols: tuple[str, ...] = (),
    ) -> tuple[AccountFillEvent, ...]:
        normalized_symbols = _normalize_symbols(symbols)
        fills: dict[tuple[str, str], AccountFillEvent] = {}
        for symbol in normalized_symbols:
            payload = await self._signed_get(
                "/fapi/v1/userTrades",
                {"symbol": symbol, "limit": 1000},
            )
            for item in _require_sequence_of_mappings(payload):
                trade_id = str(item.get("id", ""))
                fill = AccountFillEvent(
                    environment=self._environment,
                    account_label=self._account_label,
                    symbol=str(item.get("symbol", symbol)),
                    trade_id=trade_id,
                    order_id=str(item.get("orderId", "")),
                    side=str(item.get("side", "")),
                    price=_decimal(item.get("price", "0")),
                    quantity=_decimal(item.get("qty", "0")),
                    realized_pnl=_decimal(item.get("realizedPnl", "0")),
                    fee=_decimal(item.get("commission", "0")),
                    fee_asset=str(item.get("commissionAsset", "")),
                    trade_at=datetime.fromtimestamp(
                        int(str(item.get("time", 0))) / 1000,
                        tz=UTC,
                    ),
                    raw_payload=_json_mapping(item),
                )
                fills[(fill.symbol, fill.trade_id)] = fill
        return tuple(
            sorted(
                fills.values(),
                key=lambda fill: (fill.trade_at, fill.symbol, fill.trade_id),
            )
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _signed_get(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None] | None = None,
    ) -> object:
        signed_params = self._signed_params(params or {})
        response = await self._client.get(
            path,
            params=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        response.raise_for_status()
        return response.json()

    async def _signed_post(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None],
    ) -> object:
        signed_params = self._signed_params(params)
        response = await self._client.post(
            path,
            data=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        response.raise_for_status()
        return response.json()

    async def _signed_delete(
        self,
        path: str,
        params: dict[str, str | int | float | bool | None],
    ) -> object:
        signed_params = self._signed_params(params)
        response = await self._client.delete(
            path,
            params=signed_params,
            headers={"X-MBX-APIKEY": self._api_key},
        )
        response.raise_for_status()
        return response.json()

    def _signed_params(
        self,
        params: dict[str, str | int | float | bool | None],
    ) -> dict[str, str | int | float | bool | None]:
        payload = {
            key: value for key, value in params.items() if value is not None
        }
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
        clock: Callable[[], datetime] | None = None,
        recv_window_ms: int = 5000,
        entry_leverage: int | None = None,
    ) -> None:
        if entry_leverage is not None and not 1 <= entry_leverage <= 125:
            raise ValueError("entry_leverage must be between 1 and 125")
        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            environment=environment,
            account_label=account_label,
            base_url=base_url,
            http_client=http_client,
            clock=clock,
            recv_window_ms=recv_window_ms,
        )
        self._live_submit_enabled = live_submit_enabled
        self._entry_leverage = entry_leverage
        self._configured_leverage_symbols: set[str] = set()

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        if not plan.reduce_only:
            await self._ensure_entry_leverage(plan.symbol)
        params: dict[str, str | int | float | bool | None] = {
            "symbol": plan.symbol,
            "side": plan.side,
            "type": plan.order_type,
            "quantity": format(plan.quantity, "f"),
            "newClientOrderId": plan.client_order_id,
            "newOrderRespType": "RESULT",
        }
        if plan.position_side.value == "BOTH":
            params["reduceOnly"] = str(plan.reduce_only).lower()
        else:
            params["positionSide"] = plan.position_side.value
        if plan.price is not None:
            params["price"] = format(plan.price, "f")
            params["timeInForce"] = "GTC"
        try:
            payload = await self._signed_post("/fapi/v1/order", params)
        except httpx.TimeoutException as exc:
            raise ExchangeSubmissionTimeoutError(
                "Binance order submit timed out"
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                raise ExchangeSubmissionTimeoutError(
                    "Binance order submit returned an unknown server outcome"
                ) from exc
            raise ExchangeOrderRejectedError(_exchange_error_message(exc)) from exc
        return self._order_snapshot(_require_mapping(payload))

    async def _ensure_entry_leverage(self, symbol: str) -> None:
        if (
            self._entry_leverage is None
            or symbol in self._configured_leverage_symbols
        ):
            return
        try:
            payload = await self._signed_post(
                "/fapi/v1/leverage",
                {"symbol": symbol, "leverage": self._entry_leverage},
            )
            response = _require_mapping(payload)
            if str(response.get("symbol", "")) != symbol or int(
                str(response.get("leverage", 0))
            ) != self._entry_leverage:
                raise ValueError("unexpected leverage response")
        except (httpx.HTTPError, ValueError) as exc:
            raise ExchangeOrderRejectedError(
                "Binance entry leverage was not confirmed; order was not sent"
            ) from exc
        self._configured_leverage_symbols.add(symbol)

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
            if exc.response.status_code == 400 and _exchange_error_code(exc) == -2013:
                return None
            raise
        except httpx.TimeoutException as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup timed out; order state requires reconciliation"
            ) from exc
        except httpx.RequestError as exc:
            raise ExchangeOrderQueryUnknownError(
                "Binance order lookup failed; order state requires reconciliation"
            ) from exc
        return self._order_snapshot(_require_mapping(payload))

    async def cancel_order(
        self,
        *,
        symbol: str,
        client_order_id: str,
        command: RollbackCommand | None,
    ) -> ExchangeOrderSnapshot:
        if not self._live_submit_enabled:
            raise LiveSubmissionDisabledError(
                "Binance trade client requires explicit live submit enablement"
            )
        require_authorized_command(
            command,
            command_type="cancel_all_open_orders",
            confirmation_text=CANCEL_ALL_CONFIRMATION,
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
        return self._order_snapshot(_require_mapping(payload))

    async def emergency_flatten(
        self,
        *,
        plan: OrderExecutionPlan,
        command: RollbackCommand | None,
    ) -> ExchangeOrderSnapshot:
        require_authorized_command(
            command,
            command_type="emergency_flatten",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )
        if not plan.reduce_only:
            raise ValueError("emergency flatten plan must be reduce-only")
        return await self.submit_order(plan)

    def _order_snapshot(self, data: dict[str, object]) -> ExchangeOrderSnapshot:
        return ExchangeOrderSnapshot(
            client_order_id=str(data.get("clientOrderId", "")),
            exchange_order_id=str(data.get("orderId", "")),
            state=_exchange_order_state(str(data.get("status", ""))),
            observed_at=self._now(),
            executed_quantity=_decimal(data.get("executedQty", "0")),
            average_price=_decimal(data.get("avgPrice", "0")),
        )


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(str(value))


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _normalize_symbols(symbols: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(sorted({symbol.strip().upper() for symbol in symbols}))
    if any(not symbol for symbol in normalized):
        raise ValueError("fill symbols must not be empty")
    if any("/" in symbol or "\\" in symbol for symbol in normalized):
        raise ValueError("fill symbols must be valid Binance symbols")
    return normalized


def _require_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return cast(dict[str, object], value)


def _require_sequence_of_mappings(value: object) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise ValueError("expected JSON array")
    rows: list[dict[str, object]] = []
    for item in value:
        rows.append(_require_mapping(item))
    return tuple(rows)


def _json_mapping(value: dict[str, object]) -> dict[str, JsonValue]:
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def _exchange_order_state(status: str) -> ExchangeOrderState:
    states = {
        "NEW": ExchangeOrderState.ACKNOWLEDGED,
        "PARTIALLY_FILLED": ExchangeOrderState.PARTIALLY_FILLED,
        "FILLED": ExchangeOrderState.FILLED,
        "CANCELED": ExchangeOrderState.CANCELED,
        "REJECTED": ExchangeOrderState.REJECTED,
        "EXPIRED": ExchangeOrderState.EXPIRED,
        "EXPIRED_IN_MATCH": ExchangeOrderState.EXPIRED,
    }
    try:
        return states[status]
    except KeyError as exc:
        raise ValueError(f"unsupported Binance order status: {status}") from exc


def _exchange_error_code(exc: httpx.HTTPStatusError) -> int | None:
    try:
        payload = exc.response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    code = payload.get("code")
    return int(code) if code is not None else None


def _exchange_error_message(exc: httpx.HTTPStatusError) -> str:
    try:
        payload = exc.response.json()
    except ValueError:
        return f"Binance rejected order with HTTP {exc.response.status_code}"
    if isinstance(payload, dict) and payload.get("msg"):
        return str(payload["msg"])
    return f"Binance rejected order with HTTP {exc.response.status_code}"
