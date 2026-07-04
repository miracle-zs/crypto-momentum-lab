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
from crypto_momentum_lab.domain.market.models import JsonValue

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
        self._client = http_client or httpx.AsyncClient(base_url=base_url)

    async def fetch_account_config(self) -> AccountConfigSnapshot:
        payload = await self._signed_get("/fapi/v3/account")
        data = _require_mapping(payload)
        observed_at = self._now()
        return AccountConfigSnapshot(
            environment=self._environment,
            account_label=self._account_label,
            multi_assets_mode=bool(data.get("multiAssetsMargin", False)),
            can_trade=bool(data.get("canTrade", False)),
            fee_tier=_optional_int(data.get("feeTier")),
            observed_at=observed_at,
            raw_payload=_json_mapping(data),
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

    async def fetch_recent_fills(self) -> tuple[AccountFillEvent, ...]:
        return ()

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

    def _signed_params(
        self,
        params: dict[str, str | int | float | bool | None],
    ) -> dict[str, str | int | float | bool | None]:
        payload = {
            **params,
            "timestamp": int(self._now().timestamp() * 1000),
            "recvWindow": self._recv_window_ms,
        }
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
