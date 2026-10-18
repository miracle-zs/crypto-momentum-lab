from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

from crypto_momentum_lab.next_gen.engine import AccountSnapshot, PositionSnapshot
from crypto_momentum_lab.next_gen.models import OrderStatus

log = structlog.get_logger("next_gen_client")


class BinanceOrderRejectedError(Exception):
    """币安订单网关业务拒单异常 (如 -2010 余额不足, -2019 保证金不足)"""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"Binance rejected order [{code}]: {message}")
        self.code = code
        self.message = message


class BinanceRestClient:
    """下一代极简币安 USDⓈ-M 交易客户端"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self._client = http_client or httpx.AsyncClient(timeout=10.0)

    def sign_params(
        self, params: dict[str, Any], timestamp: int | None = None
    ) -> dict[str, Any]:
        """为请求参数附加毫秒时间戳与 HMAC-SHA256 签名"""
        signed_params = dict(params)
        ts = timestamp if timestamp is not None else int(time.time() * 1000)
        signed_params["timestamp"] = ts
        query_string = urlencode(signed_params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_params["signature"] = signature
        return signed_params

    async def _request(
        self,
        method: str,
        path: str,
        signed: bool = False,
        params: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        req_params = dict(params or {})
        headers = {"X-MBX-APIKEY": self.api_key}

        if signed:
            req_params = self.sign_params(req_params)

        response = await self._client.request(
            method=method,
            url=url,
            params=req_params if method.upper() in ("GET", "DELETE") else None,
            data=req_params if method.upper() in ("POST", "PUT") else None,
            headers=headers,
        )

        if response.is_error:
            try:
                err_data = response.json()
                code = err_data.get("code", response.status_code)
                msg = err_data.get("msg", response.text)
                raise BinanceOrderRejectedError(code=code, message=msg)
            except (ValueError, KeyError):
                response.raise_for_status()

        return response.json()

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[dict[str, Any]]:
        """GET /fapi/v1/openOrders - 拉取未结挂单"""
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        res = await self._request(
            "GET", "/fapi/v1/openOrders", signed=True, params=params
        )
        return list(res)

    async def fetch_account_information(self) -> AccountSnapshot:
        """GET /fapi/v3/account - 并发冷启动基线快照"""
        res = await self._request("GET", "/fapi/v3/account", signed=True)

        wallet_balances: dict[str, Decimal] = {}
        for asset_data in res.get("assets", []):
            asset = asset_data["asset"]
            wb = Decimal(str(asset_data.get("walletBalance", "0")))
            wallet_balances[asset] = wb

        avail = Decimal(str(res.get("availableBalance", "0")))

        positions: list[PositionSnapshot] = []
        for pos_data in res.get("positions", []):
            pos_amt = Decimal(str(pos_data.get("positionAmt", "0")))
            positions.append(
                PositionSnapshot(
                    symbol=pos_data["symbol"],
                    position_side=pos_data["positionSide"],
                    position_amt=pos_amt,
                )
            )

        return AccountSnapshot(
            wallet_balances=wallet_balances,
            available_balance=avail,
            positions=positions,
        )

    async def create_listen_key(self) -> str:
        """POST /fapi/v1/listenKey - 申请私有流 listenKey"""
        res = await self._request("POST", "/fapi/v1/listenKey", signed=False)
        return str(res["listenKey"])

    async def keepalive_listen_key(self, listen_key: str) -> None:
        """PUT /fapi/v1/listenKey - 续期 listenKey"""
        await self._request(
            "PUT",
            "/fapi/v1/listenKey",
            signed=False,
            params={"listenKey": listen_key},
        )

    async def post_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        order_type: str,
        quantity: Decimal,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        time_in_force: str = "GTC",
    ) -> dict[str, Any]:
        """POST /fapi/v1/order - 出向发单 (显式双向持仓，绝不传 reduceOnly)"""
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "positionSide": position_side.upper(),
            "type": order_type.upper(),
            "quantity": str(quantity),
        }
        if order_type.upper() == "LIMIT":
            if price is None:
                raise ValueError("LIMIT 订单必须提供 price 参数")
            params["price"] = str(price)
            params["timeInForce"] = time_in_force

        if client_order_id:
            params["newClientOrderId"] = client_order_id

        # 严格遵守设计原则：双向持仓模式下禁止传 reduceOnly
        params.pop("reduceOnly", None)

        res = await self._request("POST", "/fapi/v1/order", signed=True, params=params)
        return dict(res)

    async def cancel_order(
        self, symbol: str, client_order_id: str
    ) -> dict[str, Any]:
        """DELETE /fapi/v1/order - 撤单"""
        params = {"symbol": symbol, "origClientOrderId": client_order_id}
        res = await self._request(
            "DELETE", "/fapi/v1/order", signed=True, params=params
        )
        return dict(res)

    async def query_order(
        self, symbol: str, client_order_id: str
    ) -> dict[str, Any]:
        """GET /fapi/v1/order - 只读探针查单 (绝不回写内存)"""
        params = {"symbol": symbol, "origClientOrderId": client_order_id}
        res = await self._request("GET", "/fapi/v1/order", signed=True, params=params)
        return dict(res)


class BinanceUserDataStream:
    """币安私有流监听组件 (事件分发、30m 续期与 24h 断连守卫)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        ws_base_url: str = "wss://fstream.binance.com/private/ws",
        on_status_change: Callable[[bool], None] | None = None,
    ) -> None:
        self._rest = rest_client
        self._ws_base_url = ws_base_url.rstrip("/")
        self.on_status_change = on_status_change
        self.listen_key: str | None = None
        self.is_connected: bool = False

    async def dispatch_raw_event(
        self,
        event: dict[str, Any],
        on_order_trade_update: Callable[..., Any],
        on_account_update: Callable[..., Any],
    ) -> None:
        """事件解析与单向扇入"""
        event_type = event.get("e")

        if event_type == "ORDER_TRADE_UPDATE":
            o = event.get("o", {})
            status_str = o.get("X", "NEW")
            status_mapping = {
                "NEW": OrderStatus.OPEN,
                "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
                "FILLED": OrderStatus.FILLED,
                "CANCELED": OrderStatus.CANCELED,
                "EXPIRED": OrderStatus.CANCELED,
                "REJECTED": OrderStatus.REJECTED,
            }
            mapped_status = status_mapping.get(status_str, OrderStatus.OPEN)

            await on_order_trade_update(
                client_order_id=o.get("c", ""),
                symbol=o.get("s", ""),
                position_side=o.get("ps", "LONG"),
                side=o.get("S", "BUY"),
                order_status=mapped_status,
                cum_filled_qty=Decimal(str(o.get("z", "0"))),
                avg_price=Decimal(str(o.get("ap", "0"))),
                last_filled_qty=Decimal(str(o.get("l", "0"))),
                last_filled_price=Decimal(str(o.get("L", "0"))),
                exchange_order_id=int(o.get("i", 0)) if o.get("i") else None,
            )

        elif event_type == "ACCOUNT_UPDATE":
            a = event.get("a", {})
            balances: dict[str, Decimal] = {}
            for b in a.get("B", []):
                asset = b.get("a", "")
                wb = Decimal(str(b.get("wb", "0")))
                balances[asset] = wb

            await on_account_update(balances=balances)
