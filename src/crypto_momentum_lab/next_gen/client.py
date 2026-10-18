from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog
import websockets

from crypto_momentum_lab.next_gen.engine import AccountSnapshot, PositionSnapshot
from crypto_momentum_lab.next_gen.models import OrderStatus

log = structlog.get_logger("next_gen_client")


class BinanceOrderRejectedError(Exception):
    """币安订单网关业务拒单异常 (如 -2010 余额不足, -2019 保证金不足)"""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"Binance rejected order [{code}]: {message}")
        self.code = code
        self.message = message


class BinanceNetworkError(Exception):
    """网络超时或 5xx 异常，订单状态属于【存疑 (in-doubt)】，严禁判定为 REJECTED"""

    def __init__(self, status_code: int | None, message: str) -> None:
        super().__init__(f"Binance network/server error [{status_code}]: {message}")
        self.status_code = status_code
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

        try:
            response = await self._client.request(
                method=method,
                url=url,
                params=req_params if method.upper() in ("GET", "DELETE") else None,
                data=req_params if method.upper() in ("POST", "PUT") else None,
                headers=headers,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            raise BinanceNetworkError(
                status_code=None,
                message=f"HTTP network/timeout failure: {e}",
            ) from e

        if response.is_error:
            # 1. 5xx 服务端错误、408 超时、429 限频：状态存疑或不可用，严禁标记为已拒单
            if response.status_code >= 500 or response.status_code in (408, 429):
                raise BinanceNetworkError(
                    status_code=response.status_code,
                    message=f"HTTP status {response.status_code}: {response.text}",
                )

            # 2. 尝试提取币安 JSON 错误码与错误信息
            code = response.status_code
            msg = response.text
            try:
                err_data = response.json()
                code = int(err_data.get("code", response.status_code))
                msg = str(err_data.get("msg", response.text))
            except Exception:
                pass

            # 3. 币安系统未知处理/超时错误码 (-1000, -1006, -1007)
            # 这类错误发生时订单可能已被撮合引擎接收并成交，状态存疑，必须走看门狗查证
            if code in (-1000, -1006, -1007):
                raise BinanceNetworkError(
                    status_code=response.status_code,
                    message=f"Binance in-doubt error (code={code}): {msg}",
                )

            # 4. 明确的业务拒单 (如 -2010 余额不足, -2019 保证金不足, -1013 精度非法等)
            raise BinanceOrderRejectedError(code=code, message=msg)

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

    async def cancel_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        """DELETE /fapi/v1/order - 撤单"""
        params = {"symbol": symbol, "origClientOrderId": client_order_id}
        res = await self._request(
            "DELETE", "/fapi/v1/order", signed=True, params=params
        )
        return dict(res)

    async def query_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        """GET /fapi/v1/order - 只读探针查单 (绝不回写内存)"""
        params = {"symbol": symbol, "origClientOrderId": client_order_id}
        res = await self._request("GET", "/fapi/v1/order", signed=True, params=params)
        return dict(res)

    # 兼容性别名
    get_open_orders = fetch_open_orders
    get_account_v3 = fetch_account_information


class BinanceUserDataStream:
    """币安私有流监听组件 (物理网络长连、事件反序列化分发、30m 续期与断连守卫)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        ws_base_url: str = "wss://fstream.binance.com/ws",
        on_status_change: Callable[[bool], None] | None = None,
        ws_connector: Any = None,
    ) -> None:
        self._rest = rest_client
        self._ws_base_url = ws_base_url.rstrip("/")
        self.on_status_change = on_status_change
        self._ws_connector = ws_connector
        self.listen_key: str | None = None
        self.is_connected: bool = False
        self._ws: Any = None
        self._on_event: Callable[[dict[str, Any]], Any] | None = None

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
                "EXPIRED_IN_MATCH": OrderStatus.CANCELED,
                "REJECTED": OrderStatus.REJECTED,
            }
            if status_str not in status_mapping:
                log.warning("unknown_order_status_from_ws", raw_status=status_str)
                return
            mapped_status = status_mapping[status_str]

            tx_time_ms = o.get("T") or event.get("T")
            trade_time = (
                datetime.fromtimestamp(tx_time_ms / 1000.0, tz=UTC)
                if tx_time_ms
                else None
            )

            res = on_order_trade_update(
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
                trade_time=trade_time,
            )
            if asyncio.iscoroutine(res):
                await res

        elif event_type == "ACCOUNT_UPDATE":
            a = event.get("a", {})
            balances: dict[str, Decimal] = {}
            for b in a.get("B", []):
                asset = b.get("a", "")
                wb = Decimal(str(b.get("wb", "0")))
                balances[asset] = wb

            positions = a.get("P", [])
            res = on_account_update(balances=balances, positions=positions)
            if asyncio.iscoroutine(res):
                await res

        elif event_type == "listenKeyExpired":
            log.warning("binance_listen_key_expired", event=event)
            self.is_connected = False
            if self.on_status_change:
                self.on_status_change(False)

    async def connect(
        self,
        on_event: Callable[[dict[str, Any]], Any] | None = None,
        connect_network: bool = True,
    ) -> None:
        """建立 UserData WebSocket 长连并握手获取 listenKey"""
        self.listen_key = await self._rest.create_listen_key()
        self._on_event = on_event

        if connect_network and self._ws_connector is not False:
            ws_url = f"{self._ws_base_url}/{self.listen_key}"
            try:
                if self._ws_connector is not None:
                    self._ws = await self._ws_connector(ws_url)
                elif not self.listen_key.startswith("fake_"):
                    self._ws = await websockets.connect(
                        ws_url,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=10,
                    )
            except Exception as e:
                log.error("ws_connect_failed", url=ws_url, err=str(e))
                self.is_connected = False
                if self.on_status_change:
                    self.on_status_change(False)
                raise ConnectionError(f"WebSocket 物理连接失败: {e}") from e

        self.is_connected = True
        if self.on_status_change:
            self.on_status_change(True)

    async def run_stream(
        self,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """私有流接收循环：读取原始文本帧，反序列化后调用 dispatch_raw_event"""
        if self._ws is None:
            if stop_event is not None:
                await stop_event.wait()
            return

        try:
            async for raw_msg in self._ws:
                if stop_event and stop_event.is_set():
                    break
                try:
                    text_msg = (
                        raw_msg.decode("utf-8")
                        if isinstance(raw_msg, bytes)
                        else str(raw_msg)
                    )
                    data = json.loads(text_msg)
                    if self._on_event:
                        res = self._on_event(data)
                        if asyncio.iscoroutine(res):
                            await res
                except Exception as e:
                    log.error(
                        "ws_event_dispatch_error",
                        err=str(e),
                        payload=str(raw_msg)[:200],
                    )
                    if self.on_status_change:
                        self.on_status_change(False)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("ws_connection_lost", err=str(e))
        finally:
            self.is_connected = False
            if self.on_status_change:
                self.on_status_change(False)

    async def run_with_keepalive(
        self,
        keepalive_interval_sec: float = 1800.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """30m 定时续期 listenKey，直到 stop_event 被触发或任务被取消"""
        while True:
            if stop_event and stop_event.is_set():
                break
            try:
                if stop_event is not None:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=keepalive_interval_sec
                        )
                        break
                    except TimeoutError:
                        pass
                else:
                    await asyncio.sleep(keepalive_interval_sec)

                if self.listen_key:
                    await self._rest.keepalive_listen_key(self.listen_key)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("listen_key_keepalive_failed", err=str(e))

    async def close(self) -> None:
        """断开连接并通知状态守卫"""
        self.is_connected = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception as e:
                log.warning("ws_close_error", err=str(e))
            self._ws = None
        if self.on_status_change:
            self.on_status_change(False)
