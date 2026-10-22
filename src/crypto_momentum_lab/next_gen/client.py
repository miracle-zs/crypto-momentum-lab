from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import ssl
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import structlog
import websockets
from websockets.asyncio.client import ClientConnection

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


def get_binance_urls(environment: str = "live") -> tuple[str, str, str]:
    """返回 (rest_base_url, ws_private_base_url, ws_public_base_url)"""
    environment = environment.strip().lower()
    if environment not in ("live", "testnet"):
        raise ValueError(f"Unsupported trading environment: {environment}")
    if environment == "testnet":
        return (
            "https://testnet.binancefuture.com",
            "wss://stream.binancefuture.com/private/ws",
            "wss://stream.binancefuture.com/ws",
        )
    return (
        "https://fapi.binance.com",
        "wss://fstream.binance.com/private/ws",
        "wss://fstream.binance.com/market/ws",
    )


def get_binance_ws_api_url(environment: str = "live") -> str:
    environment = environment.strip().lower()
    if environment == "testnet":
        return "wss://testnet.binancefuture.com/ws-fapi/v1"
    if environment == "live":
        return "wss://ws-fapi.binance.com/ws-fapi/v1"
    raise ValueError(f"Unsupported trading environment: {environment}")


class BinanceRestClient:
    """下一代极简币安 USDⓈ-M 交易客户端"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str | None = None,
        environment: str = "live",
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.environment = environment
        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            default_rest, _, _ = get_binance_urls(environment)
            self.base_url = default_rest
        self._client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None
        self._ws_api_ssl_context: ssl.SSLContext | None = None
        self._ws_api_connection: ClientConnection | None = None

    async def close(self) -> None:
        ws_api_connection = self._ws_api_connection
        self._ws_api_connection = None
        if ws_api_connection is not None:
            try:
                await ws_api_connection.close()
            except Exception as error:
                log.debug("account_status_ws_close_failed", error=str(error))
        if self._owns_client:
            await self._client.aclose()

    def sign_params(
        self,
        params: dict[str, Any],
        timestamp: int | None = None,
        recv_window: int = 5000,
    ) -> dict[str, Any]:
        """为请求参数附加毫秒时间戳、recvWindow 与 HMAC-SHA256 签名"""
        signed_params = dict(params)
        if "recvWindow" not in signed_params:
            signed_params["recvWindow"] = recv_window
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
        _rate_limit_attempt: int = 0,
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
            # 429 on an idempotent read is safe to retry with the server's
            # requested delay.  Never retry a POST order here: it may already
            # have reached the matching engine.
            if (
                response.status_code == 429
                and method.upper() == "GET"
                and _rate_limit_attempt < 2
            ):
                retry_after_raw = response.headers.get("Retry-After", "")
                try:
                    retry_after = float(retry_after_raw)
                except ValueError:
                    retry_after = 5.0
                delay = min(max(retry_after, 1.0), 30.0)
                log.warning(
                    "binance_rate_limited_retry",
                    path=path,
                    attempt=_rate_limit_attempt + 1,
                    delay_seconds=delay,
                )
                await asyncio.sleep(delay)
                return await self._request(
                    method,
                    path,
                    signed=signed,
                    params=params,
                    _rate_limit_attempt=_rate_limit_attempt + 1,
                )

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
        hedge_mode: bool | None = None
        try:
            position_mode = await self._request(
                "GET", "/fapi/v1/positionSide/dual", signed=True
            )
            mode_value = position_mode["dualSidePosition"]
            if not isinstance(mode_value, bool):
                raise ValueError("Binance position mode was not a boolean")
            hedge_mode = mode_value
        except Exception as error:
            log.warning("account_position_mode_unavailable", error=str(error))

        wallet_balances: dict[str, Decimal] = {}
        for asset_data in res["assets"]:
            asset = asset_data["asset"]
            wb = Decimal(str(asset_data["walletBalance"]))
            wallet_balances[asset] = wb

        avail = Decimal(str(res["availableBalance"]))

        positions: list[PositionSnapshot] = []
        for pos_data in res["positions"]:
            pos_amt = Decimal(str(pos_data["positionAmt"]))
            positions.append(
                PositionSnapshot(
                    symbol=pos_data["symbol"],
                    position_side=pos_data["positionSide"],
                    position_amt=pos_amt,
                    entry_price=(
                        Decimal(str(pos_data["entryPrice"]))
                        if "entryPrice" in pos_data
                        else None
                    ),
                )
            )

        return AccountSnapshot(
            wallet_balances=wallet_balances,
            available_balance=avail,
            positions=positions,
            hedge_mode=hedge_mode,
        )

    async def fetch_account_margin_status(
        self,
    ) -> tuple[Decimal, Decimal, Decimal, int, bool]:
        """Read margin and available balance from the signed Futures WS API."""
        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "recvWindow": 5000,
            "timestamp": int(time.time() * 1000),
        }
        payload = urlencode(sorted(params.items()))
        params["signature"] = hmac.new(
            self.api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        request_id = str(uuid4())
        request = {
            "id": request_id,
            "method": "account.status",
            "params": params,
        }

        ws = self._ws_api_connection
        try:
            # Keep one signed-API connection for the 15-second status samples;
            # repeated short-lived TLS sessions retain native allocations.
            if ws is None:
                if self._ws_api_ssl_context is None:
                    self._ws_api_ssl_context = ssl.create_default_context()
                ws = await websockets.connect(
                    get_binance_ws_api_url(self.environment),
                    ssl=self._ws_api_ssl_context,
                    open_timeout=3,
                    close_timeout=1,
                    ping_interval=20,
                )
                self._ws_api_connection = ws
            await ws.send(json.dumps(request))
            response = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
            if response.get("id") != request_id or response.get("status") != 200:
                raise RuntimeError(f"Binance account margin request failed: {response}")
            result = response["result"]
            margin_used = Decimal(str(result["totalPositionInitialMargin"])) + Decimal(
                str(result["totalOpenOrderInitialMargin"])
            )
            return (
                margin_used,
                Decimal(str(result["totalMarginBalance"])),
                Decimal(str(result["availableBalance"])),
                int(result["feeTier"]),
                bool(result["multiAssetsMargin"]),
            )
        except Exception:
            self._ws_api_connection = None
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass
            raise

    async def fetch_startup_order(
        self, symbol: str, client_order_id: str
    ) -> dict[str, Any]:
        """Read a stored order once during process startup, never during RUNNING."""
        return dict(
            await self._request(
                "GET",
                "/fapi/v1/order",
                signed=True,
                params={"symbol": symbol, "origClientOrderId": client_order_id},
            )
        )

    async def fetch_startup_realized_pnl(self, cutoff: datetime) -> Decimal:
        """Read the UTC day's exchange realized PNL at a fixed startup cutoff."""
        start = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
        rows = await self._request(
            "GET",
            "/fapi/v1/income",
            signed=True,
            params={
                "incomeType": "REALIZED_PNL",
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(cutoff.timestamp() * 1000),
                "limit": 1000,
            },
        )
        if len(rows) >= 1000:
            raise RuntimeError("Startup daily PNL exceeds a complete income response")
        return sum(
            (Decimal(row["income"]) for row in rows if row["asset"] == "USDT"),
            Decimal("0"),
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
        self,
        symbol: str,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> dict[str, Any]:
        """DELETE /fapi/v1/order - 撤单"""
        params: dict[str, Any] = {"symbol": symbol}
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        res = await self._request(
            "DELETE", "/fapi/v1/order", signed=True, params=params
        )
        return dict(res)

    async def fetch_exchange_info(self) -> dict[str, Any]:
        """GET /fapi/v1/exchangeInfo - 获取交易所元数据与交易对过滤器"""
        res = await self._request("GET", "/fapi/v1/exchangeInfo", signed=False)
        return dict(res)

    async def get_position_mode(self) -> bool:
        """GET /fapi/v1/positionSide/dual - 查询持仓模式 (True 为双向持仓 Hedge Mode)"""
        res = await self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        mode = res["dualSidePosition"]
        if not isinstance(mode, bool):
            raise ValueError("Invalid dualSidePosition in account response")
        return mode

    async def set_position_mode(self, dual_side: bool = True) -> None:
        """POST /fapi/v1/positionSide/dual - 设置持仓模式为双向持仓"""
        try:
            await self._request(
                "POST",
                "/fapi/v1/positionSide/dual",
                signed=True,
                params={"dualSidePosition": "true" if dual_side else "false"},
            )
        except BinanceOrderRejectedError as e:
            if e.code == -4059:
                return
            raise

    async def set_leverage(self, symbol: str, leverage: int = 5) -> dict[str, Any]:
        """POST /fapi/v1/leverage - 设置杠杆倍数"""
        res = await self._request(
            "POST",
            "/fapi/v1/leverage",
            signed=True,
            params={"symbol": symbol.upper(), "leverage": leverage},
        )
        return dict(res)

    async def set_margin_type(self, symbol: str, margin_type: str) -> None:
        """POST /fapi/v1/marginType - 设置全仓/逐仓保证金模式。"""
        normalized = margin_type.strip().upper()
        if normalized not in ("CROSSED", "ISOLATED"):
            raise ValueError(f"Unsupported margin type: {margin_type}")
        try:
            await self._request(
                "POST",
                "/fapi/v1/marginType",
                signed=True,
                params={"symbol": symbol.upper(), "marginType": normalized},
            )
        except BinanceOrderRejectedError as error:
            # Binance returns -4046 when the requested mode already applies.
            if error.code == -4046:
                return
            raise


class BinanceUserDataStream:
    """币安私有流监听组件 (物理网络长连、事件反序列化分发、自动重连与断连守卫)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        ws_base_url: str | None = None,
        environment: str = "live",
        on_status_change: Callable[[bool], None] | None = None,
        ws_connector: Any = None,
        initial_backoff: float = 1.0,
    ) -> None:
        self._rest = rest_client
        self.environment = environment
        if ws_base_url:
            self._ws_base_url = ws_base_url.rstrip("/")
        else:
            _, default_ws, _ = get_binance_urls(environment)
            self._ws_base_url = default_ws
        self.on_status_change = on_status_change
        self._ws_connector = ws_connector
        self.initial_backoff = initial_backoff
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
                exchange_realized_pnl=Decimal(str(o["rp"])) if "rp" in o else None,
                trade_payload=dict(o) if o.get("x") == "TRADE" else None,
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
            self.listen_key = None
            self.is_connected = False
            if self.on_status_change:
                self.on_status_change(False)
            if self._ws:
                try:
                    await self._ws.close()
                except Exception:
                    pass
                self._ws = None

    async def _ensure_connection(self) -> None:
        """建立物理 WebSocket 连接"""
        if self._ws is not None:
            return

        if not self.listen_key:
            self.listen_key = await self._rest.create_listen_key()

        if self._ws_connector is False:
            self.is_connected = True
            return

        ws_url = f"{self._ws_base_url}/{self.listen_key}"
        try:
            if self._ws_connector is not None:
                self._ws = await self._ws_connector(ws_url)
            else:
                self._ws = await websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                )
            self.is_connected = True
            if self.on_status_change:
                self.on_status_change(True)
        except Exception as e:
            self.is_connected = False
            if self.on_status_change:
                self.on_status_change(False)
            raise ConnectionError(f"WebSocket 物理连接失败: {e}") from e

    async def connect(
        self,
        on_event: Callable[[dict[str, Any]], Any] | None = None,
        connect_network: bool = True,
    ) -> None:
        """建立 UserData WebSocket 长连并握手获取 listenKey"""
        self._on_event = on_event
        if connect_network and self._ws_connector is not False:
            await self._ensure_connection()
        else:
            if not self.listen_key:
                self.listen_key = await self._rest.create_listen_key()
            self.is_connected = True
            if self.on_status_change:
                self.on_status_change(True)

    async def run_stream(
        self,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """持久化私有流接收循环：支持 24h 断连自愈与指数退避自动重连"""
        backoff = self.initial_backoff
        while not (stop_event and stop_event.is_set()):
            try:
                await self._ensure_connection()

                if self._ws is None:
                    if stop_event is not None:
                        await stop_event.wait()
                    break

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
                        # A handler failure may mean an order/account update was
                        # lost. Leave this socket and let the outer loop reconnect;
                        # AccountState keeps its private gap latched for startup
                        # reconciliation when there is live responsibility.
                        raise
                    backoff = self.initial_backoff
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("ws_connection_interrupted", err=str(e), retry_in=backoff)
            finally:
                self.is_connected = False
                if self.on_status_change:
                    self.on_status_change(False)
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            if stop_event and stop_event.is_set():
                break

            # 发生断连，清空过期 listen_key 并退避等待后重连
            self.listen_key = None
            try:
                if stop_event:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                    break
                else:
                    await asyncio.sleep(backoff)
            except TimeoutError:
                pass
            backoff = min(30.0, backoff * 2.0)

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
