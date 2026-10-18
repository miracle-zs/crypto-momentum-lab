from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
import websockets

from crypto_momentum_lab.next_gen.strategy_engine import Bar15s

log = structlog.get_logger("market_feed")


class MarketDataFeed:
    """下一代公共行情接入组件 (支持 Market-State Hub 与币安公共行情流直接接入)"""

    def __init__(
        self,
        symbols: list[str],
        on_bar: Callable[[Bar15s], Any],
        hub_url: str | None = None,
        public_ws_base: str = "wss://fstream.binance.com",
        ws_connector: Any = None,
        initial_backoff: float = 1.0,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.on_bar = on_bar
        self.hub_url = hub_url.rstrip("/") if hub_url else None
        self.public_ws_base = public_ws_base.rstrip("/")
        self._ws_connector = ws_connector
        self.initial_backoff = initial_backoff
        self.is_connected: bool = False
        self._ws: Any = None

    def _build_binance_stream_url(self) -> str:
        """根据标的列表构建币安公共多流订阅 URL (1m Kline 流)"""
        stream_names = [f"{sym.lower()}@kline_1m" for sym in self.symbols]
        combined = "/".join(stream_names)
        return f"{self.public_ws_base}/stream?streams={combined}"

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """持续运行行情推流接收循环，支持自动重连与优雅停机"""
        target_url = self.hub_url or self._build_binance_stream_url()
        log.info(
            "market_feed_starting",
            source="hub" if self.hub_url else "binance_public_stream",
            url=target_url,
            symbols=self.symbols,
        )

        backoff = self.initial_backoff
        while not (stop_event and stop_event.is_set()):
            try:
                if self._ws_connector is not None:
                    self._ws = await self._ws_connector(target_url)
                elif not target_url.startswith("fake_"):
                    self._ws = await websockets.connect(
                        target_url,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=10,
                    )
                self.is_connected = True
                backoff = self.initial_backoff
                log.info("market_feed_connected", url=target_url)

                if self._ws is None:
                    if stop_event:
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
                        payload = json.loads(text_msg)
                        await self._dispatch_message(payload)
                    except Exception as e:
                        log.warning(
                            "market_feed_dispatch_error",
                            err=str(e),
                            payload=str(raw_msg)[:200],
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("market_feed_connection_lost", err=str(e), retry_in=backoff)
            finally:
                self.is_connected = False
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            if stop_event and stop_event.is_set():
                break

            try:
                if stop_event:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                    break
                else:
                    await asyncio.sleep(backoff)
            except TimeoutError:
                pass
            backoff = min(30.0, backoff * 2.0)

    async def _dispatch_message(self, data: dict[str, Any]) -> None:
        """解析来自 Hub 或币安 Public Stream 的行情载荷并转换分发 Bar15s"""
        # 1. 币安公共 Combined Stream 格式: {"stream": "...", "data": {"k": {...}}}
        if "data" in data and "k" in data["data"]:
            k = data["data"]["k"]
            symbol = str(k.get("s", "")).upper()
            if symbol in self.symbols:
                close_time_ms = int(k.get("T", 0))
                close_time = (
                    datetime.fromtimestamp(close_time_ms / 1000.0, tz=UTC)
                    if close_time_ms
                    else datetime.now(UTC)
                )
                bar = Bar15s(
                    symbol=symbol,
                    close_time=close_time,
                    open_price=Decimal(str(k.get("o", "0"))),
                    high_price=Decimal(str(k.get("h", "0"))),
                    low_price=Decimal(str(k.get("l", "0"))),
                    close_price=Decimal(str(k.get("c", "0"))),
                    volume=Decimal(str(k.get("v", "0"))),
                )
                res = self.on_bar(bar)
                if asyncio.iscoroutine(res):
                    await res
            return

        # 2. Local Market-State Hub 格式 (包含 states 列表)
        if "states" in data:
            for s in data["states"]:
                symbol = str(s.get("symbol", "")).upper()
                if symbol in self.symbols:
                    ts_val = s.get("timestamp") or s.get("bucket_end")
                    if isinstance(ts_val, (int, float)):
                        close_time = datetime.fromtimestamp(ts_val / 1000.0, tz=UTC)
                    elif isinstance(ts_val, str):
                        close_time = datetime.fromisoformat(ts_val)
                    else:
                        close_time = datetime.now(UTC)

                    bar = Bar15s(
                        symbol=symbol,
                        close_time=close_time,
                        open_price=Decimal(str(s.get("open", s.get("price", "0")))),
                        high_price=Decimal(str(s.get("high", s.get("price", "0")))),
                        low_price=Decimal(str(s.get("low", s.get("price", "0")))),
                        close_price=Decimal(str(s.get("close", s.get("price", "0")))),
                        volume=Decimal(str(s.get("volume", "0"))),
                    )
                    res = self.on_bar(bar)
                    if asyncio.iscoroutine(res):
                        await res

    async def close(self) -> None:
        """关闭底层连接"""
        self.is_connected = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
