from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
import websockets

from crypto_momentum_lab.next_gen.strategy_engine import Bar15s

log = structlog.get_logger("market_feed")


class MarketDataFeed:
    """下一代公共行情接入组件 (连接 Market-State Hub 消费 15s 权威聚合 Bar)"""

    def __init__(
        self,
        symbols: list[str],
        on_bar: Callable[[Bar15s], Any],
        hub_url: str | None = None,
        # 保留 public_ws_base 参数以向后兼容，但运行时不用于直连 1m 流
        public_ws_base: str = "wss://fstream.binance.com",
        ws_connector: Any = None,
        initial_backoff: float = 1.0,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.on_bar = on_bar
        self.hub_url = (
            hub_url.rstrip("/")
            if hub_url
            else os.environ.get(
                "CML_MARKET_STATE_HUB_URL", "ws://market-data:8766"
            ).rstrip("/")
        )
        self.public_ws_base = public_ws_base.rstrip("/")
        self._ws_connector = ws_connector
        self.initial_backoff = initial_backoff
        self.is_connected: bool = False
        self._ws: Any = None

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """持续运行行情推流接收循环，支持自动重连与优雅停机"""
        target_url = self.hub_url
        log.info(
            "market_feed_starting",
            source="hub",
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
        """解析来自 Market-State Hub 的 15s Bar 载荷并分发给策略引擎"""
        # 1. 显式拒绝 1m kline 格式 (1m 无法冒充 15s Bar，避免动量时间尺度失真)
        is_kline = ("data" in data and "k" in data["data"]) or (
            "stream" in data and "@kline" in str(data.get("stream"))
        )
        if is_kline:
            log.warning(
                "market_feed_rejecting_1m_kline",
                reason="1m kline stream cannot substitute 15s bar without aggregation",
            )
            return

        # 2. Market-State Hub 格式 (包含 states 列表)
        if "states" in data and isinstance(data["states"], list):
            for s in data["states"]:
                if not isinstance(s, dict):
                    continue
                symbol = str(s.get("symbol", "")).upper()
                if symbol in self.symbols:
                    ts_val = s.get("timestamp") or s.get("bucket_end")
                    if isinstance(ts_val, (int, float)):
                        close_time = datetime.fromtimestamp(ts_val / 1000.0, tz=UTC)
                    elif isinstance(ts_val, str):
                        try:
                            close_time = datetime.fromisoformat(ts_val)
                        except ValueError:
                            close_time = datetime.now(UTC)
                    else:
                        close_time = datetime.now(UTC)

                    open_val = s.get("open_price", s.get("open", s.get("price", "0")))
                    high_val = s.get("high_price", s.get("high", s.get("price", "0")))
                    low_val = s.get("low_price", s.get("low", s.get("price", "0")))
                    close_val = s.get(
                        "close_price", s.get("close", s.get("price", "0"))
                    )
                    vol_val = s.get("volume", s.get("trade_notional", "0"))

                    bar = Bar15s(
                        symbol=symbol,
                        close_time=close_time,
                        open_price=Decimal(str(open_val)),
                        high_price=Decimal(str(high_val)),
                        low_price=Decimal(str(low_val)),
                        close_price=Decimal(str(close_val)),
                        volume=Decimal(str(vol_val)),
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
