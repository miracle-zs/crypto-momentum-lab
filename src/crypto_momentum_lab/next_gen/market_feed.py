from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from typing import Any

import structlog
import websockets

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.state_codec import market_state_from_payload

log = structlog.get_logger("market_feed")


class MarketDataFeed:
    """下一代公共行情接入组件 (连接 Market-State Hub 消费 15s 权威聚合 Bar)"""

    def __init__(
        self,
        symbols: list[str],
        on_market_state: Callable[[MarketState15s], Any],
        hub_url: str | None = None,
        ws_connector: Any = None,
        initial_backoff: float = 1.0,
        environment: str = "research",
        consumer_id: str = "nextgen",
        on_connection_change: Callable[[bool], None] | None = None,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.on_market_state = on_market_state
        self.hub_url = (
            hub_url.rstrip("/")
            if hub_url
            else os.environ.get(
                "CML_MARKET_STATE_HUB_URL", "ws://market-data:8766"
            ).rstrip("/")
        )
        self._ws_connector = ws_connector
        self.initial_backoff = initial_backoff
        self.environment = environment
        self.consumer_id = consumer_id
        self.on_connection_change = on_connection_change
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
                else:
                    self._ws = await websockets.connect(
                        target_url,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=10,
                        max_size=4 * 1024 * 1024,
                    )
                if self._ws is None:
                    raise ConnectionError("Market Hub connection was not established")
                await self._ws.send(
                    json.dumps(
                        {
                            "type": "subscribe_market_states",
                            "schema_version": 1,
                            "environment": self.environment,
                            "consumer_id": self.consumer_id,
                        }
                    )
                )
                ready = json.loads(await asyncio.wait_for(self._ws.recv(), timeout=10))
                if (
                    ready.get("type") != "market_state_hub_ready"
                    or ready.get("environment") != self.environment
                ):
                    raise ValueError("Market Hub subscription was not acknowledged")
                self.is_connected = True
                if self.on_connection_change:
                    self.on_connection_change(True)
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
                if self.on_connection_change:
                    self.on_connection_change(False)
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
        """Accept the canonical Hub schema, including all order-flow fields."""
        if "states" not in data:
            return
        for payload in data["states"]:
            market_state = market_state_from_payload(payload)
            if market_state.symbol not in self.symbols:
                continue
            result = self.on_market_state(market_state)
            if asyncio.iscoroutine(result):
                await result

    async def close(self) -> None:
        """关闭底层连接"""
        self.is_connected = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
