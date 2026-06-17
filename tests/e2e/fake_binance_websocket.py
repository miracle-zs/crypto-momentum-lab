import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import pytest
from websockets.asyncio.server import Server, ServerConnection, serve


@dataclass(slots=True)
class FakeBinanceWebSocketServer:
    server: Server
    port: int
    subscribe_requests: list[tuple[str, ...]] = field(default_factory=list)
    unsubscribe_requests: list[tuple[str, ...]] = field(default_factory=list)
    control_events: list[tuple[str, tuple[str, ...]]] = field(
        default_factory=list
    )
    close_first_connection: bool = True
    _connection_count: int = 0
    _connection_event: asyncio.Event = field(default_factory=asyncio.Event)
    _control_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def market_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/market/ws"

    @property
    def public_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/public/ws"

    async def wait_for_connections(self, count: int) -> None:
        while self._connection_count < count:
            await asyncio.wait_for(self._connection_event.wait(), timeout=5)
            self._connection_event.clear()

    async def wait_for_subscriptions(self, names: set[str]) -> None:
        while not names.issubset(
            {name for request in self.subscribe_requests for name in request}
        ):
            await asyncio.wait_for(self._control_event.wait(), timeout=5)
            self._control_event.clear()

    async def stop(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, connection: ServerConnection) -> None:
        self._connection_count += 1
        connection_index = self._connection_count
        self._connection_event.set()
        async for message in connection:
            request = json.loads(message)
            method = request.get("method")
            if method == "SUBSCRIBE":
                names = tuple(request.get("params", ()))
                self.subscribe_requests.append(names)
                self.control_events.append(("SUBSCRIBE", names))
                self._control_event.set()
                await connection.send(
                    json.dumps({"result": None, "id": request.get("id")})
                )
                for sequence, name in enumerate(names, start=connection_index):
                    await connection.send(json.dumps(_stream_message(name, sequence)))
                if self.close_first_connection and connection_index == 1:
                    await connection.close()
                    return
            elif method == "UNSUBSCRIBE":
                names = tuple(request.get("params", ()))
                self.unsubscribe_requests.append(names)
                self.control_events.append(("UNSUBSCRIBE", names))
                self._control_event.set()
                await connection.send(
                    json.dumps({"result": None, "id": request.get("id")})
                )


@pytest.fixture
async def fake_binance_server() -> AsyncIterator[FakeBinanceWebSocketServer]:
    holder: dict[str, FakeBinanceWebSocketServer] = {}

    async def handle(connection: ServerConnection) -> None:
        await holder["server"].handle(connection)

    server = await serve(handle, "127.0.0.1", 0)
    socket = server.sockets[0]
    port = socket.getsockname()[1]
    fake = FakeBinanceWebSocketServer(server=server, port=port)
    holder["server"] = fake
    try:
        yield fake
    finally:
        await fake.stop()


def _stream_message(name: str, sequence: int) -> dict[str, object]:
    symbol, _, stream = name.partition("@")
    symbol = symbol.upper()
    event_time = 1781488800000 + sequence
    if stream == "aggTrade":
        data: dict[str, object] = {
            "e": "aggTrade",
            "E": event_time,
            "s": symbol,
            "a": sequence,
        }
    elif stream == "bookTicker":
        data = {
            "e": "bookTicker",
            "s": symbol,
            "u": sequence,
            "b": "100.0",
            "a": "100.1",
        }
    elif stream == "forceOrder":
        data = {
            "e": "forceOrder",
            "E": event_time,
            "o": {
                "s": symbol,
                "T": event_time,
            },
        }
    elif stream == "markPrice@1s":
        data = {
            "e": "markPriceUpdate",
            "E": event_time,
            "s": symbol,
            "p": "100.0",
        }
    elif stream == "kline_1m":
        data = {
            "e": "kline",
            "E": event_time,
            "s": symbol,
            "k": {
                "t": event_time,
                "s": symbol,
            },
        }
    else:
        raise AssertionError(f"unsupported stream in test fake: {stream}")
    return {"stream": name, "data": data}
