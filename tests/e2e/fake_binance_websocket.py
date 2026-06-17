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
    _connection_count: int = 0
    _connection_event: asyncio.Event = field(default_factory=asyncio.Event)

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
                await connection.send(
                    json.dumps({"result": None, "id": request.get("id")})
                )
                await connection.send(
                    json.dumps(
                        {
                            "stream": "btcusdt@aggTrade",
                            "data": {
                                "e": "aggTrade",
                                "E": 1781488800000,
                                "s": "BTCUSDT",
                                "a": connection_index,
                            },
                        }
                    )
                )
                if connection_index == 1:
                    await connection.close()
                    return
            elif method == "UNSUBSCRIBE":
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
