import asyncio
from inspect import signature

import crypto_momentum_lab.execution_account.binance.user_data as user_data_module
from crypto_momentum_lab.apps.execution_account.main import sync_command
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUsdMUserDataStream,
)


class FakeListenKeyClient:
    def __init__(self) -> None:
        self.started = 0
        self.closed: list[str] = []

    async def start_user_data_stream(self) -> str:
        self.started += 1
        return f"listen-key-{self.started}"

    async def keepalive_user_data_stream(self, listen_key: str) -> None:
        return None

    async def close_user_data_stream(self, listen_key: str) -> None:
        self.closed.append(listen_key)


def test_user_data_stream_defaults_to_private_user_data_endpoint() -> None:
    expected = "wss://fstream.binance.com/private/ws"

    stream_default = signature(BinanceUsdMUserDataStream).parameters[
        "websocket_url"
    ].default
    command_default = signature(sync_command).parameters["websocket_url"].default

    assert stream_default == expected
    assert command_default == expected


async def test_stream_replaces_listen_key_after_expiration(monkeypatch) -> None:
    client = FakeListenKeyClient()
    stream = BinanceUsdMUserDataStream(
        listen_key_client=client,
        reconnect_delays=(0,),
    )
    attempts: list[str] = []

    async def fake_run_connection(listen_key: str) -> str:
        attempts.append(listen_key)
        if len(attempts) == 1:
            return "listen_key_expired"
        stream._stopping = True
        return "stopped"

    monkeypatch.setattr(stream, "_run_connection", fake_run_connection)

    await stream.run()

    assert attempts == ["listen-key-1", "listen-key-2"]
    assert client.closed == ["listen-key-2"]


class BlockingConnection:
    def __init__(self) -> None:
        self.read_count = 0
        self.second_read = asyncio.Event()
        self.release_first_handler = asyncio.Event()
        self.second_handler_finished = asyncio.Event()

    async def recv(self) -> dict[str, object]:
        self.read_count += 1
        if self.read_count == 1:
            return _account_update_payload(1783209600000)
        if self.read_count == 2:
            self.second_read.set()
            return _account_update_payload(1783209601000)
        await self.second_handler_finished.wait()
        return _account_update_payload(1783209602000)


class FakeConnectContext:
    def __init__(self, connection: BlockingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> BlockingConnection:
        return self._connection

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None


async def test_stream_reads_next_event_while_handler_is_still_processing(
    monkeypatch,
) -> None:
    connection = BlockingConnection()
    stream = BinanceUsdMUserDataStream(
        listen_key_client=FakeListenKeyClient(),
        keepalive_interval_seconds=3600,
        reconnect_delays=(0,),
    )
    handled_events = []

    async def handler(event) -> None:
        handled_events.append(event)
        if len(handled_events) == 1:
            await connection.release_first_handler.wait()
        elif len(handled_events) == 2:
            connection.second_handler_finished.set()
            stream._stopping = True

    monkeypatch.setattr(
        user_data_module,
        "connect",
        lambda *args, **kwargs: FakeConnectContext(connection),
    )
    stream.set_handler(handler)
    task = asyncio.create_task(stream._run_connection("listen-key"))

    try:
        await asyncio.wait_for(connection.second_read.wait(), timeout=0.1)
    finally:
        connection.release_first_handler.set()
        stream._stopping = True

    await asyncio.wait_for(task, timeout=1)
    assert [event.event_at.second for event in handled_events[:2]] == [0, 1]


def _account_update_payload(event_timestamp_ms: int) -> dict[str, object]:
    return {
        "e": "ACCOUNT_UPDATE",
        "E": event_timestamp_ms,
        "a": {"B": [], "P": []},
    }
