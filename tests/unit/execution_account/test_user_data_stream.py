from inspect import signature

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
