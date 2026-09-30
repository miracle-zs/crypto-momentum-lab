"""Notification connection behavior without a live database."""

import asyncio

import pytest

import crypto_momentum_lab.persistence.postgres.runtime_state_loader as loader_module
from crypto_momentum_lab.persistence.postgres.runtime_state_loader import (
    _AsyncPostgresRuntimeStateWakeup,
)


class Connection:
    def __init__(self):
        self.closed = False
        self.events = []

    def is_closed(self):
        return self.closed

    async def add_listener(self, channel, callback):
        self.events.append(("add", channel))
        self.callback = callback

    async def remove_listener(self, channel, callback):
        self.events.append(("remove", channel))

    async def close(self):
        self.events.append(("close",))
        self.closed = True


@pytest.mark.parametrize(
    "scheme", ["postgresql+asyncpg", "postgresql+psycopg", "postgresql"]
)
async def test_listener_connects_once_and_normalizes_driver_dsn(monkeypatch, scheme):
    connection = Connection()
    calls = []

    async def connect(dsn, **kwargs):
        calls.append((dsn, kwargs["timeout"]))
        return connection

    monkeypatch.setattr(loader_module.asyncpg, "connect", connect)
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url=f"{scheme}://localhost/research",
        environment="research",
        channel="ready",
    )
    await wakeup.ensure_started()
    await wakeup.ensure_started()
    assert calls == [("postgresql://localhost/research", 5.0)]
    assert connection.events == [("add", "ready")]
    await wakeup.close()
    await wakeup.close()
    assert connection.events == [("add", "ready"), ("remove", "ready"), ("close",)]


async def test_listener_registration_failure_closes_connection(monkeypatch):
    class FailingConnection(Connection):
        async def add_listener(self, channel, callback):
            raise RuntimeError("listen failed")

    connection = FailingConnection()

    async def connect(*args, **kwargs):
        return connection

    monkeypatch.setattr(loader_module.asyncpg, "connect", connect)
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url="postgresql://localhost/research",
        environment="research",
        channel="ready",
    )
    with pytest.raises(RuntimeError, match="listen failed"):
        await wakeup.ensure_started()
    assert connection.closed
    assert wakeup._connection is None


@pytest.mark.parametrize(
    "channel,payload,expected",
    [
        ("ready", "research", True),
        ("ready", "research|15s", True),
        ("ready", "live", False),
        ("ready", "research-other|15s", False),
        ("other", "research", False),
        ("ready", "", False),
    ],
)
def test_notifications_are_filtered_by_channel_and_environment(
    channel, payload, expected
):
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url="unused", environment="research", channel="ready"
    )
    wakeup._on_notification(None, 0, channel, payload)
    assert wakeup._event.is_set() is expected


async def test_queued_notification_is_consumed_once():
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url="unused", environment="research", channel="ready"
    )
    wakeup._connection = Connection()
    wakeup._on_notification(None, 0, "ready", "research")
    assert await wakeup.wait(0.01) is True
    assert not wakeup._event.is_set()
    assert await wakeup.wait(0.001) is False
    await wakeup.close()


async def test_cancelled_connect_is_not_converted_to_polling_fallback(monkeypatch):
    async def connect(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(loader_module.asyncpg, "connect", connect)
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url="unused", environment="research", channel="ready"
    )
    with pytest.raises(asyncio.CancelledError):
        await wakeup.wait(1)


async def test_connect_failure_waits_bounded_retry_before_polling(monkeypatch):
    delays = []

    async def connect(*args, **kwargs):
        raise OSError("database unavailable")

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(loader_module.asyncpg, "connect", connect)
    monkeypatch.setattr(loader_module.asyncio, "sleep", sleep)
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url="unused", environment="research", channel="ready"
    )
    assert await wakeup.wait(0.25) is False
    assert delays == [0.25]


async def test_listener_removal_failure_still_closes_connection():
    class FailingRemoval(Connection):
        async def remove_listener(self, channel, callback):
            raise RuntimeError("remove failed")

    connection = FailingRemoval()
    wakeup = _AsyncPostgresRuntimeStateWakeup(
        database_url="unused", environment="research", channel="ready"
    )
    wakeup._connection = connection
    await wakeup.close()
    assert connection.closed
    assert wakeup._connection is None
