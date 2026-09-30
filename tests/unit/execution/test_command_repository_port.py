"""Awaited port and explicit legacy adaptation acceptance."""

from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.legacy_command_repository import (
    LegacyCommandRepositoryAdapter,
)


@pytest.mark.parametrize("scoped", [False, True])
async def test_legacy_sync_loader_is_awaited_and_scope_forwarding_is_explicit(scoped):
    calls = []

    class Legacy:
        def load_active_execution_commands(self):
            calls.append(None)
            return ({"command_id": "legacy"},)

    repo = Legacy()
    if scoped:

        def loader(account_label=None):
            calls.append(account_label)
            return ({"command_id": "legacy"},)

        repo.load_active_execution_commands = loader
    result = await LegacyCommandRepositoryAdapter(repo).load_active_execution_commands(
        account_label="account-3"
    )
    assert result == ({"command_id": "legacy"},)
    assert calls == ["account-3" if scoped else None]


async def test_legacy_async_failure_propagates_without_becoming_empty_restore():
    class Legacy:
        async def load_active_execution_commands(self, account_label=None):
            raise RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        await LegacyCommandRepositoryAdapter(Legacy()).load_active_execution_commands(
            account_label="account-3"
        )


@pytest.mark.parametrize("method", ["load_seen_event_ids", "load_seen_fill_trade_ids"])
async def test_missing_legacy_identity_capability_fails_explicitly(method):
    adapter = LegacyCommandRepositoryAdapter(object())
    with pytest.raises(RuntimeError, match="does not implement " + method):
        await getattr(adapter, method)()


async def test_legacy_sync_write_receives_unchanged_outbox_payload():
    class Legacy:
        def upsert_execution_command(self, **kwargs):
            self.write = kwargs

    repo = Legacy()
    values = dict(
        command_id="c1",
        client_order_id="c1",
        command="exit",
        status="unknown",
        requested_at=datetime(2026, 9, 30, tzinfo=UTC),
        details={"reservations": ["r1"]},
    )
    await LegacyCommandRepositoryAdapter(repo).upsert_execution_command(**values)
    assert repo.write == values


async def test_book_uses_awaited_port_without_inspecting_command_signatures(
    monkeypatch,
):
    calls = []

    class AwaitedPort:
        async def load_active_execution_commands(self, *, account_label):
            calls.append(("commands", account_label))
            return ()

        async def load_seen_event_ids(self):
            calls.append(("events", None))
            return ("existing-event",)

        async def load_seen_fill_trade_ids(self):
            calls.append(("trades", None))
            return ("existing-trade",)

        async def load_execution_order_watermarks(self, *, account_label):
            calls.append(("watermarks", account_label))
            return ()

    def no_signature_probe(*args, **kwargs):
        raise AssertionError("Book inspected a command method signature")

    monkeypatch.setattr("inspect.signature", no_signature_probe)
    book = ExecutionBook(command_repository=AwaitedPort())
    await book.restore(account_label="account-3")
    assert calls == [
        ("commands", "account-3"),
        ("events", None),
        ("trades", None),
        ("watermarks", "account-3"),
    ]
    assert "existing-event" in book._seen_evidence_ids
    assert "existing-trade" in book._seen_trade_ids
    assert not book._persistence_failed
