"""Awaited command repository port acceptance."""



from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


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
