"""Restarted execution uses durable identities to reject duplicate evidence."""

from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
)


async def test_restored_identities_prevent_replay_and_request_missing_trade_facts():
    loaded_accounts = []

    class Repository:
        async def load_active_execution_commands(self, *, account_label):
            loaded_accounts.append(account_label)
            return ()

        async def load_seen_event_ids(self):
            return ("existing-event",)

        async def load_seen_fill_trade_ids(self):
            return ("existing-trade",)

        async def load_execution_order_watermarks(self, *, account_label):
            return ()

    book = ExecutionBook(command_repository=Repository())
    await book.restore(account_label="account-3")
    assert loaded_accounts == ["account-3"]
    scope = ExecutionScope("live", "account-3", "BTCUSDT")
    now = datetime(2026, 10, 4, tzinfo=UTC)

    def evidence(identity, trade_id):
        fill = AccountFillEvent(
            environment="live", account_label="account-3", symbol="BTCUSDT",
            trade_id=trade_id, order_id="opening", side="BUY", price=Decimal("100"),
            quantity=Decimal("1"), realized_pnl=Decimal("0"), fee=Decimal("0"),
            fee_asset="USDT", trade_at=now, raw_payload={"positionSide": "BOTH", "is_system": True},
        )
        return ExecutionEvidence(identity, scope, now, fill=fill)

    assert isinstance(await book.observe(ExecutionEvidence("existing-event", scope, now)), Duplicate)
    assert (await book.read(scope)).total_quantity == 0
    replayed_trade = await book.observe(evidence("new-envelope", "existing-trade"))
    assert isinstance(replayed_trade, EvidenceConflict)
    assert "journal facts are unavailable" in replayed_trade.reason
    assert (await book.read(scope)).total_quantity == 0
    assert isinstance(await book.observe(evidence("fresh-envelope", "fresh-trade")), Applied)
    assert (await book.read(scope)).total_quantity == 1
