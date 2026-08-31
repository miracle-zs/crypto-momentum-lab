from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncService,
)


class FastSnapshotClient:
    def __init__(self) -> None:
        self.balance_calls = 0
        self.position_calls = 0
        self.balances = (
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="USDT",
                wallet_balance=Decimal("100"),
                available_balance=Decimal("80"),
                unrealized_pnl=Decimal("1.25"),
                observed_at=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
                raw_payload={},
            ),
        )
        self.positions = (
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side="BOTH",
                position_amt=Decimal("0.01"),
                entry_price=Decimal("10000"),
                mark_price=Decimal("10010"),
                unrealized_pnl=Decimal("0.10"),
                notional=Decimal("100.10"),
                leverage=10,
                margin_type="cross",
                observed_at=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
                raw_payload={},
            ),
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="ETHUSDT",
                position_side="BOTH",
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("2000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=10,
                margin_type="cross",
                observed_at=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
                raw_payload={},
            ),
        )

    async def fetch_balances(self):
        self.balance_calls += 1
        return self.balances

    async def fetch_positions(self):
        self.position_calls += 1
        return self.positions

    async def fetch_account_config(self):
        raise AssertionError("fast snapshots must not fetch account config")

    async def fetch_open_orders(self):
        raise AssertionError("fast snapshots must not fetch open orders")

    async def fetch_recent_fills(
        self,
        symbols=(),
        *,
        from_id_by_symbol=None,
        start_time_by_symbol=None,
    ):
        raise AssertionError("fast snapshots must not fetch recent fills")


class FastSnapshotRepository:
    def __init__(self) -> None:
        self.calls = []

    async def save_balance_position_snapshot(self, *, balances, positions):
        self.calls.append((balances, positions))


async def test_fast_snapshot_only_fetches_account_state_and_aligns_timestamp() -> None:
    client = FastSnapshotClient()
    repository = FastSnapshotRepository()
    service = ExecutionAccountSyncService(
        client=client,
        repository=repository,
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
        ),
    )
    observed_at = datetime(2026, 8, 28, 0, 0, 15, tzinfo=UTC)

    await service.snapshot_once(observed_at=observed_at)

    assert client.balance_calls == 1
    assert client.position_calls == 1
    assert len(repository.calls) == 1
    balances, positions = repository.calls[0]
    assert len(balances) == 1
    assert balances[0].observed_at == observed_at
    assert [position.symbol for position in positions] == ["BTCUSDT"]
    assert positions[0].observed_at == observed_at


async def test_fast_snapshot_skips_zero_balances_but_records_zero_transition() -> None:
    client = FastSnapshotClient()
    zero_bnb = AccountBalanceSnapshot(
        environment="live",
        account_label="primary",
        asset="BNB",
        wallet_balance=Decimal("0"),
        available_balance=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        observed_at=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
        raw_payload={},
    )
    client.balances = (*client.balances, zero_bnb)
    repository = FastSnapshotRepository()
    service = ExecutionAccountSyncService(
        client=client,
        repository=repository,
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
        ),
    )

    await service.snapshot_once(
        observed_at=datetime(2026, 8, 28, 0, 0, 15, tzinfo=UTC)
    )
    assert [item.asset for item in repository.calls[-1][0]] == ["USDT"]

    client.balances = (
        client.balances[0],
        replace(
            zero_bnb,
            wallet_balance=Decimal("2"),
            available_balance=Decimal("2"),
        ),
    )
    await service.snapshot_once(
        observed_at=datetime(2026, 8, 28, 0, 0, 30, tzinfo=UTC)
    )
    assert [item.asset for item in repository.calls[-1][0]] == ["USDT", "BNB"]

    client.balances = (*client.balances[:1], zero_bnb)
    await service.snapshot_once(
        observed_at=datetime(2026, 8, 28, 0, 0, 45, tzinfo=UTC)
    )
    assert [item.asset for item in repository.calls[-1][0]] == ["USDT", "BNB"]

    await service.snapshot_once(
        observed_at=datetime(2026, 8, 28, 0, 1, tzinfo=UTC)
    )
    assert [item.asset for item in repository.calls[-1][0]] == ["USDT"]
