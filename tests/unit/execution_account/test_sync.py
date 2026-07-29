from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncService,
)


class FakeClient:
    def __init__(self, *, multi_assets_mode: bool = False) -> None:
        self.config = AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=multi_assets_mode,
            can_trade=True,
            fee_tier=0,
            observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
            raw_payload={},
        )

    async def fetch_account_config(self):
        return self.config

    async def fetch_balances(self):
        return (
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="USDT",
                wallet_balance=Decimal("100"),
                available_balance=Decimal("80"),
                unrealized_pnl=Decimal("0"),
                observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
                raw_payload={},
            ),
        )

    async def fetch_positions(self):
        return ()

    async def fetch_open_orders(self):
        return ()

    async def fetch_recent_fills(self, symbols=()):
        return ()

    async def aclose(self) -> None:
        return None


class FailingClient(FakeClient):
    async def fetch_balances(self):
        raise RuntimeError("temporary Binance failure")


class FakeRepository:
    def __init__(self) -> None:
        self.process_states = []
        self.balances = []
        self.positions = []
        self.open_orders = []
        self.fills = []
        self.configs = []
        self.reconciliation_runs = []
        self.snapshot_calls = 0

    async def save_process_state(self, state):
        self.process_states.append(state)

    async def save_balance_snapshot(self, snapshot):
        self.balances.append(snapshot)

    async def save_position_snapshot(self, snapshot):
        raise AssertionError("no positions expected")

    async def upsert_open_order(self, order):
        raise AssertionError("no open orders expected")

    async def save_fill_event(self, fill):
        raise AssertionError("no fills expected")

    async def save_config_snapshot(self, snapshot):
        self.configs.append(snapshot)

    async def save_reconciliation_run(self, run):
        self.reconciliation_runs.append(run)

    async def save_reconciliation_snapshot(
        self,
        *,
        config,
        balances,
        positions,
        open_orders,
        fills,
        run,
    ):
        self.snapshot_calls += 1
        self.configs.append(config)
        self.balances.extend(balances)
        self.positions.extend(positions)
        self.open_orders.extend(open_orders)
        self.fills.extend(fills)
        self.reconciliation_runs.append(run)


async def test_sync_once_persists_snapshot_and_ready_state() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )

    result = await service.sync_once()

    assert result.status is ExecutionAccountStatus.READY_READONLY
    assert repository.snapshot_calls == 1
    assert len(repository.balances) == 1
    assert repository.process_states[-1].state is ExecutionAccountStatus.READY_READONLY
    assert repository.reconciliation_runs[-1].status == "ready"


async def test_sync_once_halts_on_account_mode_mismatch() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(multi_assets_mode=True),
        repository=repository,
        config=_config(expected_multi_assets_mode=False),
    )

    result = await service.sync_once()

    assert result.status is ExecutionAccountStatus.HALTED_READONLY
    assert repository.snapshot_calls == 1
    assert repository.process_states[-1].state is ExecutionAccountStatus.HALTED_READONLY
    assert "multi_assets_mode_mismatch" in repository.process_states[-1].reason


async def test_sync_once_marks_degraded_when_fetch_fails() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FailingClient(),
        repository=repository,
        config=_config(),
    )

    try:
        await service.sync_once()
    except RuntimeError as error:
        assert str(error) == "temporary Binance failure"
    else:
        raise AssertionError("expected sync failure")

    assert repository.process_states[-1].state is ExecutionAccountStatus.DEGRADED


def _config(expected_multi_assets_mode: bool = False) -> ExecutionAccountSyncConfig:
    return ExecutionAccountSyncConfig(
        environment="live",
        account_label="primary",
        expected_multi_assets_mode=expected_multi_assets_mode,
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
