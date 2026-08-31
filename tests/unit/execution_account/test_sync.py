from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncService,
)
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
)


class FakeClient:
    def __init__(
        self,
        *,
        multi_assets_mode: bool = False,
        hedge_mode: bool = False,
    ) -> None:
        self.config = AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=multi_assets_mode,
            hedge_mode=hedge_mode,
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
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="BNB",
                wallet_balance=Decimal("0"),
                available_balance=Decimal("0"),
                unrealized_pnl=Decimal("0"),
                observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
                raw_payload={},
            ),
        )

    async def fetch_positions(self):
        return ()

    async def fetch_open_orders(self):
        return ()

    async def fetch_recent_fills(
        self,
        symbols=(),
        *,
        from_id_by_symbol=None,
        start_time_by_symbol=None,
    ):
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
    assert len(repository.balances) == 2
    assert repository.process_states[-1].state is ExecutionAccountStatus.READY_READONLY
    assert repository.reconciliation_runs[-1].status == "ready"


async def test_sync_tracks_incremental_fill_keys_and_baselines_new_symbols() -> None:
    first_fill = _fill("BTCUSDT", "42")
    second_fill = _fill("BTCUSDT", "43")
    new_symbol_fill = _fill("ETHUSDT", "99")
    client = CursorClient(
        responses=[(first_fill,), (second_fill, new_symbol_fill)],
    )
    service = ExecutionAccountSyncService(
        client=client,
        repository=FakeRepository(),
        config=_config(),
    )
    service._tracked_fill_symbols.add("BTCUSDT")

    first = await service.sync_once()
    service._tracked_fill_symbols.add("ETHUSDT")
    second = await service.sync_once()

    assert client.calls[0] == (
        ("BTCUSDT",),
        {},
        {},
    )
    assert client.calls[1] == (
        ("BTCUSDT", "ETHUSDT"),
        {"BTCUSDT": 43},
        {},
    )
    assert first.new_fill_keys == frozenset()
    assert second.new_fill_keys == frozenset({("BTCUSDT", "43")})
    assert second.fill_count_by_symbol == (
        ("BTCUSDT", 1),
        ("ETHUSDT", 1),
    )


async def test_user_data_event_persists_merged_snapshot() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )

    initial = await service.sync_once()
    assert initial.snapshot is not None
    state = AccountUserDataState(initial.snapshot)
    event = parse_user_data_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1783123201000,
            "a": {
                "B": [{"a": "USDT", "wb": "101", "cw": "81"}],
                "P": [],
            },
        },
        received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
    )
    update = state.apply(event)

    result = await service.persist_user_data_event(
        snapshot=update.snapshot,
        event=event,
        fills=update.fills,
    )

    assert result.status is ExecutionAccountStatus.READY_READONLY
    assert repository.snapshot_calls == 2
    assert [item.asset for item in repository.balances] == [
        "USDT",
        "BNB",
        "USDT",
    ]
    assert repository.reconciliation_runs[-1].details["source"] == (
        "user_data_stream"
    )
    assert repository.process_states[-1].state is ExecutionAccountStatus.READY_READONLY


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


async def test_sync_once_halts_on_hedge_mode_mismatch() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(hedge_mode=False),
        repository=repository,
        config=_config(expected_hedge_mode=True),
    )

    result = await service.sync_once()

    assert result.status is ExecutionAccountStatus.HALTED_READONLY
    assert result.mismatch_count == 1
    assert "hedge_mode_mismatch" in repository.process_states[-1].reason


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


def _config(
    expected_multi_assets_mode: bool = False,
    expected_hedge_mode: bool = False,
) -> ExecutionAccountSyncConfig:
    return ExecutionAccountSyncConfig(
        environment="live",
        account_label="primary",
        expected_multi_assets_mode=expected_multi_assets_mode,
        expected_hedge_mode=expected_hedge_mode,
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )


class CursorClient(FakeClient):
    def __init__(self, *, responses) -> None:
        super().__init__()
        self.responses = list(responses)
        self.calls = []

    async def fetch_recent_fills(
        self,
        symbols=(),
        *,
        from_id_by_symbol=None,
        start_time_by_symbol=None,
    ):
        self.calls.append(
            (
                tuple(symbols),
                dict(from_id_by_symbol or {}),
                dict(start_time_by_symbol or {}),
            )
        )
        return self.responses.pop(0)


def _fill(symbol: str, trade_id: str) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol=symbol,
        trade_id=trade_id,
        order_id=f"order-{trade_id}",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        raw_payload={},
    )
