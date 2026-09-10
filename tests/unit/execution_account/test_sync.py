from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
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
            fee_tier=0,
            observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
            raw_payload={"totalInitialMargin": "12.34"},
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
        self.fill_cursor_calls = []
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

    async def save_fill_reconciliation_cursors(self, cursors):
        self.fill_cursor_calls.append(tuple(cursors))


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
    assert repository.reconciliation_runs[-1].details == {
        "source": "rest_reconciliation"
    }


async def test_realtime_sync_publishes_before_durable_persistence() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )

    result = await service.sync_once_for_realtime()

    assert result.status is ExecutionAccountStatus.READY_READONLY
    assert result.snapshot is not None
    assert repository.snapshot_calls == 0
    assert repository.process_states == []

    await service.persist_reconciliation_result(
        result,
        source="rest_reconciliation",
    )

    assert repository.snapshot_calls == 1
    assert repository.process_states[-1].state is ExecutionAccountStatus.READY_READONLY
    assert repository.reconciliation_runs[-1].details == {
        "source": "rest_reconciliation"
    }


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
    second = await service.sync_once(
        observed_at=datetime(2026, 7, 4, 6, 0, tzinfo=UTC)
    )

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


async def test_sync_replays_recent_fills_when_a_new_position_appears() -> None:
    fill = _fill("ETHUSDT", "99")
    client = NewPositionClient(responses=[(fill,)], positions=[(), (_position(),)])
    service = ExecutionAccountSyncService(
        client=client,
        repository=FakeRepository(),
        config=_config(),
    )

    await service.sync_once()
    result = await service.sync_once()

    assert result.new_fill_keys == frozenset({("ETHUSDT", "99")})
    assert result.new_fills == (fill,)
    assert client.calls == [
        (
            ("ETHUSDT",),
            {},
            {"ETHUSDT": 1783121400000},
        )
    ]


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
    assert repository.configs[-1].observed_at == initial.snapshot.config.observed_at
    assert repository.configs[-1].observed_at != event.received_at
    assert repository.configs[-1].raw_payload == {
        "totalInitialMargin": "12.34"
    }
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


async def test_sync_restores_cursors_and_defers_fresh_historical_symbols() -> None:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    client = CursorClient(responses=[()])
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=client,
        repository=repository,
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=observed_at,
            recent_fill_symbols=("BTCUSDT", "ETHUSDT"),
            recent_fill_cursors={
                "BTCUSDT": AccountFillReconciliationCursor(
                    environment="live",
                    account_label="primary",
                    symbol="BTCUSDT",
                    from_id=44,
                    start_time_ms=None,
                    last_checked_at=observed_at - timedelta(hours=2),
                ),
                "ETHUSDT": AccountFillReconciliationCursor(
                    environment="live",
                    account_label="primary",
                    symbol="ETHUSDT",
                    from_id=88,
                    start_time_ms=None,
                    last_checked_at=observed_at - timedelta(minutes=5),
                ),
            },
            historical_fill_reconciliation_interval_seconds=3600,
        ),
    )

    result = await service.sync_once()

    assert client.calls == [
        (("BTCUSDT",), {"BTCUSDT": 44}, {}),
    ]
    assert result.fill_cursor_updates == (
        AccountFillReconciliationCursor(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            from_id=44,
            start_time_ms=None,
            last_checked_at=observed_at,
        ),
    )
    assert repository.fill_cursor_calls == [result.fill_cursor_updates]


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


class NewPositionClient(CursorClient):
    def __init__(self, *, responses, positions) -> None:
        super().__init__(responses=responses)
        self.positions = list(positions)

    async def fetch_positions(self):
        return self.positions.pop(0)


def _position() -> AccountPositionSnapshot:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="ETHUSDT",
        position_side="BOTH",
        position_amt=Decimal("1"),
        entry_price=Decimal("3000"),
        mark_price=Decimal("3000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("3000"),
        leverage=1,
        margin_type="CROSSED",
        observed_at=observed_at,
        raw_payload={},
    )


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
