from dataclasses import replace
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
    AccountSnapshot,
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncResult,
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

    incomplete_fill_symbols: frozenset[str] = frozenset()

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
        cursors=(),
    ):
        self.snapshot_calls += 1
        self.configs.append(config)
        self.balances.extend(balances)
        self.positions.extend(positions)
        self.open_orders.extend(open_orders)
        self.fills.extend(fills)
        self.reconciliation_runs.append(run)
        if cursors:
            self.fill_cursor_calls.append(tuple(cursors))

    async def save_reconciliation_fills_and_cursors(
        self,
        *,
        fills,
        cursors=(),
    ):
        self.fills.extend(fills)
        if cursors:
            self.fill_cursor_calls.append(tuple(cursors))

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
    # Only the non-zero USDT balance is durable; the all-zero BNB row is
    # kept in the in-memory snapshot but not written every cycle.
    assert [item.asset for item in repository.balances] == ["USDT"]
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
    assert [item.asset for item in repository.balances] == ["USDT"]
    assert repository.process_states[-1].state is ExecutionAccountStatus.READY_READONLY
    assert repository.reconciliation_runs[-1].details == {
        "source": "rest_reconciliation"
    }
    assert repository.reconciliation_runs[-1].balance_count == 1


async def test_persist_reconciliation_result_skips_repeated_zero_balances() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )

    first = await service.sync_once_for_realtime()
    await service.persist_reconciliation_result(first)
    assert [item.asset for item in repository.balances] == ["USDT"]

    second = await service.sync_once_for_realtime()
    await service.persist_reconciliation_result(second)
    # Unchanged USDT still refreshes the equity series; BNB stays zero and
    # is not re-inserted.
    assert [item.asset for item in repository.balances] == ["USDT", "USDT"]
    assert all(item.asset == "USDT" for item in repository.balances)


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

    # Uncursored symbols are bounded by the historical lookback instead of
    # falling through to an unbounded "latest 1000" pull.
    first_window = int(
        (datetime(2026, 7, 4, 0, 0, tzinfo=UTC) - timedelta(days=7)).timestamp()
        * 1000
    )
    second_window = int(
        (datetime(2026, 7, 4, 6, 0, tzinfo=UTC) - timedelta(days=7)).timestamp()
        * 1000
    )
    assert client.calls[0] == (
        ("BTCUSDT",),
        {},
        {"BTCUSDT": first_window},
    )
    assert client.calls[1] == (
        ("BTCUSDT", "ETHUSDT"),
        {"BTCUSDT": 43},
        {"ETHUSDT": second_window},
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
    # BNB stays all-zero across both writes and is never durable.
    assert [item.asset for item in repository.balances] == ["USDT", "USDT"]
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


async def test_sync_batches_due_historical_fill_reconciliation() -> None:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    old_checked_at = observed_at - timedelta(hours=2)
    cursors = {
        symbol: AccountFillReconciliationCursor(
            environment="live",
            account_label="primary",
            symbol=symbol,
            from_id=index,
            start_time_ms=None,
            last_checked_at=old_checked_at,
        )
        for index, symbol in enumerate(
            ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
            start=1,
        )
    }
    client = CursorClient(responses=[(), ()])
    service = ExecutionAccountSyncService(
        client=client,
        repository=FakeRepository(),
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=observed_at,
            recent_fill_cursors=cursors,
            historical_fill_reconciliation_interval_seconds=3600,
            historical_fill_reconciliation_batch_size=2,
        ),
    )

    await service.sync_once(observed_at=observed_at)
    await service.sync_once(observed_at=observed_at + timedelta(minutes=1))

    assert client.calls[0][0] == ("BTCUSDT", "ETHUSDT")
    assert client.calls[1][0] == ("SOLUSDT",)


async def test_sync_always_includes_active_symbols_with_historical_batching() -> None:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    old_checked_at = observed_at - timedelta(hours=2)
    cursors = {
        symbol: AccountFillReconciliationCursor(
            environment="live",
            account_label="primary",
            symbol=symbol,
            from_id=index,
            start_time_ms=None,
            last_checked_at=old_checked_at,
        )
        for index, symbol in enumerate(("BTCUSDT", "SOLUSDT"), start=1)
    }
    client = NewPositionClient(
        responses=[(), ()],
        positions=[(), (_position(),)],
    )
    service = ExecutionAccountSyncService(
        client=client,
        repository=FakeRepository(),
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=observed_at,
            recent_fill_cursors=cursors,
            historical_fill_reconciliation_interval_seconds=3600,
            historical_fill_reconciliation_batch_size=1,
        ),
    )

    await service.sync_once(observed_at=observed_at)
    await service.sync_once(observed_at=observed_at + timedelta(minutes=1))

    assert client.calls[1][0] == ("ETHUSDT", "SOLUSDT")


async def test_sync_bounds_uncursored_historical_symbols_with_start_time() -> None:
    """A tracked symbol with no cursor must never be pulled unbounded."""
    observed_at = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)
    client = CursorClient(responses=[()])
    service = ExecutionAccountSyncService(
        client=client,
        repository=FakeRepository(),
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=observed_at,
            recent_fill_symbols=("LOBUSDT",),
            historical_fill_reconciliation_interval_seconds=3600,
        ),
    )

    await service.sync_once()

    assert len(client.calls) == 1
    symbols, from_ids, start_times = client.calls[0]
    assert symbols == ("LOBUSDT",)
    assert from_ids == {}
    assert "LOBUSDT" in start_times
    expected_start = int((observed_at - timedelta(days=7)).timestamp() * 1000)
    assert start_times["LOBUSDT"] == expected_start


async def test_sync_keeps_from_id_cursors_out_of_start_time_window() -> None:
    observed_at = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)
    client = CursorClient(responses=[()])
    service = ExecutionAccountSyncService(
        client=client,
        repository=FakeRepository(),
        config=ExecutionAccountSyncConfig(
            environment="live",
            account_label="primary",
            expected_multi_assets_mode=False,
            expected_hedge_mode=False,
            observed_at=observed_at,
            recent_fill_cursors={
                "BTCUSDT": AccountFillReconciliationCursor(
                    environment="live",
                    account_label="primary",
                    symbol="BTCUSDT",
                    from_id=101,
                    start_time_ms=None,
                    last_checked_at=observed_at - timedelta(hours=2),
                ),
            },
            historical_fill_reconciliation_interval_seconds=3600,
        ),
    )

    await service.sync_once()

    symbols, from_ids, start_times = client.calls[0]
    assert symbols == ("BTCUSDT",)
    assert from_ids == {"BTCUSDT": 101}
    assert "BTCUSDT" not in start_times


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


async def test_persist_reconciliation_result_stale_snapshot_still_persists_fills_and_cursors() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )
    # 1. Establish latest observation time at T2
    t2 = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)
    service._remember_observation(t2)
    assert service._latest_observation_at == t2

    # 2. Receive a result observed at T1 < T2 with new fills and cursor updates
    t1 = datetime(2026, 7, 4, 11, 0, tzinfo=UTC)
    fill_item = _fill("BTCUSDT", "trade-101")
    cursor_item = AccountFillReconciliationCursor(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        from_id=101,
        start_time_ms=None,
        last_checked_at=t1,
    )
    stale_result = ExecutionAccountSyncResult(
        reconciliation_id="stale:test",
        status=ExecutionAccountStatus.READY_READONLY,
        mismatch_count=0,
        snapshot=AccountSnapshot(
            config=AccountConfigSnapshot(
                environment="live",
                account_label="primary",
                multi_assets_mode=False,
                hedge_mode=False,
                fee_tier=0,
                observed_at=t1,
                raw_payload={},
            ),
            balances=(),
            positions=(),
            open_orders=(),
        ),
        fills=(fill_item,),
        fill_cursor_updates=(cursor_item,),
    )

    await service.persist_reconciliation_result(stale_result)

    # Snapshot tables and process state must NOT be touched
    assert repository.snapshot_calls == 0
    assert repository.process_states == []
    assert repository.balances == []
    assert repository.positions == []

    # Fills and cursors MUST be persisted
    assert repository.fills == [fill_item]
    assert repository.fill_cursor_calls == [(cursor_item,)]

    # In-memory cursor must advance to the new progress
    assert service._fill_cursors["BTCUSDT"].from_id == 101
    assert service._fill_cursor_checked_at["BTCUSDT"] == t1

    # Observation time must not regress
    assert service._latest_observation_at == t2


async def test_persist_reconciliation_result_cursors_do_not_regress() -> None:
    repository = FakeRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )
    t_base = datetime(2026, 7, 4, 10, 0, tzinfo=UTC)
    service._remember_observation(t_base)

    # Initialize in-memory cursor at higher from_id = 200
    from crypto_momentum_lab.execution_account.sync import _FillCursor
    service._fill_cursors["BTCUSDT"] = _FillCursor(from_id=200, start_time_ms=None)
    service._fill_cursor_checked_at["BTCUSDT"] = t_base

    # Incoming result has an older cursor with from_id = 150
    stale_cursor = AccountFillReconciliationCursor(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        from_id=150,
        start_time_ms=None,
        last_checked_at=t_base - timedelta(minutes=10),
    )
    result = ExecutionAccountSyncResult(
        reconciliation_id="regress:test",
        status=ExecutionAccountStatus.READY_READONLY,
        mismatch_count=0,
        snapshot=AccountSnapshot(
            config=AccountConfigSnapshot(
                environment="live",
                account_label="primary",
                multi_assets_mode=False,
                hedge_mode=False,
                fee_tier=0,
                observed_at=t_base - timedelta(minutes=10),
                raw_payload={},
            ),
            balances=(),
            positions=(),
            open_orders=(),
        ),
        fills=(),
        fill_cursor_updates=(stale_cursor,),
    )

    await service.persist_reconciliation_result(result)

    # In-memory cursor must NOT regress from 200 to 150
    assert service._fill_cursors["BTCUSDT"].from_id == 200
    assert service._fill_cursor_checked_at["BTCUSDT"] == t_base


async def test_persist_reconciliation_result_db_failure_does_not_advance_cursor() -> None:
    class FailingRepository(FakeRepository):
        async def save_reconciliation_fills_and_cursors(self, *, fills, cursors=()):
            raise RuntimeError("Database connection dropped during cursor update")

        async def save_reconciliation_snapshot(self, **kwargs):
            raise RuntimeError("Database connection dropped during snapshot write")

    repository = FailingRepository()
    service = ExecutionAccountSyncService(
        client=FakeClient(),
        repository=repository,
        config=_config(),
    )
    t = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)
    service._remember_observation(t)

    from crypto_momentum_lab.execution_account.sync import _FillCursor
    service._fill_cursors["BTCUSDT"] = _FillCursor(from_id=50, start_time_ms=None)

    new_cursor = AccountFillReconciliationCursor(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        from_id=100,
        start_time_ms=None,
        last_checked_at=t - timedelta(minutes=5),
    )
    result = ExecutionAccountSyncResult(
        reconciliation_id="fail:test",
        status=ExecutionAccountStatus.READY_READONLY,
        mismatch_count=0,
        snapshot=AccountSnapshot(
            config=AccountConfigSnapshot(
                environment="live",
                account_label="primary",
                multi_assets_mode=False,
                hedge_mode=False,
                fee_tier=0,
                observed_at=t - timedelta(minutes=5),
                raw_payload={},
            ),
            balances=(),
            positions=(),
            open_orders=(),
        ),
        fills=(_fill("BTCUSDT", "1"),),
        fill_cursor_updates=(new_cursor,),
    )

    import pytest
    with pytest.raises(RuntimeError, match="Database connection dropped"):
        await service.persist_reconciliation_result(result)

    # In-memory cursor must NOT advance when database write fails
    assert service._fill_cursors["BTCUSDT"].from_id == 50
    assert service._fill_cursors["BTCUSDT"].start_time_ms is None


async def test_sync_once_handles_incomplete_fills_catching_up() -> None:
    repository = FakeRepository()

    class IncompleteFillClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.incomplete_fill_symbols = frozenset({"BTCUSDT"})

        async def fetch_recent_fills(
            self,
            symbols=(),
            *,
            from_id_by_symbol=None,
            start_time_by_symbol=None,
        ):
            return (_fill("BTCUSDT", "100"),)

    config = replace(
        _config(),
        recent_fill_symbols=("BTCUSDT",),
    )
    service = ExecutionAccountSyncService(
        client=IncompleteFillClient(),
        repository=repository,
        config=config,
    )

    result = await service.sync_once(include_fills=True)

    # When fill coverage is incomplete (catching up), status must NOT be READY_READONLY
    assert result.status is ExecutionAccountStatus.SYNCING
    assert result.fills_catching_up is True
    # But cursor must advance to continuation point so work is not lost
    assert len(result.fill_cursor_updates) == 1
    assert result.fill_cursor_updates[0].symbol == "BTCUSDT"
    assert result.fill_cursor_updates[0].from_id == 101

    # Persisted run must record catching_up and details
    assert repository.reconciliation_runs[-1].status == "catching_up"
    assert repository.reconciliation_runs[-1].details["fills_catching_up"] is True
    assert repository.reconciliation_runs[-1].details["incomplete_symbols"] == ["BTCUSDT"]

    # Persisted process state must be SYNCING, not READY_READONLY
    assert repository.process_states[-1].state is ExecutionAccountStatus.SYNCING
    assert repository.process_states[-1].reason == "fills_catching_up"

    # Heartbeat during incomplete sync must preserve SYNCING and NOT overwrite with READY_READONLY
    heartbeat_time = datetime(2026, 7, 4, 12, 1, tzinfo=UTC)
    await service.publish_user_data_heartbeat(observed_at=heartbeat_time)
    assert repository.process_states[-1].state is ExecutionAccountStatus.SYNCING
    assert repository.process_states[-1].reason == "fills_catching_up"



