import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.daemon import (
    UserDataAccountSyncConfig,
    UserDataAccountSyncDaemon,
)
from crypto_momentum_lab.execution_account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.sync_models import (
    ExecutionAccountSyncResult,
)
from crypto_momentum_lab.execution_account.user_data_sync import AccountUserDataState


class FakeStream:
    def __init__(self) -> None:
        self.handler = None
        self.stop_count = 0

    def set_handler(self, on_event) -> None:
        self.handler = on_event

    async def run(self) -> None:
        return None

    async def stop(self) -> None:
        self.stop_count += 1


class BlockingStream(FakeStream):
    async def run(self) -> None:
        await asyncio.Event().wait()


class FakeService:
    def __init__(self, snapshot: AccountSnapshot) -> None:
        self.snapshot = snapshot
        self.sync_calls = 0
        self.persisted = []
        self.heartbeats = []
        self.heartbeat_states = []
        self.sync_include_fills = []
        self.sync_started = asyncio.Event()

    async def sync_once(
        self,
        *,
        observed_at,
        publish_transient_states,
        include_fills,
    ):
        self.sync_calls += 1
        self.sync_include_fills.append(include_fills)
        self.sync_started.set()
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=f"reconciliation-{self.sync_calls}",
            mismatch_count=0,
            snapshot=self.snapshot,
        )

    async def persist_user_data_event(self, *, snapshot, event, fills=()):
        self.persisted.append((snapshot, event, fills))
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id="event-reconciliation",
            mismatch_count=0,
            snapshot=snapshot,
        )

    async def publish_user_data_heartbeat(self, *, observed_at, state=None):
        self.heartbeats.append(observed_at)
        self.heartbeat_states.append(state)


class BlockingReconciliationService(FakeService):
    def __init__(self, snapshot: AccountSnapshot) -> None:
        super().__init__(snapshot)
        self.reconciliation_started = asyncio.Event()
        self.release_reconciliation = asyncio.Event()

    async def sync_once_for_realtime(
        self,
        *,
        observed_at,
        publish_transient_states,
        include_fills,
    ):
        del observed_at, publish_transient_states, include_fills
        self.reconciliation_started.set()
        await self.release_reconciliation.wait()
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id="blocked-reconciliation",
            mismatch_count=0,
            snapshot=self.snapshot,
        )


async def test_publish_heartbeat_propagates_syncing_state_when_fills_catching_up() -> (
    None
):
    service = FakeService(_snapshot())
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
    )
    daemon._state = _snapshot()
    daemon._accept_events = True

    # 1. Default without sync result -> publishes READY_READONLY
    daemon._last_sync_result = None
    await daemon._publish_heartbeat()
    assert service.heartbeat_states[-1] == ExecutionAccountStatus.READY_READONLY

    # 2. Sync result with fills_catching_up=True -> publishes SYNCING
    daemon._last_sync_result = SimpleNamespace(
        fills_catching_up=True,
        status=ExecutionAccountStatus.READY_READONLY,
    )
    await daemon._publish_heartbeat()
    assert service.heartbeat_states[-1] == ExecutionAccountStatus.SYNCING

    # 3. Sync result with status == SYNCING -> publishes SYNCING
    daemon._last_sync_result = SimpleNamespace(
        fills_catching_up=False,
        status=ExecutionAccountStatus.SYNCING,
    )
    await daemon._publish_heartbeat()
    assert service.heartbeat_states[-1] == ExecutionAccountStatus.SYNCING

    # 4. Sync result resolved -> publishes READY_READONLY
    daemon._last_sync_result = SimpleNamespace(
        fills_catching_up=False,
        status=ExecutionAccountStatus.READY_READONLY,
    )
    await daemon._publish_heartbeat()
    assert service.heartbeat_states[-1] == ExecutionAccountStatus.READY_READONLY

    # An authoritative REST reconciliation may take much longer than a
    # heartbeat interval. Continue publishing SYNCING while that work runs.
    daemon._accept_events = False
    daemon._reconciliation_active = True
    await daemon._publish_heartbeat()
    assert service.heartbeat_states[-1] == ExecutionAccountStatus.SYNCING


async def test_run_keeps_heartbeat_alive_during_slow_rest_reconciliation() -> None:
    service = BlockingReconciliationService(_snapshot())
    stream = BlockingStream()
    stop_requested = asyncio.Event()
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(
            rest_reconciliation_interval_seconds=0.08,
            heartbeat_interval_seconds=0.01,
            snapshot_interval_seconds=1.0,
        ),
    )
    task = asyncio.create_task(daemon.run(stop_requested=stop_requested))
    try:
        await asyncio.wait_for(service.reconciliation_started.wait(), timeout=1)
        previous_heartbeat_count = len(service.heartbeats)
        await asyncio.sleep(0.04)

        assert len(service.heartbeats) > previous_heartbeat_count
        assert ExecutionAccountStatus.SYNCING in service.heartbeat_states
    finally:
        service.release_reconciliation.set()
        stop_requested.set()
        await asyncio.wait_for(task, timeout=1)


async def test_lightweight_snapshot_keeps_realtime_events_and_readiness_live() -> None:
    class BlockingSnapshotService(FakeService):
        def __init__(self, snapshot):
            super().__init__(snapshot)
            self.snapshot_started = asyncio.Event()
            self.release_snapshot = asyncio.Event()

        async def snapshot_once(self, *, observed_at):
            self.snapshot_started.set()
            await self.release_snapshot.wait()

    service = BlockingSnapshotService(_snapshot())
    applied = asyncio.Event()
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=lambda event, result: applied.set(),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    task = asyncio.create_task(daemon._snapshot())
    try:
        await asyncio.wait_for(service.snapshot_started.wait(), timeout=1)
        await daemon._publish_heartbeat()
        assert service.heartbeat_states[-1] is ExecutionAccountStatus.READY_READONLY
        event = parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123201000,
                "a": {"B": [{"a": "USDT", "wb": "101", "cw": "81"}], "P": []},
            },
            received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
        )
        await daemon._on_event(event)
        await asyncio.wait_for(applied.wait(), timeout=1)
        assert not daemon._deferred_events
        # The WS observation must reach durable storage before REST returns.
        await asyncio.wait_for(daemon._persistence_queue.join(), timeout=0.1)
        assert service.persisted[0][0].balances[0].wallet_balance == Decimal("101")
        assert not task.done()
        service.release_snapshot.set()
        await asyncio.wait_for(task, timeout=1)
        await asyncio.wait_for(daemon._persistence_queue.join(), timeout=1)
        assert service.persisted[0][0].balances[0].wallet_balance == Decimal("101")
    finally:
        service.release_snapshot.set()
        await asyncio.gather(task, return_exceptions=True)
        await daemon._stop_pipeline()


async def test_publish_heartbeat_internal_typeerror_not_caught() -> None:
    class FailingService(FakeService):
        def __init__(self, snapshot: AccountSnapshot) -> None:
            super().__init__(snapshot)
            self.call_count = 0

        async def publish_user_data_heartbeat(self, *, observed_at, state=None):
            self.call_count += 1
            raise TypeError("internal implementation error")

    service = FailingService(_snapshot())
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
    )
    daemon._state = _snapshot()
    daemon._accept_events = True

    import pytest

    with pytest.raises(TypeError, match="internal implementation error"):
        await daemon._publish_heartbeat()

    # Must only be called once, not retried as a signature mismatch
    assert service.call_count == 1


async def test_publish_heartbeat_backward_compatible_with_old_signature() -> None:
    calls = []

    class OldSignatureService(FakeService):
        async def publish_user_data_heartbeat(self, *, observed_at):
            calls.append(observed_at)

    service = OldSignatureService(_snapshot())
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
    )
    daemon._state = _snapshot()
    daemon._accept_events = True

    await daemon._publish_heartbeat()
    assert len(calls) == 1


async def test_run_does_not_block_startup_on_historical_fill_reconciliation() -> None:
    service = FakeService(_snapshot())
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
    )
    task = asyncio.create_task(daemon.run())
    try:
        await asyncio.wait_for(service.sync_started.wait(), timeout=1)
        assert service.sync_include_fills == [False]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_run_stops_when_stop_is_requested() -> None:
    service = FakeService(_snapshot())
    stream = BlockingStream()
    stop_requested = asyncio.Event()
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(),
    )
    task = asyncio.create_task(daemon.run(stop_requested=stop_requested))
    try:
        await asyncio.wait_for(service.sync_started.wait(), timeout=1)
        stop_requested.set()
        await asyncio.wait_for(task, timeout=1)
        assert stream.stop_count == 1
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


class RealtimeFakeService(FakeService):
    def __init__(self, snapshot: AccountSnapshot) -> None:
        super().__init__(snapshot)
        self.realtime_calls = 0
        self.realtime_persisted = []
        self.realtime_persisted_event = asyncio.Event()
        self.fill = AccountFillEvent(
            environment="live",
            account_label="primary",
            symbol="ETHUSDT",
            trade_id="trade-1",
            order_id="order-1",
            side="BUY",
            price=Decimal("3000"),
            quantity=Decimal("1"),
            realized_pnl=Decimal("0"),
            fee=Decimal("0.01"),
            fee_asset="USDT",
            trade_at=datetime(2026, 7, 4, 0, 0, 3, tzinfo=UTC),
            raw_payload={},
        )

    async def sync_once_for_realtime(
        self,
        *,
        observed_at,
        publish_transient_states,
        include_fills,
    ):
        del observed_at, publish_transient_states, include_fills
        self.realtime_calls += 1
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=f"realtime-{self.realtime_calls}",
            mismatch_count=0,
            snapshot=self.snapshot,
            fill_count=1,
            fills=(self.fill,),
            new_fills=(self.fill,),
            new_fill_keys=frozenset({("ETHUSDT", "trade-1")}),
        )

    async def persist_reconciliation_result(self, result):
        self.realtime_persisted.append(result)
        self.realtime_persisted_event.set()


async def test_user_data_daemon_persists_events_and_reconciles_unknown_state() -> None:
    snapshot = _snapshot()
    service = FakeService(snapshot)
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(
            rest_reconciliation_interval_seconds=60,
            heartbeat_interval_seconds=30,
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, 2, tzinfo=UTC),
    )

    await daemon._reconcile(include_fills=True)
    await daemon._on_event(
        parse_user_data_event(
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
    )
    assert len(service.persisted) == 1
    assert service.persisted[0][0].balances[0].wallet_balance == Decimal("101")

    await daemon._on_event(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123202000,
                "a": {
                    "B": [],
                    "P": [
                        {
                            "s": "ETHUSDT",
                            "pa": "1",
                            "ep": "3000",
                            "up": "0",
                            "ps": "BOTH",
                        }
                    ],
                },
            },
            received_at=datetime(2026, 7, 4, 0, 0, 2, tzinfo=UTC),
        )
    )
    assert service.sync_calls == 2

    await daemon._publish_heartbeat()
    assert len(service.heartbeats) == 1


async def test_user_data_daemon_publishes_initial_full_snapshot() -> None:
    snapshot = _snapshot()
    service = FakeService(snapshot)
    published: list[ExecutionAccountSyncResult] = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
        on_snapshot=published.append,
    )

    result = await daemon._reconcile(include_fills=True)

    assert published == [result]
    assert published[0].snapshot == snapshot


async def test_user_data_daemon_uses_realtime_reconcile_and_replays_new_fills() -> None:
    service = RealtimeFakeService(_snapshot())
    reconciled_fills = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
        on_reconciled_fill=lambda fill, result: reconciled_fills.append((fill, result)),
    )

    await daemon._reconcile(include_fills=True)
    result = await daemon._reconcile(include_fills=True)

    assert service.sync_calls == 1
    assert service.realtime_calls == 1
    assert reconciled_fills == [(service.fill, result)]
    await asyncio.wait_for(service.realtime_persisted_event.wait(), timeout=1)
    assert service.realtime_persisted == [result]
    await daemon._stop_pipeline()


class BlockingPersistService(FakeService):
    def __init__(self, snapshot: AccountSnapshot) -> None:
        super().__init__(snapshot)
        self.persist_started = asyncio.Event()
        self.release_persist = asyncio.Event()

    async def persist_user_data_event(self, *, snapshot, event, fills=()):
        self.persist_started.set()
        await self.release_persist.wait()
        return await super().persist_user_data_event(
            snapshot=snapshot,
            event=event,
            fills=fills,
        )


class BlockingSyncService(FakeService):
    def __init__(self, snapshot: AccountSnapshot) -> None:
        super().__init__(snapshot)
        self.block_next_sync = False
        self.sync_entered = asyncio.Event()
        self.release_sync = asyncio.Event()

    async def sync_once(
        self,
        *,
        observed_at,
        publish_transient_states,
        include_fills,
    ):
        if self.block_next_sync:
            self.block_next_sync = False
            self.sync_entered.set()
            await self.release_sync.wait()
        return await super().sync_once(
            observed_at=observed_at,
            publish_transient_states=publish_transient_states,
            include_fills=include_fills,
        )


async def test_events_received_during_reconciliation_are_replayed() -> None:
    service = BlockingSyncService(_snapshot())
    applied = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=lambda event, result: applied.append((event, result)),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    service.block_next_sync = True
    service.sync_entered.clear()
    service.release_sync.clear()

    reconcile_task = asyncio.create_task(daemon._reconcile(include_fills=True))
    try:
        await asyncio.wait_for(service.sync_entered.wait(), timeout=1)
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

        await daemon._on_event(event)
        await asyncio.wait_for(daemon._event_queue.join(), timeout=1)
        assert len(daemon._deferred_events) == 1
        assert applied == []

        service.release_sync.set()
        await asyncio.wait_for(reconcile_task, timeout=1)
        assert daemon._event_queue is not None
        assert daemon._persistence_queue is not None
        await daemon._event_queue.join()
        await daemon._persistence_queue.join()

        assert len(applied) == 1
        assert applied[0][0] is event
        assert len(service.persisted) == 1
        assert service.persisted[0][1] is event
        assert service.persisted[0][0].balances[0].wallet_balance == Decimal("101")
        assert not daemon._deferred_events
    finally:
        service.release_sync.set()
        if not reconcile_task.done():
            reconcile_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reconcile_task
        await daemon._stop_pipeline()


class FailingPersistService(FakeService):
    def __init__(self, snapshot: AccountSnapshot) -> None:
        super().__init__(snapshot)
        self.fail_next_persist = True

    async def persist_user_data_event(self, *, snapshot, event, fills=()):
        if self.fail_next_persist:
            self.fail_next_persist = False
            raise RuntimeError("persistence unavailable")
        return await super().persist_user_data_event(
            snapshot=snapshot,
            event=event,
            fills=fills,
        )


async def test_daemon_notifies_live_consumers_before_slow_persistence() -> None:
    service = BlockingPersistService(_snapshot())
    applied = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=lambda event, result: applied.append((event, result)),
    )
    await daemon._reconcile(include_fills=True)

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
    task = asyncio.create_task(daemon._on_event(event))
    await service.persist_started.wait()

    assert len(applied) == 1
    assert applied[0][0] is event
    assert not task.done()

    service.release_persist.set()
    await task


async def test_daemon_returns_after_apply_while_persistence_runs_in_background() -> (
    None
):
    service = BlockingPersistService(_snapshot())
    applied = []
    applied_event = asyncio.Event()

    def on_event_applied(event, result) -> None:
        applied.append((event, result))
        applied_event.set()

    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=on_event_applied,
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
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
    try:
        task = asyncio.create_task(daemon._on_event(event))
        await task
        await asyncio.wait_for(applied_event.wait(), timeout=1)
        await service.persist_started.wait()

        assert task.done()
        assert len(applied) == 1
        assert applied[0][0] is event
        assert service.persisted == []

        service.release_persist.set()
        assert daemon._persistence_queue is not None
        await daemon._persistence_queue.join()
        assert len(service.persisted) == 1
    finally:
        service.release_persist.set()
        await daemon._stop_pipeline()


async def test_replayed_user_data_event_preserves_syncing_readiness() -> None:
    service = FakeService(_snapshot())
    published = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=lambda event, result: published.append(result),
    )
    daemon._state = AccountUserDataState(_snapshot())
    daemon._accept_events = True
    daemon._last_sync_result = ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.SYNCING,
        reconciliation_id="syncing-reconciliation",
        mismatch_count=0,
        snapshot=_snapshot(),
        fills_catching_up=True,
    )
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

    await daemon._process_event(event, replay=True)

    assert len(published) == 1
    assert published[0].status is ExecutionAccountStatus.SYNCING


async def test_persistence_failure_fails_closed_and_recovers_from_rest() -> None:
    service = FailingPersistService(_snapshot())
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
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
    try:
        await daemon._on_event(event)
        assert daemon._event_queue is not None
        assert daemon._persistence_queue is not None
        await daemon._event_queue.join()
        await daemon._persistence_queue.join()
        assert daemon._pipeline_recovery_event.is_set()
        assert daemon._accept_events is False

        result = await daemon._recover_pipeline()

        assert result.status is ExecutionAccountStatus.READY_READONLY
        assert service.sync_calls == 2
        assert daemon._accept_events is True
        assert not daemon._pipeline_recovery_event.is_set()
    finally:
        await daemon._stop_pipeline()


def _snapshot() -> AccountSnapshot:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
            fee_tier=0,
            observed_at=observed_at,
            raw_payload={},
        ),
        balances=(
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="USDT",
                wallet_balance=Decimal("100"),
                available_balance=Decimal("80"),
                unrealized_pnl=Decimal("0"),
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
        positions=(),
        open_orders=(),
    )


@pytest.mark.parametrize("concurrent_event", [False, True])
async def test_periodic_check_keeps_ws_live_without_replacing_or_refetching(
    concurrent_event,
):
    class AuditService(RealtimeFakeService):
        def __init__(self):
            super().__init__(_snapshot())
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.facts = []

        async def sync_once_for_realtime(self, **kwargs):
            from dataclasses import replace

            result = await super().sync_once_for_realtime(**kwargs)
            result = replace(
                result,
                snapshot=replace(
                    result.snapshot,
                    balances=(
                        replace(
                            result.snapshot.balances[0], available_balance=Decimal("70")
                        ),
                    ),
                ),
            )
            self.started.set()
            await self.release.wait()
            return result

        async def persist_reconciliation_facts(self, result):
            self.facts.append(result)

    service = AuditService()
    stream = BlockingStream()
    stream.continuity_token = 1
    published = []
    fills = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(),
        clock=lambda: datetime(2026, 7, 4, 0, 10, tzinfo=UTC),
        on_snapshot=published.append,
        on_reconciled_fill=lambda fill, result: fills.append(fill),
    )
    await daemon._reconcile(include_fills=True)
    published.clear()
    daemon._start_pipeline()
    state = daemon._state
    task = asyncio.create_task(daemon._check_periodic())
    try:
        await asyncio.wait_for(service.started.wait(), timeout=1)
        await daemon._publish_heartbeat()
        assert service.heartbeat_states[-1] is ExecutionAccountStatus.READY_READONLY
        assert not daemon._reconciliation_active
        if concurrent_event:
            await daemon._on_event(
                parse_user_data_event(
                    {
                        "e": "ACCOUNT_UPDATE",
                        "E": 1783123201000,
                        "a": {"B": [{"a": "USDT", "wb": "101"}], "P": []},
                    },
                    received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
                )
            )
            await asyncio.wait_for(daemon._event_queue.join(), timeout=1)
            await asyncio.wait_for(daemon._persistence_queue.join(), timeout=1)
            assert service.persisted[-1][0].balances[0].wallet_balance == Decimal("101")
        service.release.set()
        result = await asyncio.wait_for(task, timeout=1)
        assert daemon._state is state
        assert daemon._state.snapshot(daemon._now()).balances[
            0
        ].wallet_balance == Decimal("101" if concurrent_event else "100")
        assert daemon._accept_events
        assert not daemon._pipeline_recovery_event.is_set()
        assert not daemon._deferred_events
        assert len(published) == (0 if concurrent_event else 1)
        if published:
            assert published[0].snapshot is result.snapshot
        assert service.realtime_calls == 1
        assert service.facts == [result]
        assert fills == [service.fill]
        if not concurrent_event:
            await asyncio.wait_for(service.realtime_persisted_event.wait(), timeout=1)
            assert service.realtime_persisted == [result]
        else:
            assert service.realtime_persisted == []
    finally:
        service.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await daemon._stop_pipeline()


@pytest.mark.parametrize("frozen", [False, True])
async def test_raw_event_is_durable_before_application_even_during_repair(frozen):
    class JournalService(FakeService):
        def __init__(self):
            super().__init__(_snapshot())
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.receipts = []

        async def record_user_data_event(self, **receipt):
            self.started.set()
            await self.release.wait()
            self.receipts.append(receipt)
            return 1

    service = JournalService()
    applied = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=lambda *args: applied.append(args),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    daemon._reconciliation_active = frozen
    event = parse_user_data_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1783123201000,
            "a": {"B": [{"a": "USDT", "wb": "101", "cw": "81"}], "P": []},
        },
        received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
    )
    task = asyncio.create_task(daemon._on_event(event))
    try:
        await asyncio.wait_for(service.started.wait(), timeout=1)
        assert applied == []
        assert not daemon._deferred_events
        service.release.set()
        await asyncio.wait_for(task, timeout=1)
        await asyncio.wait_for(daemon._event_queue.join(), timeout=1)
        assert service.receipts[0]["event"] == event
        assert len(service.receipts[0]["receiver_session_id"]) == 32
        if frozen:
            assert list(daemon._deferred_events) == [event]
            assert applied == []
        else:
            await asyncio.wait_for(daemon._event_queue.join(), timeout=1)
            assert len(applied) == 1
    finally:
        service.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await daemon._stop_pipeline()


async def test_journal_failure_requests_recovery_without_applying_unrecorded_event():
    class FailingJournalService(FakeService):
        async def record_user_data_event(self, **receipt):
            raise RuntimeError("journal unavailable")

    service = FailingJournalService(_snapshot())
    applied = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
        on_event_applied=lambda *args: applied.append(args),
    )
    await daemon._reconcile(include_fills=True)
    event = parse_user_data_event(
        {"e": "ACCOUNT_CONFIG_UPDATE", "E": 1783123201000},
        received_at=datetime(2026, 7, 4, tzinfo=UTC),
    )
    with pytest.raises(RuntimeError, match="journal unavailable"):
        await daemon._on_event(event)
    assert daemon._pipeline_recovery_event.is_set()
    assert daemon._pipeline_recovery_origin_event == event
    assert not daemon._accept_events
    assert applied == []


async def test_pending_raw_receipt_cannot_be_included_in_a_background_cut():
    class PendingReceiptService(RealtimeFakeService):
        def __init__(self):
            super().__init__(_snapshot())
            self.journal_started = asyncio.Event()
            self.release_journal = asyncio.Event()
            self.fetch_started = asyncio.Event()
            self.release_fetch = asyncio.Event()
            self.cursor_reads = 0

        async def record_user_data_event(self, **receipt):
            self.journal_started.set()
            await self.release_journal.wait()
            return 17

        async def user_data_journal_cursor(self):
            self.cursor_reads += 1
            return 17

        async def sync_once_for_realtime(self, **kwargs):
            result = await super().sync_once_for_realtime(**kwargs)
            self.fetch_started.set()
            await self.release_fetch.wait()
            return result

        async def persist_reconciliation_facts(self, result):
            pass

    service = PendingReceiptService()
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=BlockingStream(),
        config=UserDataAccountSyncConfig(),
        clock=lambda: datetime(2026, 7, 4, 0, 0, 2, tzinfo=UTC),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    # Make the stream explicitly eligible so the in-flight receipt is the
    # only reason this scan must use the protected repair path.
    daemon._stream.continuity_token = 1
    event = parse_user_data_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1783123201000,
            "a": {"B": [{"a": "USDT", "wb": "101", "cw": "81"}], "P": []},
        },
        received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
    )
    receipt = asyncio.create_task(daemon._on_event(event))
    repair = None
    try:
        await asyncio.wait_for(service.journal_started.wait(), timeout=1)
        repair = asyncio.create_task(daemon._reconcile(include_fills=True))
        await asyncio.sleep(0)
        assert not service.fetch_started.is_set()
        await daemon._publish_heartbeat()
        assert service.heartbeat_states[-1] is ExecutionAccountStatus.SYNCING
        assert service.cursor_reads == 0
        service.release_journal.set()
        await asyncio.wait_for(receipt, timeout=1)
        await asyncio.wait_for(daemon._event_queue.join(), timeout=1)
        assert daemon._journal_receipts_in_flight == 0
        await asyncio.wait_for(service.fetch_started.wait(), timeout=1)
        service.release_fetch.set()
        result = await asyncio.wait_for(repair, timeout=1)
        assert result.baseline_checkpoint is None
        assert daemon._state.snapshot(event.received_at).balances[
            0
        ].wallet_balance == Decimal("101")
    finally:
        service.release_journal.set()
        service.release_fetch.set()
        await asyncio.gather(
            receipt, *([repair] if repair is not None else []), return_exceptions=True
        )
        await daemon._stop_pipeline()


@pytest.mark.parametrize(
    "condition", ["mismatch", "disconnect", "catchup", "failure", "receipt"]
)
async def test_periodic_check_only_requests_repair_for_known_account_problems(
    condition,
):
    from dataclasses import replace

    class AuditService(RealtimeFakeService):
        async def persist_reconciliation_facts(self, result):
            pass

        async def sync_once_for_realtime(self, **kwargs):
            if condition == "failure":
                raise RuntimeError("REST timeout")
            result = await super().sync_once_for_realtime(**kwargs)
            if condition == "disconnect":
                stream.continuity_token = None
            return replace(result, fills_catching_up=condition == "catchup")

    stream = BlockingStream()
    stream.continuity_token = 1
    service = AuditService(_snapshot())
    published = []
    fill_states = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(),
        on_snapshot=published.append,
        on_reconciled_fill=lambda fill, result: fill_states.append(result.status),
    )
    await daemon._reconcile(include_fills=True)
    original_state = daemon._state
    published.clear()
    if condition in ("mismatch", "receipt"):
        service.snapshot = replace(
            service.snapshot,
            balances=(
                replace(
                    service.snapshot.balances[0],
                    wallet_balance=Decimal("999"),
                ),
            ),
        )
    if condition == "receipt":
        daemon._journal_receipts_in_flight = 1
    if condition == "failure":
        with pytest.raises(RuntimeError, match="REST timeout"):
            await daemon._check_periodic()
    else:
        await daemon._check_periodic()
    assert daemon._state is original_state
    assert daemon._state.snapshot(daemon._now()).balances[0].wallet_balance == Decimal(
        "100"
    )
    assert not daemon._reconciliation_active
    assert published == []
    assert service.realtime_persisted == []
    should_repair = condition in ("mismatch", "disconnect", "catchup")
    assert daemon._pipeline_recovery_event.is_set() == should_repair
    assert daemon._accept_events == (not should_repair)
    # No second REST fetch, even if the account needs a separate recovery.
    assert service.realtime_calls == (0 if condition == "failure" else 1)
    if condition != "failure":
        assert fill_states == [
            ExecutionAccountStatus.SYNCING
            if should_repair
            else ExecutionAccountStatus.READY_READONLY
        ]
    await daemon._publish_heartbeat()
    assert service.heartbeat_states[-1] is (
        ExecutionAccountStatus.SYNCING
        if should_repair
        else ExecutionAccountStatus.READY_READONLY
    )


async def test_run_uses_background_check_on_the_periodic_timer():
    class AuditService(RealtimeFakeService):
        async def persist_reconciliation_facts(self, result):
            stop.set()

    stop = asyncio.Event()
    service = AuditService(_snapshot())
    stream = BlockingStream()
    stream.continuity_token = 1
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(
            rest_reconciliation_interval_seconds=0.02,
            heartbeat_interval_seconds=0.01,
            snapshot_interval_seconds=1.0,
        ),
    )
    await asyncio.wait_for(daemon.run(stop_requested=stop), timeout=1)
    assert service.realtime_calls == 1
    assert service.sync_calls == 1
    assert not daemon._pipeline_recovery_event.is_set()
    assert all(
        state is ExecutionAccountStatus.READY_READONLY
        for state in service.heartbeat_states
    )


async def test_periodic_fill_fact_write_failure_does_not_erase_fill_notification():
    class AuditService(RealtimeFakeService):
        async def persist_reconciliation_facts(self, result):
            raise RuntimeError("facts storage unavailable")

    service = AuditService(_snapshot())
    stream = BlockingStream()
    stream.continuity_token = 1
    fills = []
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(),
        on_reconciled_fill=lambda fill, result: fills.append(fill),
    )
    await daemon._reconcile(include_fills=True)
    with pytest.raises(RuntimeError, match="facts storage unavailable"):
        await daemon._check_periodic()
    assert fills == [service.fill]
    assert daemon._accept_events
    assert not daemon._pipeline_recovery_event.is_set()
    await daemon._stop_pipeline()


async def test_periodic_mismatch_runs_separate_recovery_in_the_main_loop():
    from dataclasses import replace

    class AuditService(RealtimeFakeService):
        async def persist_reconciliation_facts(self, result):
            pass

        async def sync_once_for_realtime(self, **kwargs):
            self.snapshot = replace(
                self.snapshot,
                balances=(
                    replace(
                        self.snapshot.balances[0],
                        wallet_balance=Decimal("999"),
                    ),
                ),
            )
            result = await super().sync_once_for_realtime(**kwargs)
            if self.realtime_calls == 2:
                stop.set()
            return result

    stop = asyncio.Event()
    service = AuditService(_snapshot())
    stream = BlockingStream()
    stream.continuity_token = 1
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=stream,
        config=UserDataAccountSyncConfig(
            rest_reconciliation_interval_seconds=0.02,
            heartbeat_interval_seconds=0.01,
            snapshot_interval_seconds=1.0,
        ),
    )
    await asyncio.wait_for(daemon.run(stop_requested=stop), timeout=1)
    assert service.sync_calls == 1
    assert service.realtime_calls == 2  # One check followed by one actual repair.
    assert daemon._state.snapshot(daemon._now()).balances[0].wallet_balance == Decimal(
        "999"
    )
    assert not daemon._pipeline_recovery_event.is_set()


async def test_periodic_audit_preserves_exact_position_cut_for_real_book():
    from dataclasses import replace
    from datetime import timedelta

    from tests.unit.execution_account.orders.test_fill_scan_ingestion import (
        NOW,
        coordinator,
        scan,
        snapshot,
    )
    baseline = replace(_snapshot(), positions=(snapshot(NOW - timedelta(seconds=30)),))
    rest_snapshot = replace(baseline,
        config=replace(baseline.config, observed_at=NOW - timedelta(seconds=1)),
        positions=(snapshot(),))
    proof_scan = scan()

    class Service(RealtimeFakeService):
        async def sync_once_for_realtime(self, **kwargs):
            return ExecutionAccountSyncResult(
                status=ExecutionAccountStatus.READY_READONLY,
                reconciliation_id="audit-cut", mismatch_count=0,
                snapshot=rest_snapshot, fill_load_scans=(proof_scan,),
            )

        async def persist_reconciliation_facts(self, result):
            pass

    stream = BlockingStream()
    stream.continuity_token = 1
    published = []
    daemon = UserDataAccountSyncDaemon(service=Service(baseline), stream=stream,
        config=UserDataAccountSyncConfig(), clock=lambda: NOW, on_snapshot=published.append)
    await daemon._reconcile(include_fills=True)
    published.clear()
    state = daemon._state
    try:
        await daemon._check_periodic()
        assert daemon._state is state
        assert len(published) == 1
        runtime, book = coordinator()
        event = published[0]
        await runtime.observe_account_snapshot(event.snapshot,
            stream_id="hub", stream_epoch="epoch", sequence=1,
            fill_load_scans=event.fill_load_scans)
        evidence = book.observe.await_args.args[0]
        assert evidence.coverage_evidence.checkpoint_event_cut == NOW
        assert event.snapshot.positions[0].observed_at == NOW
    finally:
        await daemon._stop_pipeline()


async def test_slow_journal_does_not_block_receive_and_captures_each_stream_token():
    class SlowJournal(FakeService):
        def __init__(self):
            super().__init__(_snapshot())
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.receipts = []

        async def record_user_data_event(self, **receipt):
            self.started.set()
            await self.release.wait()
            self.receipts.append(receipt)
            return len(self.receipts)

    service = SlowJournal()
    stream = BlockingStream()
    stream.continuity_token = 1
    daemon = UserDataAccountSyncDaemon(service=service, stream=stream,
                                      config=UserDataAccountSyncConfig())
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    events = [parse_user_data_event(
        {"e": "ACCOUNT_UPDATE", "E": 1783123201000 + i,
         "a": {"B": [{"a": "USDT", "wb": str(101 + i), "cw": "81"}], "P": []}},
        received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC)
    ) for i in range(2)]
    try:
        await asyncio.wait_for(daemon._on_event(events[0]), 1)
        await asyncio.wait_for(service.started.wait(), 1)
        stream.continuity_token = 2
        await asyncio.wait_for(daemon._on_event(events[1]), 1)
        assert daemon._journal_receipts_in_flight == 2
        assert service.receipts == []
        assert daemon._state.snapshot(events[0].received_at).balances[0].wallet_balance == Decimal("100")
        service.release.set()
        await asyncio.wait_for(daemon._event_queue.join(), 1)
        assert [r["event"] for r in service.receipts] == events
        assert [r["stream_token"] for r in service.receipts] == [1, 2]
        assert daemon._journal_receipts_in_flight == 0
        assert daemon._state.snapshot(events[1].received_at).balances[0].wallet_balance == Decimal("102")
    finally:
        service.release.set()
        await daemon._stop_pipeline()


async def test_queued_journal_failure_cannot_apply_unrecorded_event():
    class FailingJournal(FakeService):
        async def record_user_data_event(self, **receipt):
            raise RuntimeError("journal unavailable")

    service = FailingJournal(_snapshot())
    applied = []
    daemon = UserDataAccountSyncDaemon(
        service=service, stream=BlockingStream(), config=UserDataAccountSyncConfig(),
        on_event_applied=lambda *args: applied.append(args),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    event = parse_user_data_event({"e": "ACCOUNT_CONFIG_UPDATE", "E": 1783123201000},
                                 received_at=datetime(2026, 7, 4, tzinfo=UTC))
    try:
        await asyncio.wait_for(daemon._on_event(event), 1)
        await asyncio.wait_for(daemon._event_queue.join(), 1)
        assert daemon._pipeline_recovery_event.is_set()
        assert not daemon._accept_events
        assert not applied
        assert daemon._journal_receipts_in_flight == 0
    finally:
        await daemon._stop_pipeline()


async def test_receipt_queue_overflow_requests_repair_without_blocking_reader():
    class SlowJournal(FakeService):
        def __init__(self):
            super().__init__(_snapshot())
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def record_user_data_event(self, **receipt):
            self.started.set()
            await self.release.wait()
            return 1

    service = SlowJournal()
    daemon = UserDataAccountSyncDaemon(
        service=service, stream=BlockingStream(),
        config=UserDataAccountSyncConfig(event_queue_size=1),
    )
    await daemon._reconcile(include_fills=True)
    daemon._start_pipeline()
    event = parse_user_data_event({"e": "ACCOUNT_CONFIG_UPDATE", "E": 1783123201000},
                                 received_at=datetime(2026, 7, 4, tzinfo=UTC))
    try:
        await daemon._on_event(event)
        await asyncio.wait_for(service.started.wait(), 1)
        await daemon._on_event(event)
        await asyncio.wait_for(daemon._on_event(event), 1)
        assert daemon._pipeline_recovery_event.is_set()
        assert daemon._pipeline_recovery_reason == "event_queue_overflow"
        assert not daemon._accept_events
        assert daemon._journal_receipts_in_flight == 2
        service.release.set()
        await asyncio.wait_for(daemon._event_queue.join(), 1)
        assert daemon._journal_receipts_in_flight == 0
    finally:
        service.release.set()
        await daemon._stop_pipeline()
