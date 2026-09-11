import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import crypto_momentum_lab.execution_account.hub as account_hub_module
import crypto_momentum_lab.market_data.hub as market_hub_module
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.daemon import (
    UserDataAccountSyncConfig,
    UserDataAccountSyncDaemon,
)
from crypto_momentum_lab.execution_account.hub import (
    WebSocketAccountEventSource,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
)
from crypto_momentum_lab.market_data.hub import (
    MarketStateHub,
    MarketStateHubConfig,
    MarketStateHubReplayUnavailable,
    WebSocketMarketStateSource,
    encode_market_state_batch,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _machine,
    _plan,
    _snapshot,
)
from tests.unit.execution_account.test_hub import _event
from tests.unit.execution_account.test_user_data import _initial_snapshot
from tests.unit.execution_account.test_user_data_daemon import (
    BlockingSyncService,
    FakeStream,
)
from tests.unit.execution_account.test_user_data_daemon import (
    _snapshot as account_snapshot,
)
from tests.unit.live_rollout.test_daemon import _runtime_context
from tests.unit.live_rollout.test_submission import (
    RecordingPreparedRepository,
    _submission,
)
from tests.unit.market_data.test_hub import fixture_state
from tests.unit.shadow_operation.test_service import _intent, _state

pytestmark = pytest.mark.e2e


async def test_fault_injection_market_hub_gap_and_replay_window_fail_closed(
    monkeypatch,
) -> None:
    """A disconnect cannot turn a sequence hole into a fresh live state."""

    hub = MarketStateHub(MarketStateHubConfig(replay_batch_count=1))
    await hub.publish((fixture_state("BTCUSDT", 0),))
    await hub.publish((fixture_state("BTCUSDT", 1),))
    available, oldest, latest, messages = hub._replay_snapshot("research", 0)
    assert available is False
    assert oldest == 2
    assert latest == 2
    assert messages == ()

    first = fixture_state("BTCUSDT", 0)
    skipped = fixture_state("BTCUSDT", 2)
    sent_messages: list[str] = []

    def ready(*, replay_available: bool, oldest_sequence: int | None = None) -> str:
        return json.dumps(
            {
                "type": "market_state_hub_ready",
                "schema_version": 1,
                "environment": "research",
                "stream_id": "stream-a",
                "replay_available": replay_available,
                "oldest_sequence": oldest_sequence,
                "latest_sequence": 30 if not replay_available else 1,
            }
        )

    class FakeConnection:
        def __init__(self, messages: tuple[object, ...]) -> None:
            self._messages = list(messages)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, message: str) -> None:
            sent_messages.append(message)

        async def recv(self):
            message = self._messages.pop(0)
            if isinstance(message, BaseException):
                raise message
            return message

    connections = [
        FakeConnection(
            (
                ready(replay_available=True),
                encode_market_state_batch(
                    (first,),
                    sequence=1,
                    published_at=first.bucket_end,
                    stream_id="stream-a",
                ),
                encode_market_state_batch(
                    (skipped,),
                    sequence=3,
                    published_at=skipped.bucket_end,
                    stream_id="stream-a",
                ),
            )
        ),
        FakeConnection(
            (
                ready(replay_available=False, oldest_sequence=20),
            )
        ),
    ]
    monkeypatch.setattr(
        market_hub_module,
        "connect",
        lambda *_args, **_kwargs: connections.pop(0),
    )
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="fault-gate-market",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
        fail_on_replay_unavailable=True,
    )
    iterator = source.__aiter__()
    try:
        assert await anext(iterator) == first
        with pytest.raises(MarketStateHubReplayUnavailable) as error_info:
            await anext(iterator)
    finally:
        source.stop()
        await iterator.aclose()

    assert len(sent_messages) == 2
    assert json.loads(sent_messages[1])["last_sequence"] == 1
    assert error_info.value.oldest_sequence == 20
    assert error_info.value.latest_sequence == 30


class _InterruptibleSubmitExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__(
            submit_result=_snapshot(ExchangeOrderState.FILLED),
            query_result=_snapshot(ExchangeOrderState.FILLED),
        )
        self.submit_started = asyncio.Event()
        self.release_submit = asyncio.Event()

    async def submit_order(self, plan) -> ExchangeOrderSnapshot:
        self.calls.append("submit")
        self.submit_started.set()
        await self.release_submit.wait()
        result = self.submit_result
        if isinstance(result, Exception):
            raise result
        return result


async def test_fault_injection_submitting_sigterm_is_reconciled_after_restart() -> None:
    exchange = _InterruptibleSubmitExchange()
    repository = FakeOrderRepository()
    plan = _plan()
    submitting_task = asyncio.create_task(
        _machine(exchange, repository).execute_approved_intent(plan)
    )

    await asyncio.wait_for(exchange.submit_started.wait(), timeout=1)
    submitting_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await submitting_task

    assert [event.state for event in repository.events] == [
        ExchangeOrderState.SUBMITTING
    ]
    assert repository.plans == [plan]

    restarted_result = await _machine(exchange, repository).reconcile_order(plan)

    assert restarted_result.state is ExchangeOrderState.FILLED
    assert exchange.calls == ["submit", "query"]
    assert repository.events[-1].state is ExchangeOrderState.FILLED


class _HaltAwareCoordinator:
    def __init__(self) -> None:
        self.prepare_calls = 0
        self.exchange_calls = 0

    async def prepare_and_execute(self, plan, *, prepare_submission):
        self.prepare_calls += 1
        prepared = await prepare_submission()
        if prepared is None:
            return None
        self.exchange_calls += 1
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="exchange-1",
            plan=plan,
        )


async def test_fault_injection_operator_halt_wins_entry_post_race() -> None:
    repository = RecordingPreparedRepository()
    coordinator = _HaltAwareCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    entry_checks = 0

    def entry_enabled() -> bool:
        nonlocal entry_checks
        entry_checks += 1
        return entry_checks < 3

    submission._entry_enabled = entry_enabled
    submission._entry_enabled_reason = lambda: "operator_halt"

    result = await submission.execute(
        replace(_intent(), desired_notional=Decimal("20")),
        requested_quantity=None,
        state=_state(),
        context=_runtime_context(),
    )

    assert result is None
    assert entry_checks == 3
    assert coordinator.prepare_calls == 1
    assert coordinator.exchange_calls == 0
    assert repository.prepare_calls == []


async def test_fault_injection_account_overflow_defers_and_recovers() -> None:
    previous = account_snapshot()
    source = WebSocketAccountEventSource(
        url="ws://unused",
        environment="live",
        account_label="primary",
        consumer_id="fault-gate-account",
    )
    receive_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=1)
    source._enqueue_account_event(
        receive_queue,
        replace(_event(), sequence=1),
    )
    source._enqueue_account_event(
        receive_queue,
        replace(_event(), event_id="event-2", sequence=2),
    )
    overflow = receive_queue.get_nowait()
    assert isinstance(overflow, account_hub_module._AccountEventQueueOverflow)
    assert source.metrics.queue_overflow_count == 1

    source._prepare_full_snapshot_recovery("account_event_queue_overflow")
    full = replace(
        _event(),
        event_id="event-recovered",
        sequence=100,
        snapshot_kind="full",
        account_snapshot=previous,
    )
    materialized = source._materialize_event(full)
    assert materialized is not None
    assert materialized.account_snapshot == previous
    assert source.metrics.recovery_count == 1
    assert source.metrics.last_recovery_reason == "account_event_queue_overflow"

    service = BlockingSyncService(account_snapshot())
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
        assert not daemon._deferred_events
    finally:
        service.release_sync.set()
        if not reconcile_task.done():
            reconcile_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reconcile_task
        await daemon._stop_pipeline()


def test_fault_injection_exchange_clock_rollback_requires_reconciliation() -> None:
    state = AccountUserDataState(_initial_snapshot())
    first = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1783123203000,
        "T": 1783123203000,
        "o": {
            "s": "BTCUSDT",
            "c": "entry-1",
            "S": "BUY",
            "o": "LIMIT",
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "i": 1001,
            "t": 5002,
            "q": "0.002",
            "p": "50000",
            "z": "0.001",
            "l": "0.001",
            "L": "50100",
            "rp": "0.1",
            "n": "0.01",
            "N": "USDT",
            "R": False,
        },
    }
    state.apply(
        parse_user_data_event(
            first,
            received_at=datetime(2026, 7, 4, 0, 0, 3, tzinfo=UTC),
        )
    )

    rollback = state.apply(
        parse_user_data_event(
            {
                **first,
                "E": 1783123202000,
                "T": 1783123202000,
                "o": {**first["o"], "x": "CANCELED", "X": "CANCELED"},
            },
            received_at=datetime(2026, 7, 4, 0, 0, 4, tzinfo=UTC),
        )
    )

    assert rollback.needs_reconciliation is True
    assert rollback.reason == "stale_exchange_event"
    assert rollback.snapshot.open_orders[0].status == "PARTIALLY_FILLED"
