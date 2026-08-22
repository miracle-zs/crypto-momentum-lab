from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.telemetry import (
    MARKET_STATE_RECEIVED,
    LiveRuntimeTelemetry,
)
from tests.unit.shadow_operation.test_service import _intent, _state


async def test_live_telemetry_rolls_up_phase_latency_by_symbol_and_lane() -> None:
    telemetry = LiveRuntimeTelemetry(run_id="run-1")
    state = _state()
    start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    candidate = _intent()

    await telemetry.market_state_received(state, occurred_at=start)
    await telemetry.strategy_decision(
        state,
        occurred_at=start + timedelta(seconds=1),
        signal_count=1,
        candidate_count=1,
    )
    await telemetry.candidate_accepted(
        candidate,
        state=state,
        occurred_at=start + timedelta(seconds=2),
        lane="entry",
    )
    await telemetry.risk_approved(
        candidate,
        state=state,
        occurred_at=start + timedelta(seconds=3),
        lane="entry",
        evaluation_id="evaluation-1",
    )
    await telemetry.intent_saved(
        candidate,
        state=state,
        occurred_at=start + timedelta(seconds=4),
        lane="entry",
    )

    plan = OrderExecutionPlan(
        intent_id=candidate.candidate_id,
        run_id=candidate.run_id,
        client_order_id="cml_12345678901234567890123456789012",
        symbol=state.symbol,
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.003"),
        price=None,
        reduce_only=False,
        created_at=start + timedelta(seconds=4),
        quantized=True,
    )
    await telemetry.order_event(
        plan,
        ExchangeOrderEvent(
            event_id="submitting-1",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            occurred_at=start + timedelta(seconds=5),
            exchange_order_id=None,
            details={},
        ),
    )
    await telemetry.exchange_request_started(
        plan,
        "submit_request_started",
        start + timedelta(seconds=5, milliseconds=100),
    )
    await telemetry.exchange_response_received(
        plan,
        "submit_response_received",
        start + timedelta(seconds=6),
    )
    await telemetry.order_event(
        plan,
        ExchangeOrderEvent(
            event_id="filled-1",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=start + timedelta(seconds=7),
            exchange_order_id="exchange-1",
            details={"executed_quantity": "0.003"},
        ),
    )
    await telemetry.account_fill(
        AccountEvent(
            environment="live",
            account_label="primary",
            event_type="ORDER_TRADE_UPDATE",
            event_id="account-fill-1",
            event_at=start + timedelta(seconds=7),
            received_at=start + timedelta(seconds=8),
            symbols=(state.symbol,),
            symbol=state.symbol,
            client_order_id=plan.client_order_id,
            order_status="FILLED",
            has_fill=True,
            trade_id="trade-1",
        ),
        occurred_at=start + timedelta(seconds=8),
    )

    summary = telemetry.latency_summary()["BTCUSDT"]["entry"]

    assert summary["intent_saved->submitting"]["p50_ms"] == 1000
    assert summary["submitting->exchange_request_started"]["p50_ms"] == 100
    assert summary["exchange_request_started->exchange_response_received"][
        "p95_ms"
    ] == 900
    assert summary["exchange_response_received->exchange_filled"][
        "p95_ms"
    ] == 1000
    assert summary["exchange_filled->account_fill"]["max_ms"] == 1000


async def test_live_telemetry_persists_events_in_batches_without_blocking_records(
) -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        persist=persist,
        persist_event_types=frozenset({MARKET_STATE_RECEIVED}),
    )
    await telemetry.start()
    await telemetry.market_state_received(
        _state(),
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
    await telemetry.stop()

    assert len(batches) == 1
    assert batches[0][0]["event_type"] == "market_state_received"
    assert batches[0][0]["details"]["lane"] == "entry"


async def test_high_frequency_telemetry_stays_in_memory_when_not_persisted() -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        persist=persist,
        persist_event_types=frozenset(),
    )
    await telemetry.start()
    state = _state()
    await telemetry.market_state_received(
        state,
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
    await telemetry.strategy_decision(
        state,
        occurred_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
        signal_count=0,
        candidate_count=0,
    )
    await telemetry.stop()

    assert batches == []
    assert telemetry.recorded_event_count == 2
    assert telemetry.persist_failure_count == 0


def test_live_database_plane_urls_prefer_explicit_plane_environment(
    monkeypatch,
) -> None:
    from crypto_momentum_lab.apps.live_rollout import main

    monkeypatch.setenv("CML_DATABASE_URL", "postgresql+asyncpg://data")
    monkeypatch.setenv("CML_EXECUTION_DATABASE_URL", "postgresql+asyncpg://exec")
    monkeypatch.setenv(
        "CML_OBSERVABILITY_DATABASE_URL",
        "postgresql+asyncpg://observability",
    )

    assert main._execution_database_url(None) == "postgresql+asyncpg://exec"
    assert main._observability_database_url(None) == (
        "postgresql+asyncpg://observability"
    )
    assert main._market_database_url(None) == "postgresql+asyncpg://data"
