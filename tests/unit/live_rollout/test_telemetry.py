from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.telemetry import (
    CONSUMER_HEALTH,
    EXCHANGE_REQUEST_STARTED,
    EXCHANGE_RESPONSE_RECEIVED,
    MARKET_STATE_PROGRESS,
    MARKET_STATE_RECEIVED,
    STRATEGY_OUTPUT_OBSERVED,
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


async def test_consumer_health_persists_low_cardinality_operational_event() -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        persist=persist,
        persist_event_types=frozenset({CONSUMER_HEALTH}),
    )
    occurred_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    await telemetry.start()
    telemetry.consumer_health(
        consumer="market_state_hub",
        available=False,
        occurred_at=occurred_at,
        reason="market_state_consumer_lagged",
        lag=True,
    )
    await telemetry.stop()

    event = batches[0][0]
    assert event["event_type"] == CONSUMER_HEALTH
    assert event["symbol"] is None
    assert event["bucket_start"] is None
    assert event["details"] == {
        "consumer": "market_state_hub",
        "available": False,
        "recovery": False,
        "lag": True,
        "reason": "market_state_consumer_lagged",
        "sequence": None,
    }


async def test_market_progress_persists_sampled_delay_and_account_identity() -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        account_label="account-2",
        strategy_config_hash="config-1",
        persist=persist,
        persist_event_types=frozenset({MARKET_STATE_PROGRESS}),
    )
    state = _state()
    await telemetry.start()
    telemetry.market_state_progress(
        state,
        occurred_at=datetime(2026, 7, 4, 0, 0, 45, tzinfo=UTC),
        received_at=datetime(2026, 7, 4, 0, 0, 45, tzinfo=UTC),
    )
    telemetry.market_state_progress(
        state,
        occurred_at=datetime(2026, 7, 4, 0, 1, 15, tzinfo=UTC),
        received_at=datetime(2026, 7, 4, 0, 1, 16, tzinfo=UTC),
    )
    await telemetry.stop()

    assert len(batches) == 1
    event = batches[0][0]
    assert event["event_type"] == MARKET_STATE_PROGRESS
    assert event["symbol"] == state.symbol
    assert event["bucket_start"] == state.bucket_start
    assert event["details"]["account_label"] == "account-2"
    assert event["details"]["strategy_config_hash"] == "config-1"
    assert event["details"]["market_delay_ms"] == 30_000.0


async def test_strategy_output_observation_is_durable_as_a_sampled_heartbeat() -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        account_label="primary",
        strategy_config_hash="config-1",
        persist=persist,
        persist_event_types=frozenset({STRATEGY_OUTPUT_OBSERVED}),
    )
    state = _state()
    await telemetry.start()
    await telemetry.strategy_decision(
        state,
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        signal_count=0,
        candidate_count=0,
    )
    await telemetry.stop()

    event = batches[0][0]
    assert event["event_type"] == STRATEGY_OUTPUT_OBSERVED
    assert event["details"] == {
        "account_label": "primary",
        "strategy_config_hash": "config-1",
        "signal_count": 0,
        "candidate_count": 0,
    }


async def test_strategy_output_heartbeat_is_sampled_per_symbol() -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        account_label="primary",
        strategy_config_hash="config-1",
        persist=persist,
        persist_event_types=frozenset({STRATEGY_OUTPUT_OBSERVED}),
    )
    timestamp = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    await telemetry.start()
    await telemetry.strategy_decision(
        _state(),
        occurred_at=timestamp,
        signal_count=0,
        candidate_count=0,
    )
    await telemetry.strategy_decision(
        replace(_state(), symbol="ETHUSDT"),
        occurred_at=timestamp,
        signal_count=1,
        candidate_count=1,
    )
    await telemetry.stop()

    events = [event for batch in batches for event in batch]
    assert {event["symbol"] for event in events} == {"BTCUSDT", "ETHUSDT"}


async def test_persisted_order_events_carry_decision_slo_transition_samples() -> None:
    state = _state()
    candidate = _intent()
    telemetry = LiveRuntimeTelemetry(run_id="run-1")
    start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)

    await telemetry.market_state_received(state, occurred_at=start)
    await telemetry.context_ready(
        state,
        occurred_at=start + timedelta(milliseconds=100),
        prefetched=True,
        reloaded=False,
    )
    await telemetry.candidate_accepted(
        candidate,
        state=state,
        occurred_at=start + timedelta(milliseconds=300),
        lane="entry",
    )
    await telemetry.intent_saved(
        candidate,
        state=state,
        occurred_at=start + timedelta(milliseconds=500),
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
        created_at=start + timedelta(milliseconds=500),
        quantized=True,
    )
    await telemetry.exchange_request_started(
        plan,
        "submit_request_started",
        start + timedelta(milliseconds=700),
    )

    events = {event.event_type: event for event in telemetry.recent_events}

    assert events["candidate_accepted"].details["decision_slo_latency_ms"] == {
        "market_state_received->context_ready": 100.0,
        "context_ready->candidate_accepted": 200.0,
    }
    assert events["intent_saved"].details["decision_slo_latency_ms"] == {
        "candidate_accepted->intent_saved": 200.0,
    }
    assert events[EXCHANGE_REQUEST_STARTED].details[
        "decision_slo_latency_ms"
    ] == {
        "intent_saved->exchange_request_started": 200.0,
    }


async def test_exchange_latency_pairs_each_operation_attempt() -> None:
    telemetry = LiveRuntimeTelemetry(run_id="run-1")
    plan = OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.003"),
        price=None,
        reduce_only=False,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        quantized=True,
    )
    start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)

    await telemetry.exchange_request_started(
        plan,
        "submit_request_started",
        start,
    )
    await telemetry.exchange_response_received(
        plan,
        "submit_response_received",
        start + timedelta(milliseconds=20),
    )
    cancel_started = start + timedelta(minutes=10)
    await telemetry.exchange_request_started(
        plan,
        "cancel_request_started",
        cancel_started,
    )
    await telemetry.exchange_response_received(
        plan,
        "cancel_response_received",
        cancel_started + timedelta(milliseconds=30),
    )

    cancel_response = next(
        event
        for event in telemetry.recent_events
        if event.event_type == EXCHANGE_RESPONSE_RECEIVED
        and event.details["operation"] == "cancel"
    )
    assert cancel_response.details["request_attempt"] == 1
    assert cancel_response.details["request_paired"] is True
    assert cancel_response.details["request_started_at"] == cancel_started.isoformat()
    assert cancel_response.details["latency_ms_from_request"] == 30.0
    assert "latency_ms_from_previous" not in cancel_response.details
    assert (
        telemetry.latency_summary()["BTCUSDT"]["entry"][
            "cancel_request_started->cancel_response_received"
        ]["p50_ms"]
        == 30.0
    )


async def test_exchange_persistence_allowlist_keeps_submit_and_cancel_audit(
) -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        persist=persist,
        persist_event_types=frozenset(
            {EXCHANGE_REQUEST_STARTED, EXCHANGE_RESPONSE_RECEIVED}
        ),
        persist_exchange_operations=frozenset({"submit", "cancel"}),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.003"),
        price=None,
        reduce_only=False,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        quantized=True,
    )
    await telemetry.start()
    for index, operation in enumerate(("query", "submit", "cancel")):
        request_at = datetime(2026, 7, 4, 0, 0, index, tzinfo=UTC)
        await telemetry.exchange_request_started(
            plan,
            f"{operation}_request_started",
            request_at,
        )
        await telemetry.exchange_response_received(
            plan,
            f"{operation}_response_received",
            request_at + timedelta(milliseconds=10),
        )
    await telemetry.stop()

    persisted_operations = [
        event["details"]["operation"]
        for batch in batches
        for event in batch
    ]
    assert persisted_operations == ["submit", "submit", "cancel", "cancel"]
    assert telemetry.recorded_event_count == 6


async def test_exchange_persistence_defaults_to_all_operations() -> None:
    batches: list[tuple[dict[str, object], ...]] = []

    async def persist(events) -> None:
        batches.append(tuple(dict(event) for event in events))

    telemetry = LiveRuntimeTelemetry(
        run_id="run-1",
        persist=persist,
        persist_event_types=frozenset(
            {EXCHANGE_REQUEST_STARTED, EXCHANGE_RESPONSE_RECEIVED}
        ),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.003"),
        price=None,
        reduce_only=False,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        quantized=True,
    )
    await telemetry.start()
    await telemetry.exchange_request_started(
        plan,
        "query_request_started",
        datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
    await telemetry.exchange_response_received(
        plan,
        "query_response_received",
        datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
    )
    await telemetry.stop()

    persisted_operations = [
        event["details"]["operation"]
        for batch in batches
        for event in batch
    ]
    assert persisted_operations == ["query", "query"]


def test_exchange_persistence_allowlist_rejects_blank_operation_names() -> None:
    with pytest.raises(ValueError, match="non-empty names"):
        LiveRuntimeTelemetry(
            run_id="run-1",
            persist_exchange_operations=frozenset({"  "}),
        )


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
    assert telemetry.recorded_event_count == 3
    assert telemetry.persist_failure_count == 0


def test_live_telemetry_prunes_only_inactive_unprotected_series() -> None:
    telemetry = LiveRuntimeTelemetry(run_id="run-1")
    old = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    telemetry._add_sample(
        symbol="BTCUSDT",
        lane="entry",
        transition="a->b",
        value=1.0,
        occurred_at=old,
    )
    telemetry._add_sample(
        symbol="ETHUSDT",
        lane="entry",
        transition="a->b",
        value=2.0,
        occurred_at=old,
    )

    pruned = telemetry.prune_inactive_symbols(
        now=old + timedelta(hours=2),
        protected_symbols={"BTCUSDT"},
        inactive_after=timedelta(hours=1),
    )

    assert pruned == 1
    assert telemetry.sample_series_count == 1
    assert telemetry.latency_summary()["BTCUSDT"]["entry"]["a->b"]["count"] == 1


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
