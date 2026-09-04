import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_EXIT,
    LIVE_TRIGGER_SOURCE_QUOTE,
    SOURCE_RECEIVED,
    TRACE_TERMINATED,
    LiveRuntimeTelemetry,
    SourceIngress,
    TraceKey,
)
from tests.unit.shadow_operation.test_service import _intent, _state


def test_source_ingress_keeps_source_and_local_receive_times_separate() -> None:
    source_at = datetime(2026, 9, 4, 0, 0, tzinfo=UTC)
    received_at = source_at + timedelta(milliseconds=125)
    ingress = SourceIngress(
        run_id="run-1",
        source_event_id="quote:BTCUSDT:123",
        lane=LIVE_LANE_EXIT,
        trigger_source=LIVE_TRIGGER_SOURCE_QUOTE,
        source_occurred_at=source_at,
        received_at=received_at,
        symbol="BTCUSDT",
    )

    assert ingress.trace_key == TraceKey("run-1", "quote:BTCUSDT:123")
    assert ingress.trace_id == ingress.trace_key.as_id()
    assert ingress.details()["source_occurred_at"] == source_at.isoformat()
    assert ingress.details()["source_received_at"] == received_at.isoformat()


def test_trace_key_length_prefix_prevents_separator_collisions() -> None:
    left = TraceKey("run:a", "event")
    right = TraceKey("run", "a:event")

    assert left != right
    assert left.as_id() != right.as_id()


def test_exit_source_ingress_requires_a_known_trigger_source() -> None:
    with pytest.raises(ValueError, match="trigger_source"):
        SourceIngress(
            run_id="run-1",
            source_event_id="event-1",
            lane=LIVE_LANE_EXIT,
            trigger_source=None,
            received_at=datetime(2026, 9, 4, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", " "),
        ("source_event_id", ""),
        ("lane", "other"),
        ("trigger_source", "webhook"),
    ],
)
def test_source_ingress_rejects_invalid_identity_or_metadata(
    field: str,
    value: str,
) -> None:
    kwargs: dict[str, object] = {
        "run_id": "run-1",
        "source_event_id": "event-1",
        "lane": "entry",
        "trigger_source": None,
        "received_at": datetime(2026, 9, 4, tzinfo=UTC),
    }
    kwargs[field] = value

    with pytest.raises(ValueError):
        SourceIngress(**kwargs)  # type: ignore[arg-type]


async def test_source_received_records_a_stable_trace_and_ingress_details() -> None:
    received_at = datetime(2026, 9, 4, 0, 0, 1, tzinfo=UTC)
    ingress = SourceIngress(
        run_id="run-1",
        source_event_id="account-event-1",
        lane="unknown",
        trigger_source=None,
        received_at=received_at,
    )
    telemetry = LiveRuntimeTelemetry(run_id="run-1")

    await telemetry.source_received(ingress)

    event = telemetry.recent_events[0]
    assert event.event_type == SOURCE_RECEIVED
    assert event.occurred_at == received_at
    assert event.details["trace_id"] == ingress.trace_id
    assert event.details["source_event_id"] == "account-event-1"


async def test_source_received_rejects_cross_run_ingress() -> None:
    ingress = SourceIngress(
        run_id="run-2",
        source_event_id="event-1",
        lane="entry",
        trigger_source=None,
        received_at=datetime(2026, 9, 4, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="run_id"):
        await LiveRuntimeTelemetry(run_id="run-1").source_received(ingress)


async def test_trace_terminated_records_reason_on_the_source_trace() -> None:
    received_at = datetime(2026, 9, 4, 0, 0, 1, tzinfo=UTC)
    ingress = SourceIngress(
        run_id="run-1",
        source_event_id="quote-1",
        lane=LIVE_LANE_EXIT,
        trigger_source=LIVE_TRIGGER_SOURCE_QUOTE,
        received_at=received_at,
        symbol="BTCUSDT",
    )
    telemetry = LiveRuntimeTelemetry(run_id="run-1")

    await telemetry.source_received(ingress)
    await telemetry.trace_terminated(
        ingress,
        occurred_at=received_at + timedelta(milliseconds=25),
        reason="no_exit_request",
        details={"request_count": 0},
    )

    event = telemetry.recent_events[-1]
    assert event.event_type == TRACE_TERMINATED
    assert event.details["trace_id"] == ingress.trace_id
    assert event.details["reason"] == "no_exit_request"
    assert event.details["request_count"] == 0


async def test_trace_terminated_rejects_an_empty_reason() -> None:
    ingress = SourceIngress(
        run_id="run-1",
        source_event_id="quote-1",
        lane=LIVE_LANE_EXIT,
        trigger_source=LIVE_TRIGGER_SOURCE_QUOTE,
        received_at=datetime(2026, 9, 4, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="reason"):
        await LiveRuntimeTelemetry(run_id="run-1").trace_terminated(
            ingress,
            occurred_at=datetime(2026, 9, 4, tzinfo=UTC),
            reason=" ",
        )


async def test_terminal_reason_summary_groups_by_lane_and_source() -> None:
    received_at = datetime(2026, 9, 4, tzinfo=UTC)
    quote_ingress = SourceIngress(
        run_id="run-1",
        source_event_id="quote-1",
        lane=LIVE_LANE_EXIT,
        trigger_source=LIVE_TRIGGER_SOURCE_QUOTE,
        received_at=received_at,
        symbol="BTCUSDT",
    )
    account_ingress = replace(
        quote_ingress,
        source_event_id="account-1",
        trigger_source="account",
    )
    telemetry = LiveRuntimeTelemetry(run_id="run-1")

    await telemetry.trace_terminated(
        quote_ingress,
        occurred_at=received_at,
        reason="no_exit_request",
    )
    await telemetry.trace_terminated(
        quote_ingress,
        occurred_at=received_at + timedelta(seconds=1),
        reason="no_exit_request",
    )
    await telemetry.trace_terminated(
        account_ingress,
        occurred_at=received_at + timedelta(seconds=2),
        reason="exit_intent_processed",
    )

    summary = telemetry.terminal_reason_summary()

    assert summary == {
        "exit": {
            "account": {"exit_intent_processed": 1},
            "quote": {"no_exit_request": 2},
        }
    }
    assert json.loads(json.dumps(summary)) == summary
    summary["exit"]["quote"]["no_exit_request"] = 99
    assert telemetry.terminal_reason_summary()["exit"]["quote"] == {
        "no_exit_request": 2
    }


async def test_source_metadata_survives_child_order_events() -> None:
    state = _state()
    candidate = replace(
        _intent(),
        candidate_id="exit-candidate-1",
        reduce_only=True,
    )
    ingress = SourceIngress(
        run_id="run-1",
        source_event_id="account-1",
        lane=LIVE_LANE_EXIT,
        trigger_source="account",
        received_at=state.first_received_at,
        symbol=state.symbol,
        bucket_start=state.bucket_start,
    )
    telemetry = LiveRuntimeTelemetry(run_id="run-1")
    plan = OrderExecutionPlan(
        intent_id=candidate.candidate_id,
        run_id="run-1",
        client_order_id="cml_exit_12345678901234567890123456789012",
        symbol=state.symbol,
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("0.001"),
        price=None,
        reduce_only=True,
        created_at=state.bucket_end,
        quantized=True,
    )

    await telemetry.source_received(ingress)
    await telemetry.candidate_accepted(
        candidate,
        state=state,
        occurred_at=state.bucket_end,
        lane=LIVE_LANE_EXIT,
        ingress=ingress,
    )
    await telemetry.order_event(
        plan,
        ExchangeOrderEvent(
            event_id="submitting-1",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            occurred_at=state.bucket_end,
            exchange_order_id=None,
            details={},
        ),
    )

    event = telemetry.recent_events[-1]
    assert event.details["source_trace_id"] == ingress.trace_id
    assert event.details["source_event_id"] == ingress.source_event_id
