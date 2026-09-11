import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
    RiskControlHub,
    RiskControlHubConfig,
    RiskControlHubProtocolError,
    RiskControlHubSequenceGap,
    WebSocketRiskControlPublisher,
    WebSocketRiskControlSource,
    decode_risk_control_event,
    encode_risk_control_event,
)


def _event(
    *,
    event_id: str = "command-1",
    action: RiskControlAction = RiskControlAction.DRAIN,
    session_id: str | None = "live-primary-v1",
) -> RiskControlEvent:
    return RiskControlEvent(
        environment="live",
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id=session_id,
        action=action,
        event_id=event_id,
        command_id=event_id,
        reason="operator_disabled_new_entries",
        issued_at=datetime(2026, 9, 11, 6, 0, tzinfo=UTC),
        details={"transition_id": event_id},
    )


def test_risk_control_event_round_trips() -> None:
    event = _event()

    decoded = decode_risk_control_event(encode_risk_control_event(event))

    assert decoded == event


@pytest.mark.parametrize(
    "action",
    [
        RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
        RiskControlAction.REQUEST_FLATTEN,
    ],
)
def test_one_shot_risk_control_actions_round_trip(action: RiskControlAction) -> None:
    event = _event(action=action)

    decoded = decode_risk_control_event(encode_risk_control_event(event))

    assert decoded.action is action


def test_risk_control_publish_message_requires_valid_token_shape() -> None:
    event = _event()
    payload = encode_risk_control_event(
        event,
        message_type="publish_risk_control",
        auth_token="secret",
    )

    decoded = decode_risk_control_event(payload)

    assert decoded == event


def test_risk_control_event_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="issued_at"):
        _event().__class__(
            environment="live",
            account_label="primary",
            strategy_name="orderflow_impulse",
            session_id="live-primary-v1",
            action=RiskControlAction.DRAIN,
            event_id="command-1",
            command_id="command-1",
            reason="reason",
            issued_at=datetime(2026, 9, 11, 6, 0),
        )


async def test_hub_publishes_only_matching_scope_and_replays_contiguously() -> None:
    hub = RiskControlHub(
        RiskControlHubConfig(
            port=0,
            reconnect_delays=(0,),
            unavailable_timeout_seconds=2,
        )
    )
    await hub.start()
    source = WebSocketRiskControlSource(
        url=hub.url,
        environment="live",
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id="live-primary-v1",
        consumer_id="test-risk-control",
        config=RiskControlHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=2,
        ),
    )
    iterator = source.__aiter__()
    try:
        first_event_task = asyncio.create_task(iterator.__anext__())
        await asyncio.sleep(0.05)
        hub.publish(_event())
        first = await asyncio.wait_for(first_event_task, timeout=2)
        assert first.sequence == 1
        assert first.action is RiskControlAction.DRAIN

        ignored_task = asyncio.create_task(iterator.__anext__())
        await asyncio.sleep(0.05)
        hub.publish(
            _event(
                event_id="other-session",
                session_id="another-session",
            )
        )
        hub.publish(_event(event_id="command-2", action=RiskControlAction.HALT))
        second = await asyncio.wait_for(ignored_task, timeout=2)
        assert second.event_id == "command-2"
        assert second.sequence == 3
    finally:
        source.stop()
        await iterator.aclose()
        await hub.stop()


async def test_publisher_authenticates_and_returns_transport_metadata() -> None:
    hub = RiskControlHub(
        RiskControlHubConfig(
            port=0,
            publish_token="secret",
        )
    )
    await hub.start()
    try:
        publisher = WebSocketRiskControlPublisher(
            url=hub.url,
            token="secret",
        )
        published = await publisher.publish(_event())
        assert published.sequence == 1
        assert published.stream_epoch is not None

        unauthorized = WebSocketRiskControlPublisher(
            url=hub.url,
            token="wrong",
        )
        with pytest.raises(RiskControlHubProtocolError):
            await unauthorized.publish(_event(event_id="command-2"))
        assert hub.metrics.published_event_count == 1
    finally:
        await hub.stop()


def test_source_recovers_after_sequence_gap() -> None:
    source = WebSocketRiskControlSource(
        url="ws://unused",
        environment="live",
        account_label="primary",
        consumer_id="test-risk-control",
    )
    first = replace(_event(), sequence=1, stream_epoch="epoch")
    gap = replace(
        first,
        event_id="command-3",
        sequence=3,
    )
    assert source._materialize(first).sequence == 1
    assert source._materialize(first) is None
    with pytest.raises(RiskControlHubSequenceGap):
        source._materialize(gap)
    assert source.metrics.recovery_count == 1
    assert source.metrics.last_recovery_reason == "risk_control_sequence_gap"
