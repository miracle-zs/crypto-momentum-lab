import json

import pytest

import crypto_momentum_lab.market_data.hub as market_hub_module
from crypto_momentum_lab.market_data.hub import (
    MarketStateHub,
    MarketStateHubConfig,
    MarketStateHubEpochError,
    MarketStateHubReplayUnavailable,
    WebSocketMarketStateSource,
    encode_market_state_batch,
)
from tests.unit.market_data.test_hub import fixture_state

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
        FakeConnection((ready(replay_available=False, oldest_sequence=20),)),
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


async def test_fault_injection_durable_market_hub_missing_epoch_fails_closed(
    monkeypatch,
) -> None:
    """A durable worker must reject a batch that lost its stream epoch."""

    state = fixture_state("BTCUSDT", 0)
    sent_messages: list[str] = []

    ready = json.dumps(
        {
            "type": "market_state_hub_ready",
            "schema_version": 1,
            "environment": "research",
            "stream_id": "stream-a",
            "replay_available": True,
            "oldest_sequence": 1,
            "latest_sequence": 0,
        }
    )

    class FakeConnection:
        def __init__(self) -> None:
            self._messages = [
                ready,
                encode_market_state_batch(
                    (state,),
                    sequence=1,
                    published_at=state.bucket_end,
                ),
            ]

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

    monkeypatch.setattr(
        market_hub_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(),
    )
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="fault-gate-epoch",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
        fail_on_replay_unavailable=True,
    )
    iterator = source.batches()
    try:
        with pytest.raises(
            MarketStateHubEpochError,
            match="batch omitted stream epoch",
        ):
            await anext(iterator)
    finally:
        source.stop()
        await iterator.aclose()

    assert len(sent_messages) == 1
    assert "stream_id" not in json.loads(sent_messages[0])
