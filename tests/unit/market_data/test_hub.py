import asyncio
import json
import os
from dataclasses import replace

import pytest

import crypto_momentum_lab.market_data.hub as hub_module
from crypto_momentum_lab.market_data.hub import (
    MarketStateHub,
    MarketStateHubConfig,
    MarketStateHubError,
    MarketStateHubReplayUnavailable,
    WebSocketMarketStateSource,
    decode_market_state_batch,
    decode_market_state_batch_envelope,
    encode_market_state_batch,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


def test_market_state_batch_round_trips_decimal_and_timestamps() -> None:
    state = replace(
        fixture_state("BTCUSDT", 0),
        data_complete=False,
        missing_agg_trade_count=2,
    )

    encoded = encode_market_state_batch(
        (state,),
        sequence=1,
        published_at=state.bucket_end,
    )

    decoded = decode_market_state_batch(
        encoded,
        expected_environment="research",
    )

    assert decoded == (state,)


def test_market_state_batch_decoder_defaults_legacy_completeness_fields() -> None:
    state = fixture_state("BTCUSDT", 0)
    payload = json.loads(
        encode_market_state_batch(
            (state,),
            sequence=1,
            published_at=state.bucket_end,
        )
    )
    del payload["states"][0]["data_complete"]
    del payload["states"][0]["missing_agg_trade_count"]

    decoded = decode_market_state_batch(json.dumps(payload))

    assert decoded == (state,)


def test_market_state_batch_decoder_preserves_sequence_metadata() -> None:
    state = fixture_state("BTCUSDT", 0)
    encoded = encode_market_state_batch(
        (state,),
        sequence=7,
        published_at=state.bucket_end,
    )

    decoded = decode_market_state_batch_envelope(
        encoded,
        expected_environment="research",
    )

    assert decoded.sequence == 7
    assert decoded.environment == "research"
    assert decoded.states == (state,)


async def test_market_state_hub_replay_window_detects_unrecoverable_gap() -> None:
    hub = MarketStateHub(
        MarketStateHubConfig(
            replay_batch_count=2,
        )
    )
    await hub.publish((fixture_state("BTCUSDT", 0),))
    await hub.publish((fixture_state("BTCUSDT", 1),))
    await hub.publish((fixture_state("BTCUSDT", 2),))

    available, oldest, latest, messages = hub._replay_snapshot("research", 1)
    assert available is True
    assert oldest == 2
    assert latest == 3
    assert [
        decode_market_state_batch_envelope(item).sequence for item in messages
    ] == [2, 3]

    available, oldest, latest, messages = hub._replay_snapshot("research", 0)
    assert available is False
    assert oldest == 2
    assert latest == 3
    assert messages == ()


async def test_batch_source_can_fail_closed_with_replay_metadata(monkeypatch) -> None:
    messages = [
        json.dumps(
            {
                "type": "market_state_hub_ready",
                "schema_version": 1,
                "environment": "research",
                "stream_id": "stream-a",
                "replay_available": False,
                "oldest_sequence": 20,
                "latest_sequence": 30,
            }
        )
    ]

    class FakeConnection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _message):
            return None

        async def recv(self):
            return messages.pop(0)

    monkeypatch.setattr(
        hub_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(),
    )
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-collector",
        config=MarketStateHubConfig(reconnect_delays=(0,)),
        fail_on_replay_unavailable=True,
    )
    source.set_resume_cursor(stream_id="stream-a", sequence=5)

    with pytest.raises(
        MarketStateHubReplayUnavailable,
        match="replay is unavailable",
    ) as raised:
        await anext(source.batches())

    assert raised.value.requested_sequence == 5
    assert raised.value.oldest_sequence == 20
    assert raised.value.latest_sequence == 30
    assert raised.value.stream_id == "stream-a"


@pytest.mark.skipif(
    os.environ.get("CML_RUN_HUB_NETWORK_TESTS") != "1",
    reason="requires local loopback socket permission",
)
async def test_market_state_hub_fans_out_without_postgres() -> None:
    hub = MarketStateHub(
        MarketStateHubConfig(
            host="127.0.0.1",
            port=0,
            reconnect_delays=(0,),
            unavailable_timeout_seconds=1,
        )
    )
    await hub.start()
    source = WebSocketMarketStateSource(
        url=hub.url,
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=1,
        ),
    )
    iterator = source.__aiter__()
    next_state = asyncio.create_task(iterator.__anext__())
    try:
        for _ in range(100):
            if hub.subscriber_count == 1:
                break
            await asyncio.sleep(0.01)
        assert hub.subscriber_count == 1

        first = fixture_state("BTCUSDT", 0)
        second = fixture_state("ETHUSDT", 0)
        await hub.publish((first, second))

        assert await asyncio.wait_for(next_state, timeout=1) == first
        assert await asyncio.wait_for(iterator.__anext__(), timeout=1) == second
        assert hub.metrics.published_batch_count == 1
    finally:
        source.stop()
        if not next_state.done():
            next_state.cancel()
        await hub.stop()
        await asyncio.gather(next_state, return_exceptions=True)


async def test_market_state_source_fails_closed_when_hub_is_unavailable() -> None:
    source = WebSocketMarketStateSource(
        url="ws://127.0.0.1:1",
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=0.05,
            handshake_timeout_seconds=0.01,
        ),
    )

    with pytest.raises(RuntimeError, match="market-state hub unavailable"):
        await anext(source.__aiter__())


async def test_market_state_source_reports_reconnect_to_resilient_consumer(
    monkeypatch,
) -> None:
    state = fixture_state("BTCUSDT", 0)
    next_state = fixture_state("BTCUSDT", 1)
    messages = [
        json.dumps(
            {
                "type": "market_state_hub_ready",
                "schema_version": 1,
                "environment": "research",
            }
        ),
        encode_market_state_batch(
            (state,),
            sequence=1,
            published_at=state.bucket_end,
        ),
        encode_market_state_batch(
            (next_state,),
            sequence=2,
            published_at=next_state.bucket_end,
        ),
    ]

    class FakeConnection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _message):
            return None

        async def recv(self):
            return messages.pop(0)

    class ConnectFactory:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise OSError("simulated hub outage")
            return FakeConnection()

    connect = ConnectFactory()
    monkeypatch.setattr(hub_module, "connect", connect)
    statuses = []
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
        on_connection_change=lambda available, reason: statuses.append(
            (available, reason)
        ),
    )

    iterator = source.__aiter__()
    try:
        assert await asyncio.wait_for(anext(iterator), timeout=1) == state
        assert await asyncio.wait_for(anext(iterator), timeout=1) == next_state
    finally:
        source.stop()
        await iterator.aclose()

    assert connect.calls == 2
    assert statuses[0][0] is False
    assert any(available for available, _reason in statuses)


async def test_market_state_source_resumes_from_last_sequence(monkeypatch) -> None:
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("BTCUSDT", 1)
    ready = json.dumps(
        {
            "type": "market_state_hub_ready",
            "schema_version": 1,
            "environment": "research",
            "replay_available": True,
        }
    )
    sent_messages: list[str] = []

    class FakeConnection:
        def __init__(self, messages) -> None:
            self._messages = list(messages)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, message):
            sent_messages.append(message)

        async def recv(self):
            message = self._messages.pop(0)
            if isinstance(message, BaseException):
                raise message
            return message

    connections = [
        FakeConnection(
            (
                ready,
                encode_market_state_batch(
                    (first,),
                    sequence=1,
                    published_at=first.bucket_end,
                ),
                OSError("simulated disconnect"),
            )
        ),
        FakeConnection(
            (
                ready,
                encode_market_state_batch(
                    (second,),
                    sequence=2,
                    published_at=second.bucket_end,
                ),
            )
        ),
    ]

    def fake_connect(*_args, **_kwargs):
        return connections.pop(0)

    monkeypatch.setattr(hub_module, "connect", fake_connect)
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
    )
    iterator = source.__aiter__()
    try:
        assert await anext(iterator) == first
        assert await anext(iterator) == second
    finally:
        source.stop()
        await iterator.aclose()

    assert json.loads(sent_messages[0]).get("last_sequence") is None
    assert json.loads(sent_messages[1])["last_sequence"] == 1


async def test_market_state_source_reader_skips_backlog_when_consumer_is_slow(
    monkeypatch,
) -> None:
    states = [fixture_state("BTCUSDT", index) for index in range(5)]
    sent_messages: list[str] = []
    disconnect = asyncio.Event()

    def ready(latest_sequence: int) -> str:
        return json.dumps(
            {
                "type": "market_state_hub_ready",
                "schema_version": 1,
                "environment": "research",
                "stream_id": "stream-a",
                "replay_available": True,
                "latest_sequence": latest_sequence,
            }
        )

    class FakeConnection:
        def __init__(self, messages) -> None:
            self._messages = list(messages)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, message):
            sent_messages.append(message)

        async def recv(self):
            if self._messages:
                message = self._messages.pop(0)
                if isinstance(message, BaseException):
                    raise message
                return message
            await disconnect.wait()
            raise OSError("simulated disconnect")

    connections = [
        FakeConnection(
            (
                ready(1),
                encode_market_state_batch(
                    (states[0],),
                    sequence=1,
                    published_at=states[0].bucket_end,
                    stream_id="stream-a",
                ),
                encode_market_state_batch(
                    (states[1],),
                    sequence=2,
                    published_at=states[1].bucket_end,
                    stream_id="stream-a",
                ),
                encode_market_state_batch(
                    (states[2],),
                    sequence=3,
                    published_at=states[2].bucket_end,
                    stream_id="stream-a",
                ),
                encode_market_state_batch(
                    (states[3],),
                    sequence=4,
                    published_at=states[3].bucket_end,
                    stream_id="stream-a",
                ),
            )
        ),
        FakeConnection(
            (
                ready(4),
                encode_market_state_batch(
                    (states[4],),
                    sequence=5,
                    published_at=states[4].bucket_end,
                    stream_id="stream-a",
                ),
            )
        ),
    ]

    monkeypatch.setattr(
        hub_module,
        "connect",
        lambda *_args, **_kwargs: connections.pop(0),
    )
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
    )
    iterator = source.__aiter__()
    try:
        assert await anext(iterator) == states[0]
        # Give the independent reader a chance to drain the socket while the
        # strategy consumer is between state-processing calls.
        await asyncio.sleep(0.01)
        assert await anext(iterator) == states[4]
    finally:
        source.stop()
        await iterator.aclose()

    assert json.loads(sent_messages[1])["last_sequence"] == 4


async def test_market_state_source_fails_closed_on_sequence_gap(monkeypatch) -> None:
    first = fixture_state("BTCUSDT", 0)
    skipped = fixture_state("BTCUSDT", 2)
    ready = json.dumps(
        {
            "type": "market_state_hub_ready",
            "schema_version": 1,
            "environment": "research",
        }
    )

    class FakeConnection:
        def __init__(self) -> None:
            self._messages = [
                ready,
                encode_market_state_batch(
                    (first,),
                    sequence=1,
                    published_at=first.bucket_end,
                ),
                encode_market_state_batch(
                    (skipped,),
                    sequence=3,
                    published_at=skipped.bucket_end,
                ),
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _message):
            return None

        async def recv(self):
            return self._messages.pop(0)

    monkeypatch.setattr(
        hub_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(),
    )
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=0.000001,
        ),
    )
    iterator = source.__aiter__()
    try:
        assert await anext(iterator) == first
        with pytest.raises(MarketStateHubError, match="market-state hub unavailable"):
            await anext(iterator)
    finally:
        source.stop()
        await iterator.aclose()


async def test_market_state_source_accepts_new_stream_epoch_after_hub_restart(
    monkeypatch,
) -> None:
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("BTCUSDT", 1)
    sent_messages: list[str] = []

    def ready(stream_id: str, latest_sequence: int) -> str:
        return json.dumps(
            {
                "type": "market_state_hub_ready",
                "schema_version": 1,
                "environment": "research",
                "stream_id": stream_id,
                "stream_reset": stream_id == "stream-b",
                "replay_available": True,
                "latest_sequence": latest_sequence,
            }
        )

    class FakeConnection:
        def __init__(self, messages) -> None:
            self._messages = list(messages)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, message):
            sent_messages.append(message)

        async def recv(self):
            message = self._messages.pop(0)
            if isinstance(message, BaseException):
                raise message
            return message

    connections = [
        FakeConnection(
            (
                ready("stream-a", 1),
                encode_market_state_batch(
                    (first,),
                    sequence=1,
                    published_at=first.bucket_end,
                    stream_id="stream-a",
                ),
                OSError("hub restarted"),
            )
        ),
        FakeConnection(
            (
                ready("stream-b", 0),
                encode_market_state_batch(
                    (second,),
                    sequence=1,
                    published_at=second.bucket_end,
                    stream_id="stream-b",
                ),
            )
        ),
    ]

    monkeypatch.setattr(
        hub_module,
        "connect",
        lambda *_args, **_kwargs: connections.pop(0),
    )
    source = WebSocketMarketStateSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-live",
        config=MarketStateHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
    )
    iterator = source.__aiter__()
    try:
        assert await anext(iterator) == first
        assert await anext(iterator) == second
    finally:
        source.stop()
        await iterator.aclose()

    assert json.loads(sent_messages[1])["stream_id"] == "stream-a"
    assert json.loads(sent_messages[1])["last_sequence"] == 1
