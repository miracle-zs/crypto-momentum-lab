import asyncio

from crypto_momentum_lab.live_rollout.resource_lifecycle import (
    LiveResourceLifecycle,
)


class FakeResource:
    def __init__(self, label: str, events: list[str]) -> None:
        self._label = label
        self._events = events

    async def stop(self) -> None:
        self._events.append(self._label)

    async def aclose(self) -> None:
        self._events.append(self._label)

    def close(self) -> None:
        self._events.append(self._label)

    async def dispose(self) -> None:
        self._events.append(self._label)

    def stopped(self) -> None:
        self._events.append(self._label)


class HangingResource:
    async def stop(self) -> None:
        await asyncio.Event().wait()


async def test_live_resource_lifecycle_preserves_shutdown_order() -> None:
    events: list[str] = []

    lifecycle = LiveResourceLifecycle(
        entry_runtime=FakeResource("entry-runtime", events),  # type: ignore[arg-type]
        entry_order_lifecycle=FakeResource("entry-orders", events),  # type: ignore[arg-type]
        execution_coordinator=FakeResource("coordinator", events),  # type: ignore[arg-type]
        client=FakeResource("client", events),  # type: ignore[arg-type]
        closed_candle_feed=FakeResource("candle-feed", events),  # type: ignore[arg-type]
        candle_source=FakeResource("candle-source", events),  # type: ignore[arg-type]
        ema_candle_source=FakeResource("ema-source", events),  # type: ignore[arg-type]
        signal_recorder=FakeResource("signal-recorder", events),  # type: ignore[arg-type]
        telemetry=FakeResource("telemetry", events),  # type: ignore[arg-type]
        volume_cache=FakeResource("volume-cache", events),  # type: ignore[arg-type]
        volume_rest_client=FakeResource("volume-client", events),  # type: ignore[arg-type]
        execution_engine=FakeResource("execution-engine", events),  # type: ignore[arg-type]
        market_engine=FakeResource("market-engine", events),  # type: ignore[arg-type]
        observability_engine=FakeResource("observability-engine", events),  # type: ignore[arg-type]
        checkpoint_engine=FakeResource("checkpoint-engine", events),  # type: ignore[arg-type]
        heartbeat_engine=FakeResource("heartbeat-engine", events),  # type: ignore[arg-type]
        health=FakeResource("health", events),  # type: ignore[arg-type]
    )

    await lifecycle.close()

    assert events == [
        "entry-runtime",
        "entry-orders",
        "coordinator",
        "client",
        "candle-feed",
        "candle-source",
        "ema-source",
        "signal-recorder",
        "telemetry",
        "volume-cache",
        "volume-client",
        "execution-engine",
        "market-engine",
        "observability-engine",
        "checkpoint-engine",
        "heartbeat-engine",
        "health",
    ]


async def test_live_resource_lifecycle_closes_shared_candle_source_once() -> None:
    events: list[str] = []
    source = FakeResource("candle-source", events)

    lifecycle = LiveResourceLifecycle(
        entry_runtime=None,
        entry_order_lifecycle=None,
        execution_coordinator=None,
        client=None,
        closed_candle_feed=None,
        candle_source=source,  # type: ignore[arg-type]
        ema_candle_source=source,  # type: ignore[arg-type]
        signal_recorder=None,
        telemetry=None,
        volume_cache=None,
        volume_rest_client=None,
        execution_engine=None,
        market_engine=None,
        observability_engine=None,
        checkpoint_engine=None,
        heartbeat_engine=None,
        health=None,
    )

    await lifecycle.close()

    assert events == ["candle-source"]


async def test_live_resource_lifecycle_bounds_hanging_cleanup() -> None:
    events: list[str] = []

    lifecycle = LiveResourceLifecycle(
        entry_runtime=HangingResource(),  # type: ignore[arg-type]
        entry_order_lifecycle=None,
        execution_coordinator=None,
        client=None,
        closed_candle_feed=None,
        candle_source=None,
        ema_candle_source=None,
        signal_recorder=None,
        telemetry=None,
        volume_cache=None,
        volume_rest_client=None,
        execution_engine=None,
        market_engine=None,
        observability_engine=None,
        checkpoint_engine=None,
        heartbeat_engine=None,
        health=FakeResource("health", events),  # type: ignore[arg-type]
        shutdown_timeout_seconds=0.01,
    )

    await lifecycle.close()

    assert events == ["health"]
