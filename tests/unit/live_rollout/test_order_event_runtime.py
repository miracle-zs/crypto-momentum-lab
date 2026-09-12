from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout.order_event_runtime import (
    LiveOrderEventRuntime,
)


@pytest.mark.asyncio
async def test_telemetry_failure_does_not_skip_order_observers() -> None:
    observed: list[str] = []

    class Telemetry:
        async def order_event(self, _plan: object, _event: object) -> None:
            raise OSError("telemetry unavailable")

    class Lifecycle:
        def observe(self, _plan: object, _event: object) -> None:
            observed.append("lifecycle")

    class Daemon:
        def observe_entry_order_event(
            self,
            _plan: object,
            _event: object,
        ) -> None:
            observed.append("daemon")

    runtime = LiveOrderEventRuntime(telemetry=Telemetry())  # type: ignore[arg-type]
    runtime.set_entry_order_lifecycle(Lifecycle())  # type: ignore[arg-type]
    runtime.set_daemon(Daemon())  # type: ignore[arg-type]

    await runtime.handle(
        SimpleNamespace(symbol="BTCUSDT", client_order_id="entry-1"),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
    )

    assert observed == ["lifecycle", "daemon"]
