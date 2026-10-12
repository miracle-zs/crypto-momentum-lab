from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
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

    runtime = LiveOrderEventRuntime(
        telemetry=Telemetry(), request_recovery=Mock()
    )  # type: ignore[arg-type]
    runtime.set_entry_order_lifecycle(Lifecycle())  # type: ignore[arg-type]
    runtime.set_entry_observer(
        Daemon().observe_entry_order_event  # type: ignore[arg-type]
    )

    await runtime.handle(
        SimpleNamespace(  # type: ignore[arg-type]
            symbol="BTCUSDT", client_order_id="entry-1"
        ),
        SimpleNamespace(  # type: ignore[arg-type]
            state=ExchangeOrderState.ACKNOWLEDGED
        ),
    )

    assert observed == ["lifecycle", "daemon"]


@pytest.mark.parametrize(
    "state",
    [
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    ],
)
async def test_only_unknown_order_results_request_repair(state):
    from unittest.mock import AsyncMock, Mock

    request = Mock()
    runtime = LiveOrderEventRuntime(
        telemetry=SimpleNamespace(order_event=AsyncMock()), request_recovery=request
    )
    await runtime.handle(SimpleNamespace(), SimpleNamespace(state=state))
    assert request.call_count == (
        1 if state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION else 0
    )


@pytest.mark.asyncio
async def test_order_event_runtime_queries_durable_terminal_barrier() -> None:
    from decimal import Decimal
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.live_rollout.pending_entries import (
        TerminalOrderBarrier,
    )

    async def mock_durable_checker(
        cid: str,
    ) -> tuple[bool, Decimal] | None:
        if cid == "evicted-terminal-order":
            return True, Decimal("1.5")
        return None

    barrier = TerminalOrderBarrier(
        capacity=10,
        durable_async_checker=mock_durable_checker,
    )

    runtime = LiveOrderEventRuntime(
        telemetry=SimpleNamespace(order_event=AsyncMock()),
        request_recovery=Mock(),
        terminal_barrier=barrier,
    )

    plan = SimpleNamespace(
        symbol="BTCUSDT", client_order_id="evicted-terminal-order"
    )
    event = SimpleNamespace(state=ExchangeOrderState.ACKNOWLEDGED)

    assert not barrier.is_terminal("evicted-terminal-order")
    await runtime.handle(plan, event)  # type: ignore[arg-type]
    assert barrier.is_terminal("evicted-terminal-order")
    assert barrier.get_executed_quantity(
        "evicted-terminal-order"
    ) == Decimal("1.5")


@pytest.mark.asyncio
async def test_order_event_runtime_durable_check_error_keeps_observers() -> None:
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.live_rollout.pending_entries import (
        TerminalOrderBarrier,
    )

    observed: list[str] = []

    async def throwing_durable_checker(
        _cid: str,
    ) -> tuple[bool, Decimal] | None:
        raise OSError("database connection failed")

    barrier = TerminalOrderBarrier(
        capacity=10,
        durable_async_checker=throwing_durable_checker,
    )

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

    runtime = LiveOrderEventRuntime(
        telemetry=SimpleNamespace(order_event=AsyncMock()),
        request_recovery=Mock(),
        terminal_barrier=barrier,
    )
    runtime.set_entry_order_lifecycle(Lifecycle())  # type: ignore[arg-type]
    runtime.set_entry_observer(
        Daemon().observe_entry_order_event  # type: ignore[arg-type]
    )

    plan = SimpleNamespace(
        symbol="BTCUSDT", client_order_id="order-err"
    )
    event = SimpleNamespace(state=ExchangeOrderState.ACKNOWLEDGED)

    await runtime.handle(plan, event)  # type: ignore[arg-type]
    assert observed == ["lifecycle", "daemon"]
