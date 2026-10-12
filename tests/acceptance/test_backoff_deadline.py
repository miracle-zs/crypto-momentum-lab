"""SYSTEM.md section 8: recovery backoff survives ordinary account events.

The repair worker must re-check its deadline and evidence *before each
execution*, not only at the request entry point (section 6: "每次执行前复核截止
和证据，不能仅在 request 入口做检查"). These cases drive the real
``LiveUnmanagedPositionRepair`` worker, so an unchanged divergence must not be
repaired again before its recorded deadline.

``repair_pending()`` returns whether the worker must *retain* a retry, so it is
allowed to keep returning True before the deadline. The observable that matters
is whether another repair attempt reached the exchange facts.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairBlocked,
)
from crypto_momentum_lab.live_rollout.position_self_healing import (
    LiveUnmanagedPositionRepair,
)
from tests.unit.live_rollout.test_postgres_runtime import (
    _position,
    _runtime_context,
)

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


class _StepClock:
    """An injectable monotonic clock the test advances explicitly."""

    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class BlockedRepairBook:
    """Evidence never proves this repair, so every attempt must be recorded.

    This is a test double for the *exchange facts* boundary only: it performs no
    backoff, settlement or recovery decision of its own. The decision under test
    lives entirely in the real ``LiveUnmanagedPositionRepair`` worker.
    """

    def __init__(self) -> None:
        self.calls: list[object] = []

    def get_active_stream(self, *_args: object) -> tuple[str, str]:
        return ("account_event_hub", "epoch-1")

    async def repair_position(self, request: object, **_kwargs: object) -> object:
        self.calls.append(request)
        raise PositionRepairBlocked(
            "repair projection does not match actual account exposure"
        )


def _worker(
    book: BlockedRepairBook,
    *,
    retry_interval_seconds: float,
    wakes: list[int],
    clock: _StepClock,
) -> LiveUnmanagedPositionRepair:
    return LiveUnmanagedPositionRepair(
        account_label="account-2",
        run_id="run-1",
        book=book,
        uow=object(),
        context_is_current=lambda context: True,
        invalidate_context=lambda: None,
        request_recovery=lambda: wakes.append(1),
        clock=clock,
        retry_interval_seconds=retry_interval_seconds,
    )


def _divergent(quantity: str) -> object:
    return replace(
        _runtime_context(),
        unmanaged_position_symbols=frozenset({"WUSDT"}),
        account_snapshot=SimpleNamespace(
            positions=(
                _position(symbol="WUSDT", position_amt=Decimal(quantity)),
            )
        ),
    )


@pytest.mark.asyncio
async def test_no_repair_attempt_before_the_recorded_deadline() -> None:
    """Ordinary account events must not add repair attempts before the deadline."""
    book = BlockedRepairBook()
    wakes: list[int] = []
    clock = _StepClock()
    runtime = _worker(
        book, retry_interval_seconds=3600.0, wakes=wakes, clock=clock
    )
    context = _divergent("3184.7")

    runtime.request(context)
    assert wakes == [1], "new divergent evidence requests recovery once"
    await runtime.repair_pending()
    assert len(book.calls) == 1

    # Ordinary account events keep arriving: the projection is republished with
    # the same quantity, price and stream. Each request must stay quiet, and the
    # worker must not attempt the same repair again before its deadline. The
    # unfinished repair is still retained for its own retry.
    for _ in range(5):
        runtime.request(replace(context, now=NOW))
        retained = await runtime.repair_pending()
        assert len(book.calls) == 1, (
            "the same unproven divergence was repaired again before its deadline"
        )
        assert retained is True, "the unfinished repair must stay retained"

    assert wakes == [1], "ordinary account events must not bypass the backoff"
    status = runtime.recovery_status()
    assert len(status) == 1
    assert status[0]["attempts"] == 1
    assert status[0]["next_attempt_in_seconds"] == pytest.approx(3600.0)


@pytest.mark.asyncio
async def test_new_evidence_may_repair_before_the_deadline() -> None:
    """A material change is not the same evidence and may act immediately."""
    book = BlockedRepairBook()
    wakes: list[int] = []
    clock = _StepClock()
    runtime = _worker(
        book, retry_interval_seconds=3600.0, wakes=wakes, clock=clock
    )

    runtime.request(_divergent("3184.7"))
    await runtime.repair_pending()
    assert len(book.calls) == 1

    runtime.request(_divergent("3100"))
    assert wakes == [1, 1], "changed evidence requests a new attempt"
    await runtime.repair_pending()
    assert len(book.calls) == 2
    assert book.calls[-1].expected_quantity == Decimal("3100")


@pytest.mark.asyncio
async def test_deadline_expiry_allows_exactly_one_more_attempt() -> None:
    """The worker's own deadline re-attempts the same evidence once."""
    book = BlockedRepairBook()
    wakes: list[int] = []
    clock = _StepClock()
    runtime = _worker(book, retry_interval_seconds=60.0, wakes=wakes, clock=clock)
    context = _divergent("3184.7")

    runtime.request(context)
    await runtime.repair_pending()
    assert len(book.calls) == 1

    # Before the deadline nothing re-attempts, however often facts are reread.
    clock.advance(59.0)
    runtime.request(replace(context, now=NOW))
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 1

    # At the deadline the same evidence is attempted exactly once more.
    clock.advance(1.0)
    runtime.request(replace(context, now=NOW))
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 2
    assert runtime.recovery_status()[0]["attempts"] == 2
