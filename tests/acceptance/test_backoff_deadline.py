"""SYSTEM.md section 8: recovery backoff survives ordinary account events.

The repair worker must re-check its deadline and evidence *before each
execution*, not only at the request entry point (section 6: "每次执行前复核截止
和证据，不能仅在 request 入口做检查").  These cases drive the real
``LiveUnmanagedPositionRepair`` worker, so a repair that is already deferred
must not be re-run by later context observations that carry the same evidence.
"""

from __future__ import annotations

import asyncio
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


class BlockedRepairBook:
    """Evidence never proves this repair, so every attempt must be recorded.

    This is a test double for the *exchange facts* boundary only: it performs no
    backoff, settlement or recovery decision of its own.  The decision under
    test lives entirely in the real ``LiveUnmanagedPositionRepair`` worker.
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
) -> LiveUnmanagedPositionRepair:
    return LiveUnmanagedPositionRepair(
        account_label="account-2",
        run_id="run-1",
        book=book,
        uow=object(),
        context_is_current=lambda context: True,
        invalidate_context=lambda: None,
        request_recovery=lambda: wakes.append(1),
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
async def test_deferred_repair_is_not_rerun_before_its_deadline() -> None:
    """The recorded backoff deadline gates every execution, not just request()."""
    book = BlockedRepairBook()
    wakes: list[int] = []
    runtime = _worker(book, retry_interval_seconds=3600.0, wakes=wakes)
    context = _divergent("3184.7")

    runtime.request(context)
    assert wakes == [1], "new divergent evidence requests recovery once"
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 1

    # Ordinary account events keep arriving: the projection is republished with
    # the same quantity, price and stream. Each request() must stay quiet, and
    # the worker's own execution path must honour the recorded deadline too.
    for _ in range(5):
        runtime.request(replace(context, now=NOW))
        assert await runtime.repair_pending() is False, (
            "a deferred repair ran again before its deadline"
        )

    assert wakes == [1], "ordinary account events must not bypass the backoff"
    assert len(book.calls) == 1, (
        "the same unproven divergence was repaired again before its deadline"
    )
    status = runtime.recovery_status()
    assert len(status) == 1
    assert status[0]["attempts"] == 1
    assert status[0]["next_attempt_in_seconds"] > 0


@pytest.mark.asyncio
async def test_new_evidence_may_repair_before_the_deadline() -> None:
    """A material change is not the same evidence and may act immediately."""
    book = BlockedRepairBook()
    wakes: list[int] = []
    runtime = _worker(book, retry_interval_seconds=3600.0, wakes=wakes)

    runtime.request(_divergent("3184.7"))
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 1

    runtime.request(_divergent("3100"))
    assert wakes == [1, 1], "changed evidence requests a new attempt"
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 2
    assert book.calls[-1].expected_quantity == Decimal("3100")


@pytest.mark.asyncio
async def test_deadline_expiry_allows_exactly_one_more_attempt() -> None:
    """The worker's own deadline re-attempts the same evidence once."""
    book = BlockedRepairBook()
    wakes: list[int] = []
    runtime = _worker(book, retry_interval_seconds=0.05, wakes=wakes)
    context = _divergent("3184.7")

    runtime.request(context)
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 1

    await asyncio.sleep(0.1)

    runtime.request(replace(context, now=NOW))
    assert await runtime.repair_pending() is True
    assert len(book.calls) == 2
    assert runtime.recovery_status()[0]["attempts"] == 2
