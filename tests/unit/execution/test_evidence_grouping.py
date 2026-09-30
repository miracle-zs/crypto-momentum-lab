import asyncio
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.evidence_grouping import (
    observe_evidence_group,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
)
from tests.unit.execution.test_cumulative_report import NOW, SCOPE, fill, report


def evidence():
    return ExecutionEvidence(
        "group",
        SCOPE,
        NOW,
        fills=(
            replace(fill("2"), trade_id="first"),
            replace(fill("4"), trade_id="second"),
        ),
        cumulative_order=report(),
    )


@pytest.mark.asyncio
async def test_evidence_without_fills_is_forwarded_unchanged():
    source = ExecutionEvidence("group", SCOPE, NOW, cumulative_order=report())
    result = Applied("group", "view")
    observe = AsyncMock(return_value=result)
    forget = Mock()
    assert (
        await observe_evidence_group(
            source, observe_one=observe, forget_identity=forget
        )
        is result
    )
    observe.assert_awaited_once_with(source)
    forget.assert_not_called()


@pytest.mark.asyncio
async def test_fills_are_processed_sequentially_before_report_and_aggregate_once():
    source = evidence()
    calls = []
    forgotten = []

    async def observe(item):
        calls.append(item)
        if item.fill is not None:
            assert item.cumulative_order is None and not item.fills
            assert item.snapshot is None and item.order_event is None
            assert item.stream_checkpoint_adoption is None
            return Applied(
                item.evidence_id,
                "intermediate",
                consumed_quantity=item.fill.quantity,
                diagnostics=("repeat",),
            )
        assert len(forgotten) == 2
        assert item.cumulative_order is source.cumulative_order
        return Applied(
            item.evidence_id,
            "final",
            released_quantity=Decimal("1"),
            recovery_required=True,
            diagnostics=("repeat", "recovery"),
        )

    result = await observe_evidence_group(
        source, observe_one=observe, forget_identity=forgotten.append
    )
    assert [item.fill.trade_id for item in calls[:-1]] == ["first", "second"]
    assert calls[-1].fill is None and calls[-1].fills == ()
    assert result.evidence_id == "group" and result.updated_view_token == "final"
    assert result.consumed_quantity == Decimal(
        "6"
    ) and result.released_quantity == Decimal("1")
    assert result.recovery_required and result.diagnostics == ("repeat", "recovery")
    assert len(forgotten) == 2
    assert (
        source.fills[0].quantity == Decimal("2") and source.cumulative_order is not None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict_index", [0, 1])
async def test_fill_conflict_stops_group_and_reports_original_identity(conflict_index):
    responses = [Applied("first", "view"), EvidenceConflict("internal", "conflict")]
    if conflict_index == 0:
        responses = responses[1:]
    observe = AsyncMock(side_effect=responses)
    forget = Mock()
    result = await observe_evidence_group(
        evidence(), observe_one=observe, forget_identity=forget
    )
    assert result == EvidenceConflict("group", "conflict")
    assert observe.await_count == conflict_index + 1
    assert forget.call_count == conflict_index + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "remainder",
    [Duplicate("group", "view"), EvidenceConflict("group", "remainder conflict")],
)
async def test_remainder_duplicate_or_conflict_is_returned_without_rewriting(remainder):
    observe = AsyncMock(
        side_effect=[Applied("first", "v"), Applied("second", "v"), remainder]
    )
    assert (
        await observe_evidence_group(
            evidence(), observe_one=observe, forget_identity=Mock()
        )
        is remainder
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [RuntimeError("write failed"), asyncio.CancelledError()]
)
async def test_callback_failure_or_cancellation_propagates_without_processing_remainder(
    failure,
):
    observe = AsyncMock(side_effect=failure)
    forget = Mock()
    with pytest.raises(type(failure)):
        await observe_evidence_group(
            evidence(), observe_one=observe, forget_identity=forget
        )
    assert observe.await_count == 1
    forget.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_fill_is_not_counted_in_group_totals():
    observe = AsyncMock(
        side_effect=[
            Duplicate("first", "v"),
            Applied("second", "v", consumed_quantity=Decimal("4")),
            Applied("group", "final"),
        ]
    )
    result = await observe_evidence_group(
        evidence(), observe_one=observe, forget_identity=Mock()
    )
    assert result.consumed_quantity == Decimal("4")
