from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.live_rollout.submission_fence import LiveSubmissionFence


@pytest.mark.asyncio
async def test_submission_fence_accepts_matching_entry_state() -> None:
    checked_at = datetime(2026, 8, 4, tzinfo=UTC)
    lease = SimpleNamespace(
        lease_id="lease-1",
        owner="worker-1",
        strategy_name="strategy-1",
        code_generation="commit-1",
    )
    calls: list[tuple[str, str]] = []

    class RiskState:
        async def load_active_lease(
            self, environment: str, account_label: str, now: datetime
        ) -> Any:
            assert now == checked_at
            calls.append((environment, account_label))
            return lease

        async def load_active_halts(
            self, environment: str, account_label: str
        ) -> tuple[Any, ...]:
            calls.append((environment, account_label))
            return ()

    fence = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        active_lease=cast(Any, lambda: lease),
    )

    await fence.validate(
        cast(Any, SimpleNamespace(reduce_only=False, client_order_id="ord-1")),
        checked_at,
    )

    assert calls == [("live", "account-1"), ("live", "account-1")]


@pytest.mark.asyncio
async def test_submission_fence_blocks_entry_when_entry_lane_is_disabled() -> None:
    class RiskState:
        async def load_active_lease(self, *args: Any) -> Any:
            raise AssertionError("durable state should not be read")

        async def load_active_halts(self, *args: Any) -> tuple[Any, ...]:
            raise AssertionError("durable state should not be read")

    fence = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: False,
    )

    with pytest.raises(OrderPreSubmissionError, match="entry lane is disabled"):
        await fence.validate(
            cast(Any, SimpleNamespace(reduce_only=False, client_order_id="ord-1")),
            datetime(2026, 8, 4, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_submission_fence_allows_reduce_only_with_valid_lease() -> None:
    """Reduce-only bypasses entry lane and active halts,
    but requires valid writer lease.
    """
    checked_at = datetime(2026, 8, 4, tzinfo=UTC)
    lease = SimpleNamespace(
        lease_id="lease-1",
        owner="worker-1",
        strategy_name="strategy-1",
        code_generation="commit-1",
    )

    class RiskState:
        async def load_active_lease(
            self, environment: str, account_label: str, now: datetime
        ) -> Any:
            return lease

        async def load_active_halts(self, *args: Any) -> tuple[Any, ...]:
            raise AssertionError("halts should not block reduce-only")

    fence = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: False,  # Disabled entry lane bypassed
    )

    await fence.validate(
        cast(Any, SimpleNamespace(reduce_only=True, client_order_id="exit-ord-1")),
        checked_at,
    )


@pytest.mark.asyncio
async def test_submission_fence_blocks_reduce_only_when_lease_owner_changed() -> None:
    """Zombie/stale executor cannot issue reduce_only orders after lease handover."""
    checked_at = datetime(2026, 8, 4, tzinfo=UTC)
    foreign_lease = SimpleNamespace(
        lease_id="lease-2",
        owner="worker-other",
        strategy_name="strategy-1",
        code_generation="commit-1",
    )

    class RiskState:
        async def load_active_lease(
            self, environment: str, account_label: str, now: datetime
        ) -> Any:
            return foreign_lease

        async def load_active_halts(self, *args: Any) -> tuple[Any, ...]:
            return ()

    fence = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: True,
    )

    with pytest.raises(OrderPreSubmissionError, match="active lease owner changed"):
        await fence.validate(
            cast(Any, SimpleNamespace(reduce_only=True, client_order_id="exit-ord-1")),
            checked_at,
        )


@pytest.mark.asyncio
async def test_submission_fence_rejects_empty_client_order_id() -> None:
    """All outbound exchange actions require a non-empty client_order_id identity."""
    fence = LiveSubmissionFence(
        risk_state=cast(Any, None),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
    )

    with pytest.raises(
        OrderPreSubmissionError, match="client_order_id must not be empty"
    ):
        await fence.validate(
            cast(Any, SimpleNamespace(reduce_only=True, client_order_id="   ")),
            datetime(2026, 8, 4, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_submission_fence_blocks_entry_on_evaluator_reject() -> None:
    from crypto_momentum_lab.domain.runtime import (
        CapabilityEvaluator,
        CapabilityEvidence,
        RuntimePlanCompiler,
    )

    checked_at = datetime(2026, 8, 4, tzinfo=UTC)
    lease = SimpleNamespace(
        lease_id="lease-1",
        owner="worker-1",
        strategy_name="strategy-1",
        code_generation="commit-1",
    )

    class RiskState:
        async def load_active_lease(self, *args: Any) -> Any:
            return lease

        async def load_active_halts(self, *args: Any) -> tuple[Any, ...]:
            return ()

    plan = RuntimePlanCompiler.compile(
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        git_commit="commit-1",
        schema_version="20260925_0043",
        runtime_generation="commit-1",
    )
    evaluator = CapabilityEvaluator(max_entry_market_age_seconds=15.0)

    # 1. Market stale for entry (>15s)
    stale_evidence = CapabilityEvidence(
        evidence_version="ev-stale",
        market_freshness_seconds=20.0,
        is_account_concordant=True,
        is_approval_valid=True,
    )
    fence = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        capability_evaluator=evaluator,
        runtime_plan=plan,
        evidence_provider=lambda _p, _c: stale_evidence,
    )
    with pytest.raises(
        OrderPreSubmissionError,
        match="capability_evaluator_blocked: market_data_stale_for_entry",
    ):
        await fence.validate(
            cast(
                Any,
                SimpleNamespace(reduce_only=False, client_order_id="ord-entry-1"),
            ),
            checked_at,
        )


@pytest.mark.asyncio
async def test_submission_fence_blocks_exit_on_discordance() -> None:
    from crypto_momentum_lab.domain.runtime import (
        CapabilityEvaluator,
        CapabilityEvidence,
        RuntimePlanCompiler,
    )

    checked_at = datetime(2026, 8, 4, tzinfo=UTC)
    lease = SimpleNamespace(
        lease_id="lease-1",
        owner="worker-1",
        strategy_name="strategy-1",
        code_generation="commit-1",
    )

    class RiskState:
        async def load_active_lease(self, *args: Any) -> Any:
            return lease

        async def load_active_halts(self, *args: Any) -> tuple[Any, ...]:
            return ()

    plan = RuntimePlanCompiler.compile(
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        git_commit="commit-1",
        schema_version="20260925_0043",
        runtime_generation="commit-1",
    )
    evaluator = CapabilityEvaluator(max_exit_market_age_seconds=60.0)

    # Batch conflict/gap on exit
    discordant_evidence = CapabilityEvidence(
        evidence_version="ev-discordant",
        market_freshness_seconds=5.0,
        is_account_concordant=False,
    )
    fence = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        capability_evaluator=evaluator,
        runtime_plan=plan,
        evidence_provider=lambda _p, _c: discordant_evidence,
    )
    with pytest.raises(
        OrderPreSubmissionError,
        match="capability_evaluator_blocked: batch_attribution_conflict_or_gap",
    ):
        await fence.validate(
            cast(Any, SimpleNamespace(reduce_only=True, client_order_id="ord-exit-1")),
            checked_at,
        )

    # Valid exit succeeds
    valid_exit_evidence = CapabilityEvidence(
        evidence_version="ev-valid-exit",
        market_freshness_seconds=5.0,
        is_account_concordant=True,
        unresolved_inflight_orders_count=0,
        is_approval_valid=False,  # Does not block exit
    )
    fence_valid = LiveSubmissionFence(
        risk_state=cast(Any, RiskState()),
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        capability_evaluator=evaluator,
        runtime_plan=plan,
        evidence_provider=lambda _p, _c: valid_exit_evidence,
    )
    await fence_valid.validate(
        cast(Any, SimpleNamespace(reduce_only=True, client_order_id="ord-exit-2")),
        checked_at,
    )
