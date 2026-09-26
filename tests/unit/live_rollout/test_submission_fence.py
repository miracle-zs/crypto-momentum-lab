from datetime import UTC, datetime
from types import SimpleNamespace

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
        async def load_active_lease(self, environment, account_label, now):
            assert now == checked_at
            calls.append((environment, account_label))
            return lease

        async def load_active_halts(self, environment, account_label):
            calls.append((environment, account_label))
            return ()

    fence = LiveSubmissionFence(
        risk_state=RiskState(),  # type: ignore[arg-type]
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        active_lease=lambda: lease,
    )

    await fence.validate(
        SimpleNamespace(reduce_only=False, client_order_id="ord-1"),  # type: ignore[arg-type]
        checked_at,
    )

    assert calls == [("live", "account-1"), ("live", "account-1")]


@pytest.mark.asyncio
async def test_submission_fence_blocks_entry_when_entry_lane_is_disabled() -> None:
    class RiskState:
        async def load_active_lease(self, *_args):
            raise AssertionError("durable state should not be read")

        async def load_active_halts(self, *_args):
            raise AssertionError("durable state should not be read")

    fence = LiveSubmissionFence(
        risk_state=RiskState(),  # type: ignore[arg-type]
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: False,
    )

    with pytest.raises(OrderPreSubmissionError, match="entry lane is disabled"):
        await fence.validate(
            SimpleNamespace(reduce_only=False, client_order_id="ord-1"),  # type: ignore[arg-type]
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
        async def load_active_lease(self, environment, account_label, now):
            return lease

        async def load_active_halts(self, *_args):
            raise AssertionError("halts should not block reduce-only")

    fence = LiveSubmissionFence(
        risk_state=RiskState(),  # type: ignore[arg-type]
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: False,  # Disabled entry lane bypassed
    )

    await fence.validate(
        SimpleNamespace(reduce_only=True, client_order_id="exit-ord-1"),  # type: ignore[arg-type]
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
        async def load_active_lease(self, environment, account_label, now):
            return foreign_lease

        async def load_active_halts(self, *_args):
            return ()

    fence = LiveSubmissionFence(
        risk_state=RiskState(),  # type: ignore[arg-type]
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: True,
    )

    with pytest.raises(OrderPreSubmissionError, match="active lease owner changed"):
        await fence.validate(
            SimpleNamespace(reduce_only=True, client_order_id="exit-ord-1"),  # type: ignore[arg-type]
            checked_at,
        )


@pytest.mark.asyncio
async def test_submission_fence_rejects_empty_client_order_id() -> None:
    """All outbound exchange actions require a non-empty client_order_id identity."""
    fence = LiveSubmissionFence(
        risk_state=None,  # type: ignore[arg-type]
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
            SimpleNamespace(reduce_only=True, client_order_id="   "),  # type: ignore[arg-type]
            datetime(2026, 8, 4, tzinfo=UTC),
        )
