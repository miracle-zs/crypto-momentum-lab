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
        SimpleNamespace(reduce_only=False),  # type: ignore[arg-type]
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
            SimpleNamespace(reduce_only=False),  # type: ignore[arg-type]
            datetime(2026, 8, 4, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_submission_fence_allows_reduce_only_without_control_reads() -> None:
    class RiskState:
        async def load_active_lease(self, *_args):
            raise AssertionError("reduce-only should bypass entry fencing")

        async def load_active_halts(self, *_args):
            raise AssertionError("reduce-only should bypass entry fencing")

    fence = LiveSubmissionFence(
        risk_state=RiskState(),  # type: ignore[arg-type]
        environment="live",
        account_label="account-1",
        strategy_name="strategy-1",
        lease_owner="worker-1",
        code_generation="commit-1",
        entry_enabled=lambda: False,
    )

    await fence.validate(
        SimpleNamespace(reduce_only=True),  # type: ignore[arg-type]
        datetime(2026, 8, 4, tzinfo=UTC),
    )
