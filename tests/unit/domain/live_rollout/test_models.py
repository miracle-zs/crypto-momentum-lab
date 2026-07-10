from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.live_rollout import LiveOperatorApproval


def test_live_approval_requires_exact_confirmation_phrase() -> None:
    now = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="confirmation phrase"):
        LiveOperatorApproval(
            approval_id="approval-1",
            account_label="primary",
            strategy_name="compression_breakout",
            strategy_config_hash="a" * 64,
            risk_config_hash="b" * 64,
            git_commit_hash="abc123",
            database_migration_revision="20260704_0010",
            approved_notional_cap=Decimal("25"),
            approved_max_open_positions=1,
            approved_max_daily_loss=Decimal("10"),
            approver_name="operator",
            approval_text="yes",
            expires_at=now + timedelta(hours=1),
            created_at=now,
        )
