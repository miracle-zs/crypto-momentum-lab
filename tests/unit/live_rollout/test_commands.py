from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.live_rollout import RollbackCommand
from crypto_momentum_lab.live_rollout.commands import (
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)


def test_emergency_flatten_requires_explicit_confirmation() -> None:
    command = _command(confirmation_text="wrong")

    with pytest.raises(PermissionError, match="confirmation"):
        require_authorized_command(
            command,
            command_type="emergency_flatten",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )


def _command(confirmation_text: str) -> RollbackCommand:
    return RollbackCommand(
        command_id="command-1",
        command_type="emergency_flatten",
        requested_by="operator",
        confirmation_text=confirmation_text,
        requested_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        idempotency_key="flatten-live-1",
        account_label="primary",
        strategy_name="compression_breakout",
        session_id="live-1",
        status="requested",
        completed_at=None,
        failure_reason=None,
    )
