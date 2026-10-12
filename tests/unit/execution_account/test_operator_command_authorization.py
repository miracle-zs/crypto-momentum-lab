"""Persisted operator commands require the correct operation and confirmation."""

from __future__ import annotations

from datetime import UTC

import pytest

from crypto_momentum_lab.domain.live_rollout.authorization import (
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)
from crypto_momentum_lab.domain.live_rollout.models import RollbackCommand

pytestmark = pytest.mark.contract


def test_domain_authorization_require_authorized_command() -> None:
    """Verify authorization domain contract validates operator commands correctly."""
    from datetime import datetime

    cmd = RollbackCommand(
        command_id="cmd-1",
        command_type="emergency_flatten",
        requested_by="operator-1",
        confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        requested_at=datetime.now(UTC),
        idempotency_key="idemp-1",
        account_label="binance-prod",
        strategy_name="top_momentum",
        session_id="session-1",
        status="requested",
        completed_at=None,
        failure_reason=None,
    )

    validated = require_authorized_command(
        cmd,
        command_type="emergency_flatten",
        confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
    )
    assert validated.command_id == "cmd-1"

    # None command
    with pytest.raises(PermissionError, match="persisted operator command is required"):
        require_authorized_command(
            None,
            command_type="emergency_flatten",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )

    # Wrong command_type
    with pytest.raises(PermissionError, match="operator command type mismatch"):
        require_authorized_command(
            cmd,
            command_type="cancel_all_open_entries",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )

    # Wrong confirmation
    with pytest.raises(PermissionError, match="operator command confirmation mismatch"):
        require_authorized_command(
            cmd,
            command_type="emergency_flatten",
            confirmation_text="WRONG CONFIRMATION",
        )
