from datetime import UTC, datetime

from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    _prepare_transition_values,
)


def test_transition_reason_is_bounded_without_losing_full_diagnostic() -> None:
    reason = "x" * 256
    transition = LiveSessionTransition(
        transition_id="transition-1",
        session_id="live-1",
        state=LiveSessionState.HALTED,
        occurred_at=datetime(2026, 8, 21, tzinfo=UTC),
        operator="operator",
        strategy_config_hash="a" * 64,
        risk_config_hash="b" * 64,
        reason=reason,
        details={},
    )

    values = _prepare_transition_values(transition)

    assert len(values["reason"]) == 128
    assert values["reason"].endswith("...")
    assert values["details"] == {"full_reason": reason}
