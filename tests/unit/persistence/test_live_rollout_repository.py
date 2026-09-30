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


async def test_operating_state_query_excludes_preflight_and_preserves_unknown() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from sqlalchemy.dialects import postgresql

    from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
        PostgresLiveRolloutRepository,
    )

    session = AsyncMock()
    session.scalar.return_value = "future_state"
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    repository = PostgresLiveRolloutRepository(factory)
    assert await repository.load_latest_operating_state("s1") == "future_state"
    statement = session.scalar.await_args.args[0]
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "'s1'" in compiled
    assert "NOT IN ('preflight', 'shadow_preflight')" in compiled
    assert "occurred_at DESC" in compiled
    assert "LIMIT 1" in compiled
