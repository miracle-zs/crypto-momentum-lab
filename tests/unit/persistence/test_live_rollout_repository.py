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
    assert "state != 'preflight'" in compiled
    assert "occurred_at DESC" in compiled
    assert "LIMIT 1" in compiled


async def test_runtime_transition_publishes_scoped_state_in_same_transaction():
    from unittest.mock import AsyncMock, MagicMock

    from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
        PostgresLiveRolloutRepository,
    )

    session = AsyncMock()
    session.begin = MagicMock()
    session.begin.return_value.__aenter__ = AsyncMock()
    session.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    repository = PostgresLiveRolloutRepository(
        factory, strategy_scope=("live", "account-3", "orderflow_impulse")
    )
    transition = LiveSessionTransition(
        "t",
        "run-3",
        LiveSessionState.LIVE_ENABLED,
        datetime(2026, 10, 1, tzinfo=UTC),
        "operator",
        "a" * 64,
        "b" * 64,
        None,
        {},
    )
    await repository.save_transition(transition)
    statements = [call.args[0] for call in session.execute.await_args_list]
    assert [statement.table.name for statement in statements] == [
        "live_session_transitions",
        "strategy_live_states",
    ]
    state = statements[1].compile().params
    assert state["account_label"] == "account-3"
    assert state["strategy_name"] == "orderflow_impulse"
    assert state["state"] == "active"
    assert session.begin.call_count == 1
