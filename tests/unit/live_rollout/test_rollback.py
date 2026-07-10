from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.live_rollout.rollback import (
    FlatReconciliationState,
    draining_allows_intent,
    require_flat_before_lease_release,
)


def test_disable_new_entries_allows_reduce_only_orders() -> None:
    assert draining_allows_intent(_intent(reduce_only=True)) is True
    assert draining_allows_intent(_intent(reduce_only=False)) is False


def test_reconcile_until_flat_before_releasing_lease() -> None:
    state = FlatReconciliationState(1, 1, 0, 0, 0)

    with pytest.raises(RuntimeError, match="before local and exchange state are flat"):
        require_flat_before_lease_release(state)


def _intent(*, reduce_only: bool) -> OrderIntentCandidate:
    now = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("25"),
        reduce_only=reduce_only,
        expires_at=now + timedelta(seconds=30),
        created_at=now,
        reason="rollback",
        features={},
    )
