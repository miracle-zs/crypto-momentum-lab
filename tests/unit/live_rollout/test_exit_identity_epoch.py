"""Grace fallback must mint a new client identity after a durable conflict."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import FuturesPositionSide
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitMode
from tests.unit.live_rollout.test_exits import _config, _long_position
from tests.unit.shadow_operation.test_service import _state


def test_grace_timeout_identity_changes_after_conflict_epoch() -> None:
    manager = LiveExitManager(config=_config(PositionExitMode.CANDLE_15M))
    opened = datetime(2026, 9, 16, 20, 0, tzinfo=UTC)
    recovery = opened + timedelta(hours=1)
    position = replace(
        _long_position(),
        symbol="龙虾USDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("266"),
        opened_at=opened,
        batch_id="batch-1",
        recovery_exit_started_at=recovery,
    )
    state = replace(
        _state(),
        symbol="龙虾USDT",
        bucket_start=recovery + timedelta(minutes=20),
        bucket_end=recovery + timedelta(minutes=20, seconds=15),
    )

    def build(trigger_offset: timedelta):
        return manager._build_order_request(
            state=state,
            position=position,
            reason="candle_15m_grace_timeout_8",
            trigger_at=state.bucket_end + trigger_offset,
            identity_trigger_at=recovery,
            reference_price=Decimal("0.23"),
            quantity=Decimal("266"),
        )

    first = build(timedelta(0))
    retry_same_epoch = build(timedelta(seconds=15))
    assert retry_same_epoch.candidate.candidate_id == first.candidate.candidate_id

    manager.note_order_identity_conflict("龙虾USDT")
    after_conflict = build(timedelta(seconds=30))
    assert after_conflict.candidate.candidate_id != first.candidate.candidate_id
    assert after_conflict.candidate.symbol == first.candidate.symbol
    assert after_conflict.quantity == first.quantity
