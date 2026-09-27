"""Independent acceptance of nonzero checkpoint-only position recovery."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    PositionKey,
)


@pytest.mark.parametrize("position_side", list(FuturesPositionSide))
def test_checkpoint_suffix_preserves_nonzero_batch_without_prefix(position_side):
    key = PositionKey(
        environment="live",
        account_label="checkpoint-test",
        symbol="BTCUSDT",
        position_side=position_side,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trades", stream_epoch="one"
    )
    start = datetime(2026, 9, 27, tzinfo=UTC)
    short = position_side == FuturesPositionSide.SHORT

    def fill(identity, quantity, price, at, reduce=False):
        side = "BUY" if short == reduce else "SELL"
        return AccountFillEvent(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            trade_id=identity,
            order_id=identity,
            side=side,
            quantity=Decimal(quantity),
            price=Decimal(price),
            realized_pnl=Decimal(0),
            fee=Decimal(0),
            fee_asset="USDT",
            trade_at=at,
            raw_payload={"positionSide": position_side.value, "is_system": True},
        )

    opening = fill("open", "10", "100", start)
    reducing = fill("reduce", "3", "110", start + timedelta(seconds=1), True)
    ledger = PositionLedger(key)
    prefix = AccountFacts(position_key=key, stream_scope=scope, fills=(opening,))
    checkpoint = ledger.create_recovery_checkpoint(
        prefix, source_revision=1, event_cut=start
    )
    recovered = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(reducing,),
        recovery_checkpoint=checkpoint,
        prefix_facts_complete=False,
    )
    restored = ledger.project(recovered)
    complete = ledger.project(
        AccountFacts(position_key=key, stream_scope=scope, fills=(opening, reducing))
    )
    assert restored.active_episode == complete.active_episode
    assert restored.active_batches == complete.active_batches
    assert restored.total_active_quantity == Decimal("7")
    # Subsequent checkpoints must not require reloading an archived prefix.
    next_checkpoint = ledger.create_recovery_checkpoint(
        recovered,
        source_revision=2,
        event_cut=reducing.trade_at,
    )
    next_projection = ledger.project(
        AccountFacts(
            position_key=key,
            stream_scope=scope,
            recovery_checkpoint=next_checkpoint,
            prefix_facts_complete=False,
        )
    )
    assert next_projection.active_episode == complete.active_episode
