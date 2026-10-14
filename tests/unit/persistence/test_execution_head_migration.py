from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.persistence.postgres.execution_head_migration import (
    convert_legacy_execution_head,
)

START = datetime(2026, 10, 1, tzinfo=UTC)


def _fill(trade_id: str, side: str, second: int) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id=trade_id,
        order_id=f"order-{trade_id}",
        side=side,
        price=Decimal("100"),
        quantity=Decimal("1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=START + timedelta(seconds=second),
        raw_payload={"positionSide": "LONG"},
    )


def test_legacy_head_migration_keeps_only_the_new_current_state() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trade-source", stream_epoch="epoch-1"
    )
    fills = (
        _fill("entry-1", "BUY", 1),
        _fill("exit-1", "SELL", 2),
        _fill("entry-2", "BUY", 3),
    )
    facts = AccountFacts(position_key=key, fills=fills, stream_scope=scope)
    projection = PositionLedger(key).project(facts)
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("1"),
        entry_price=Decimal("100"),
        mark_price=Decimal("101"),
        unrealized_pnl=Decimal("1"),
        notional=Decimal("101"),
        leverage=3,
        margin_type="isolated",
        observed_at=START + timedelta(seconds=4),
        raw_payload={"positionSide": "LONG"},
    )
    legacy = {
        "schema_version": 1,
        "position_key": encode_position_key(key),
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "projection": PositionRecoveryCodec.encode_projection(projection),
        "active_fills": [PositionRecoveryCodec.encode_fill(item) for item in fills],
        "active_snapshots": [PositionRecoveryCodec.encode_snapshot(snapshot)],
        "active_exit_boundaries": [],
        "coverage": None,
        "last_sequence": 9,
        "active_reservation_ids": ["res-active"],
        "external_recovery_ids": ["external-1"],
        "recovery_command_ids": ["command-1"],
    }

    migrated = convert_legacy_execution_head(legacy)
    restored = decode_execution_head_state(migrated, key=key, scope=scope)
    migrated_projection = PositionRecoveryCodec.decode_projection(
        migrated["position_state"]["projection"]
    )
    verification = PositionRecoveryCodec.decode_facts(
        migrated["position_state"]["verification"]
    )

    assert set(migrated) == {
        "schema_version",
        "position_key",
        "stream_scope",
        "position_state",
        "last_sequence",
        "active_reservation_ids",
        "external_recovery_ids",
        "recovery_command_ids",
    }
    assert migrated["schema_version"] == 4
    assert migrated_projection.archived_episodes == ()
    assert migrated_projection.total_active_quantity == projection.total_active_quantity
    assert migrated["position_state"]["episode_sequence"] == 2
    assert migrated["position_state"]["batch_sequence"] == 1
    assert verification.fills == ()
    assert verification.snapshots == (snapshot,)
    assert restored.projection.total_active_quantity == projection.total_active_quantity
    assert migrated["active_reservation_ids"] == ["res-active"]
    assert "active_fills" not in migrated
