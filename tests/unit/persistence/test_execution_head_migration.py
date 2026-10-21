from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionLedgerBatch,
    PositionKey,
)
from crypto_momentum_lab.domain.trading import TradeSide
from crypto_momentum_lab.persistence.postgres import (
    execution_head_migration_projection,
)
from crypto_momentum_lab.persistence.postgres.execution_head_migration_projection import (
    AccountFactStreamScope,
    LegacyPositionEpisode,
)
from crypto_momentum_lab.persistence.postgres.execution_head_migration import (
    convert_legacy_execution_head,
)
from crypto_momentum_lab.persistence.postgres.execution_head_migration_codec import (
    PositionRecoveryCodec,
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
    archived_episode = LegacyPositionEpisode(
        episode_id="episode_1",
        position_key=key,
        side=TradeSide.LONG,
        opened_at=START + timedelta(seconds=1),
        closed_at=START + timedelta(seconds=2),
        is_active=False,
        cumulative_bought=Decimal("1"),
        cumulative_sold=Decimal("1"),
        peak_quantity=Decimal("1"),
        batches=(
            PositionLedgerBatch(
                batch_id="episode_1_b1",
                episode_id="episode_1",
                quantity=Decimal("0"),
                original_quantity=Decimal("1"),
                entry_price=Decimal("100"),
                opened_at=START + timedelta(seconds=1),
            ),
        ),
    )
    active_episode = LegacyPositionEpisode(
        episode_id="episode_2",
        position_key=key,
        side=TradeSide.LONG,
        opened_at=START + timedelta(seconds=3),
        cumulative_bought=Decimal("1"),
        peak_quantity=Decimal("1"),
        batches=(
            PositionLedgerBatch(
                batch_id="episode_2_b1",
                episode_id="episode_2",
                quantity=Decimal("1"),
                original_quantity=Decimal("1"),
                entry_price=Decimal("100"),
                opened_at=START + timedelta(seconds=3),
                order_id="order-entry-2",
                entry_order_ids=("order-entry-2",),
            ),
        ),
    )
    projection = execution_head_migration_projection.LegacyPositionProjection(
        position_key=key,
        active_episode=active_episode,
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=START + timedelta(seconds=3),
        archived_episodes=(archived_episode,),
        stream_scope=scope,
    )
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
        "position_key": execution_head_migration_projection.encode_position_key(
            key
        ),
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
    migrated_projection = PositionRecoveryCodec.decode_projection(
        migrated["position_state"]["projection"]
    )
    verification = migrated["position_state"]["verification"]

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
    assert migrated_projection.position_key == key
    assert migrated_projection.stream_scope == scope
    assert migrated["position_state"]["episode_sequence"] == 2
    assert migrated["position_state"]["batch_sequence"] == 1
    assert verification["fills"] == []
    assert [
        PositionRecoveryCodec.decode_snapshot(item)
        for item in verification["snapshots"]
    ] == [snapshot]
    assert migrated["active_reservation_ids"] == ["res-active"]
    assert "active_fills" not in migrated
