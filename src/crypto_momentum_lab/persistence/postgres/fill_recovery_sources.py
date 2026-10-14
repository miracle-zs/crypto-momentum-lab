"""Load fill-scan anchors from the current head and raw exchange snapshots."""

from typing import cast

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account.models import (
    AccountFillSourceAnchor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
    is_supported_execution_head_schema,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    PositionRecoveryCheckpoint,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import AccountPositionSnapshotRow


async def load_fill_recovery_sources(
    sessions: async_sessionmaker[AsyncSession],
    *,
    environment: str,
    account_label: str,
    position_keys: tuple[tuple[str, str], ...] | None = None,
) -> dict[tuple[str, str], AccountFillSourceAnchor | None]:
    """Use the current head checkpoint or a real flat exchange observation."""
    statement = select(ExecutionBookHeadRow).where(
        ExecutionBookHeadRow.environment == environment,
        ExecutionBookHeadRow.account_label == account_label,
    )
    if position_keys is not None:
        if not position_keys:
            return {}
        statement = statement.where(
            tuple_(ExecutionBookHeadRow.symbol, ExecutionBookHeadRow.position_side).in_(
                position_keys
            )
        )

    async with sessions() as session:
        heads = (await session.scalars(statement)).all()
        result: dict[tuple[str, str], AccountFillSourceAnchor | None] = {
            (symbol, side): None for symbol, side in (position_keys or ())
        }
        for head in heads:
            payload = head.state_payload or {}
            if not is_supported_execution_head_schema(payload.get("schema_version")):
                if position_keys is None:
                    continue
                raise ValueError(
                    f"execution head {head.symbol}:{head.position_side} "
                    "requires explicit format migration"
                )
            key = (head.symbol, head.position_side)
            position_key = PositionKey(
                environment,
                account_label,
                head.symbol,
                FuturesPositionSide(head.position_side),
            )
            scope = AccountFactStreamScope.for_position_key(
                position_key,
                stream_id=head.stream_id,
                stream_epoch=head.stream_epoch,
            )
            try:
                facts = decode_execution_head_state(
                    payload, key=position_key, scope=scope
                ).current_facts
            except Exception as err:
                raise ValueError(
                    f"execution head {head.symbol}:{head.position_side} is invalid"
                ) from err
            result.setdefault(key, None)

            checkpoint = facts.recovery_checkpoint
            if _checkpoint_is_authoritative(
                checkpoint, environment, account_label, key
            ):
                assert checkpoint is not None
                result[key] = AccountFillSourceAnchor(
                    key[0],
                    key[1],
                    checkpoint.checkpoint_id,
                    checkpoint.event_cut,
                    checkpoint.stream_scope.stream_id,
                    checkpoint.stream_scope.stream_epoch,
                )
                continue

            flat_snapshots = [
                snapshot
                for snapshot in facts.snapshots
                if snapshot.position_amt == 0
                and (snapshot.symbol, snapshot.position_side.upper()) == key
            ]
            if flat_snapshots:
                snapshot = max(flat_snapshots, key=lambda item: item.observed_at)
                result[key] = _snapshot_anchor(snapshot)

        raw_keys = [key for key, anchor in result.items() if anchor is None]
        if raw_keys:
            flat_rows = (
                await session.scalars(
                    select(AccountPositionSnapshotRow)
                    .where(
                        AccountPositionSnapshotRow.environment == environment,
                        AccountPositionSnapshotRow.account_label == account_label,
                        AccountPositionSnapshotRow.position_amt == 0,
                        tuple_(
                            AccountPositionSnapshotRow.symbol,
                            AccountPositionSnapshotRow.position_side,
                        ).in_(raw_keys),
                    )
                    .distinct(
                        AccountPositionSnapshotRow.symbol,
                        AccountPositionSnapshotRow.position_side,
                    )
                    .order_by(
                        AccountPositionSnapshotRow.symbol,
                        AccountPositionSnapshotRow.position_side,
                        AccountPositionSnapshotRow.observed_at.desc(),
                    )
                )
            ).all()
            for row in flat_rows:
                key = (row.symbol, row.position_side)
                if result.get(key) is not None:
                    continue
                snapshot = AccountPositionSnapshot(
                    row.environment,
                    row.account_label,
                    row.symbol,
                    row.position_side,
                    row.position_amt,
                    row.entry_price,
                    row.mark_price,
                    row.unrealized_pnl,
                    row.notional,
                    row.leverage,
                    row.margin_type,
                    row.observed_at,
                    cast(dict[str, JsonValue], row.raw_payload),
                )
                result[key] = _snapshot_anchor(snapshot)
    return result


def _snapshot_anchor(snapshot: AccountPositionSnapshot) -> AccountFillSourceAnchor:
    anchor_id = stable_snapshot_anchor_id(snapshot)
    return AccountFillSourceAnchor(
        snapshot.symbol,
        snapshot.position_side,
        anchor_id,
        snapshot.observed_at,
        "exchange_snapshot",
        anchor_id,
        zero_snapshot=snapshot,
    )


def _checkpoint_is_authoritative(
    checkpoint: PositionRecoveryCheckpoint | None,
    environment: str,
    account_label: str,
    key: tuple[str, str],
) -> bool:
    if checkpoint is None:
        return False
    return (
        checkpoint.key.environment == environment
        and checkpoint.key.account_label == account_label
        and (checkpoint.key.symbol, checkpoint.key.position_side.value) == key
        and checkpoint.coverage is not None
        and checkpoint.coverage.is_authoritative
        and not checkpoint.has_conflicts
        and not checkpoint.has_synthetic_fills
        and not checkpoint.has_late_events
        and not checkpoint.integrity_issues
    )
