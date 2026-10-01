"""Load source anchors without deriving completeness from polling cursors."""

from typing import cast as typing_cast

from sqlalchemy import Integer, cast, func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account.models import (
    AccountFillSourceAnchor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import AccountPositionSnapshotRow
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionRecoveryCheckpointRow,
)


async def load_fill_recovery_sources(
    sessions: async_sessionmaker[AsyncSession], *, environment: str, account_label: str
) -> dict[tuple[str, str], AccountFillSourceAnchor | None]:
    """Use a verified current head checkpoint, otherwise an explicit flat row.

    Empty historical symbols need no private REST scan. A durable Book with
    real trade history must get a source-backed baseline even if now flat.
    """
    async with sessions() as session:
        heads = (
            await session.scalars(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == environment,
                    ExecutionBookHeadRow.account_label == account_label,
                    or_(
                        cast(
                            ExecutionBookHeadRow.state_payload[
                                "seen_trade_count"
                            ].astext,
                            Integer,
                        )
                        > 0,
                        ExecutionBookHeadRow.state_payload["recovery_checkpoint"][
                            "checkpoint_id"
                        ].astext.is_not(None),
                    ),
                )
            )
        ).all()
        if not heads:
            return {}
        keys = [(head.symbol, head.position_side) for head in heads]
        bindings = []
        for head in heads:
            binding = head.state_payload.get("recovery_checkpoint")
            if isinstance(binding, dict) and binding.get("checkpoint_id"):
                bindings.append(
                    (
                        head.symbol,
                        head.position_side,
                        head.stream_id,
                        head.stream_epoch,
                        binding["checkpoint_id"],
                    )
                )
        checkpoints = (
            (
                await session.scalars(
                    select(PositionRecoveryCheckpointRow).where(
                        PositionRecoveryCheckpointRow.environment == environment,
                        PositionRecoveryCheckpointRow.account_label == account_label,
                        tuple_(
                            PositionRecoveryCheckpointRow.symbol,
                            PositionRecoveryCheckpointRow.position_side,
                            PositionRecoveryCheckpointRow.stream_id,
                            PositionRecoveryCheckpointRow.stream_epoch,
                            PositionRecoveryCheckpointRow.checkpoint_id,
                        ).in_(bindings),
                    )
                )
            ).all()
            if bindings
            else []
        )
        flat_rows = (
            await session.scalars(
                select(AccountPositionSnapshotRow)
                .where(
                    AccountPositionSnapshotRow.environment == environment,
                    AccountPositionSnapshotRow.account_label == account_label,
                    AccountPositionSnapshotRow.position_amt == 0,
                    func.mod(
                        func.extract(
                            "microseconds", AccountPositionSnapshotRow.observed_at
                        ),
                        1000,
                    )
                    == 0,
                    tuple_(
                        AccountPositionSnapshotRow.symbol,
                        AccountPositionSnapshotRow.position_side,
                    ).in_(keys),
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
    heads_by_key = {(head.symbol, head.position_side): head for head in heads}
    # Preserve the exact heads needing recovery even when no trusted durable
    # anchor exists. None requests a real prior REST zero, never a synthetic cut.
    result: dict[tuple[str, str], AccountFillSourceAnchor | None] = {
        key: None for key in heads_by_key
    }
    for row in checkpoints:
        key = (row.symbol, row.position_side)
        head = heads_by_key[key]
        if result.get(key) is not None or (row.stream_id, row.stream_epoch) != (
            head.stream_id,
            head.stream_epoch,
        ):
            continue
        checkpoint = PositionRecoveryCodec.decode_checkpoint(row.payload)
        binding = head.state_payload.get("recovery_checkpoint")
        if (
            not isinstance(binding, dict)
            or recovery_checkpoint_head_binding(checkpoint) != binding
            or checkpoint.key.environment != environment
            or checkpoint.key.account_label != account_label
            or (checkpoint.key.symbol, checkpoint.key.position_side.value) != key
        ):
            continue
        if (
            checkpoint.coverage is None
            or not checkpoint.coverage.is_authoritative
            or checkpoint.has_conflicts
            or checkpoint.has_synthetic_fills
            or checkpoint.has_late_events
            or checkpoint.integrity_issues
        ):
            continue
        result[key] = AccountFillSourceAnchor(
            row.symbol,
            row.position_side,
            checkpoint.checkpoint_id,
            checkpoint.event_cut,
            row.stream_id,
            row.stream_epoch,
        )
    for flat_row in flat_rows:
        key = (flat_row.symbol, flat_row.position_side)
        if result.get(key) is not None:
            continue
        snapshot = AccountPositionSnapshot(
            flat_row.environment,
            flat_row.account_label,
            flat_row.symbol,
            flat_row.position_side,
            flat_row.position_amt,
            flat_row.entry_price,
            flat_row.mark_price,
            flat_row.unrealized_pnl,
            flat_row.notional,
            flat_row.leverage,
            flat_row.margin_type,
            flat_row.observed_at,
            typing_cast(dict[str, JsonValue], flat_row.raw_payload),
        )
        result[key] = AccountFillSourceAnchor(
            flat_row.symbol,
            flat_row.position_side,
            stable_snapshot_anchor_id(snapshot),
            snapshot.observed_at,
            "exchange_snapshot",
            str(flat_row.snapshot_id),
            zero_snapshot=snapshot,
        )
    return result
