"""Session-bound durable account fact journal and recovery checkpoint store."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import replace
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
    extract_fill_position_side,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactConflict,
    AccountFacts,
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    JournalFactDelta,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import (
    PositionRecoveryCodec,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    DurableJournalCut,
    JournalPersistResult,
    PositionRecoveryCheckpoint,
    RecoverySchemaError,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    AccountFillReconciliationCursorRow,
    AccountPositionSnapshotRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)

LEGACY_STREAM_ID = "legacy-postgres-account"
LEGACY_STREAM_EPOCH = "unversioned"


class JournalFactConflict(RuntimeError):
    """A durable immutable identity was reused with different checkpoint data."""


class PostgresAccountJournalStore:
    """Persists and restores journal facts using only the caller's transaction."""

    async def persist_facts_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        facts: AccountFacts,
        revision: int,
        delta: JournalFactDelta | None = None,
    ) -> JournalPersistResult:
        """Persist facts for one exact stream inside the caller's transaction.

        ``delta`` narrows the append-only rows (fills, snapshots, exit
        boundaries) to the events recorded since the last successful persist.
        State-carrying rows always come from the full ``facts``, so the stored
        facts_state, coverage, checkpoint and provenance stay complete. Passing
        ``None`` keeps the historical full-history write.
        """
        if type(revision) is not int or revision < 0:
            raise ValueError("revision must be a non-negative integer")
        if facts.stream_scope != scope:
            raise ValueError("facts stream scope does not match persistence scope")
        if not scope.matches(facts.position_key):
            raise ValueError("facts position key does not match persistence scope")
        if any(
            type(value) is not bool
            for value in (
                facts.has_synthetic_fills,
                facts.has_late_events,
                facts.prefix_facts_complete,
            )
        ):
            raise ValueError("journal status fields must be booleans")
        if facts.coverage is not None and facts.coverage.stream_scope != scope:
            raise ValueError("coverage stream scope does not match persistence scope")
        if facts.fill_load_provenance is not None:
            if facts.fill_load_provenance.stream_scope != scope:
                raise ValueError(
                    "fill load provenance scope does not match persistence scope"
                )
            await self._validate_load_provenance_continuity_in_session(
                session,
                scope=scope,
                provenance=facts.fill_load_provenance,
            )

        specs = _fact_event_specs(facts, delta=delta)
        inserted = 0
        scope_values = _scope_values(scope)
        chunk_size = 500
        row_dicts: list[dict[str, Any]] = []
        for kind, event_id, occurred_at, payload in specs:
            if occurred_at is not None and (
                occurred_at.tzinfo is None or occurred_at.utcoffset() is None
            ):
                raise ValueError("journal event time must be timezone-aware")
            payload_hash = _json_digest(payload)
            event_record_id = _json_digest(
                [scope.canonical_id, kind, event_id, payload_hash]
            )
            row_dicts.append(
                {
                    "event_record_id": event_record_id,
                    **scope_values,
                    "event_kind": kind,
                    "event_id": event_id,
                    "payload_hash": payload_hash,
                    "source_revision": revision,
                    "occurred_at": occurred_at if occurred_at is not None else func.now(),
                    "payload": payload,
                }
            )

        for offset in range(0, len(row_dicts), chunk_size):
            chunk = row_dicts[offset : offset + chunk_size]
            statement = (
                insert(PositionFactJournalEventRow)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["event_record_id"])
            )
            result = await session.execute(statement)
            inserted += max(result.rowcount or 0, 0)

        if facts.recovery_checkpoint is not None:
            await self.save_checkpoint_in_session(session, facts.recovery_checkpoint)

        duplicate_count = len(specs) - inserted
        conflict_count = len(facts.conflicting_fills) + len(facts.fact_conflicts)
        return JournalPersistResult(
            inserted_count=inserted,
            duplicate_count=duplicate_count,
            conflict_count=conflict_count,
            revision=revision,
        )

    async def _validate_load_provenance_continuity_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        provenance: AccountFillLoadProvenance,
    ) -> None:
        statement = (
            select(PositionFactJournalEventRow)
            .where(
                *_scope_conditions(PositionFactJournalEventRow, scope),
                PositionFactJournalEventRow.event_kind == "fill_load_provenance",
            )
            .order_by(
                PositionFactJournalEventRow.source_revision.desc(),
                PositionFactJournalEventRow.recorded_at.desc(),
                PositionFactJournalEventRow.event_id.desc(),
            )
            .limit(1)
        )
        previous_row = await session.scalar(statement)
        if previous_row is None:
            _validate_new_load_origin(provenance)
            return
        if _json_digest(previous_row.payload) != previous_row.payload_hash:
            raise RecoverySchemaError("stored fill-load provenance checksum mismatch")
        previous = PositionRecoveryCodec.decode_fill_load_provenance(
            previous_row.payload
        )
        if previous.stream_scope != scope:
            raise RecoverySchemaError("stored fill-load provenance scope mismatch")
        if previous == provenance:
            return
        if previous.load_id != provenance.load_id:
            _validate_new_load_origin(provenance)
            return
        if previous.page_exhausted:
            raise JournalFactConflict("completed fill-load scan cannot be resumed")
        if previous.next_from_id is None:
            raise JournalFactConflict(
                "incomplete fill-load scan has no durable resume cursor"
            )
        if provenance.request_from_id != previous.next_from_id:
            raise JournalFactConflict("fill-load resume cursor is discontinuous")
        if (
            provenance.scan_origin_from_id != previous.scan_origin_from_id
            or provenance.scan_origin_start_time_ms
            != previous.scan_origin_start_time_ms
            or provenance.source_anchor_id != previous.source_anchor_id
            or provenance.source_anchor_event_cut != previous.source_anchor_event_cut
            or provenance.source_anchor_kind != previous.source_anchor_kind
            or provenance.observed_at < previous.observed_at
        ):
            raise JournalFactConflict("fill-load resume changed source origin/anchor")

    async def save_checkpoint_in_session(
        self,
        session: AsyncSession,
        checkpoint: PositionRecoveryCheckpoint,
    ) -> None:
        payload = PositionRecoveryCodec.encode_checkpoint(checkpoint)
        payload_hash = _json_digest(payload)
        scope_values = _scope_values(checkpoint.stream_scope)
        statement = (
            insert(PositionRecoveryCheckpointRow)
            .values(
                **scope_values,
                checkpoint_id=checkpoint.checkpoint_id,
                schema_version=checkpoint.schema_version,
                event_cut=checkpoint.event_cut,
                source_revision=checkpoint.source_revision,
                facts_hash=checkpoint.facts_hash,
                payload_hash=payload_hash,
                payload=payload,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    "environment",
                    "account_label",
                    "symbol",
                    "position_side",
                    "stream_id",
                    "stream_epoch",
                    "checkpoint_id",
                ]
            )
        )
        await session.execute(statement)
        row = await session.get(
            PositionRecoveryCheckpointRow,
            (*_scope_identity(checkpoint.stream_scope), checkpoint.checkpoint_id),
        )
        if row is None:
            raise RuntimeError("checkpoint insert was not visible in its transaction")
        if (
            row.payload_hash != payload_hash
            or row.facts_hash != checkpoint.facts_hash
            or row.event_cut != checkpoint.event_cut
            or row.source_revision != checkpoint.source_revision
        ):
            raise JournalFactConflict(
                f"checkpoint id {checkpoint.checkpoint_id} was reused with other data"
            )

    async def load_checkpoint_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> PositionRecoveryCheckpoint | None:
        _require_aware(as_of, "as_of")
        statement = (
            select(PositionRecoveryCheckpointRow)
            .where(
                *_scope_conditions(PositionRecoveryCheckpointRow, scope),
                PositionRecoveryCheckpointRow.event_cut <= as_of,
                PositionRecoveryCheckpointRow.recorded_at <= as_of,
            )
            .order_by(
                PositionRecoveryCheckpointRow.event_cut.desc(),
                PositionRecoveryCheckpointRow.source_revision.desc(),
                PositionRecoveryCheckpointRow.recorded_at.desc(),
                PositionRecoveryCheckpointRow.checkpoint_id.desc(),
            )
        )
        rows = list((await session.scalars(statement)).all())
        if not rows:
            return None
        latest = rows[0]
        same_revision = [
            row
            for row in rows
            if row.event_cut == latest.event_cut
            and row.source_revision == latest.source_revision
        ]
        semantic_payloads: set[str] = set()
        for row in same_revision:
            if _json_digest(row.payload) != row.payload_hash:
                raise RecoverySchemaError("stored checkpoint payload checksum mismatch")
            semantic_payload = dict(row.payload)
            semantic_payload.pop("checkpoint_id", None)
            semantic_payloads.add(_json_digest(semantic_payload))
        if len(semantic_payloads) > 1:
            raise JournalFactConflict(
                "multiple different checkpoints claim the same scope, cut, and revision"
            )
        if _json_digest(latest.payload) != latest.payload_hash:
            raise RecoverySchemaError("stored checkpoint payload checksum mismatch")
        checkpoint = PositionRecoveryCodec.decode_checkpoint(latest.payload)
        if (
            checkpoint.stream_scope != scope
            or checkpoint.checkpoint_id != latest.checkpoint_id
            or checkpoint.schema_version != latest.schema_version
            or checkpoint.event_cut != latest.event_cut
            or checkpoint.source_revision != latest.source_revision
            or checkpoint.facts_hash != latest.facts_hash
        ):
            raise RecoverySchemaError("checkpoint columns disagree with payload")
        if checkpoint.event_cut > as_of:
            raise RecoverySchemaError("stored checkpoint exceeds requested cut")
        return checkpoint

    async def load_checkpoint_by_id_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        checkpoint_id: str,
    ) -> PositionRecoveryCheckpoint | None:
        """Load one exact immutable checkpoint, including a prior stream epoch."""
        if not checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        row = await session.get(
            PositionRecoveryCheckpointRow,
            (*_scope_identity(scope), checkpoint_id),
        )
        if row is None:
            return None
        if _json_digest(row.payload) != row.payload_hash:
            raise RecoverySchemaError("stored checkpoint payload checksum mismatch")
        checkpoint = PositionRecoveryCodec.decode_checkpoint(row.payload)
        if (
            checkpoint.stream_scope != scope
            or checkpoint.checkpoint_id != checkpoint_id
            or checkpoint.schema_version != row.schema_version
            or checkpoint.event_cut != row.event_cut
            or checkpoint.source_revision != row.source_revision
            or checkpoint.facts_hash != row.facts_hash
        ):
            raise RecoverySchemaError("checkpoint columns disagree with payload")
        return checkpoint

    async def load_recovery_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
        include_checkpoint_prefix: bool = False,
    ) -> DurableJournalCut:
        _require_aware(as_of, "as_of")
        if scope.stream_id == LEGACY_STREAM_ID:
            return await self._load_legacy_recovery_in_session(
                session, scope=scope, as_of=as_of
            )

        statement = (
            select(PositionFactJournalEventRow)
            .where(
                *_scope_conditions(PositionFactJournalEventRow, scope),
                PositionFactJournalEventRow.occurred_at <= as_of,
                PositionFactJournalEventRow.recorded_at <= as_of,
            )
            .order_by(
                PositionFactJournalEventRow.recorded_at,
                PositionFactJournalEventRow.event_kind,
                PositionFactJournalEventRow.event_id,
                PositionFactJournalEventRow.payload_hash,
            )
        )
        rows = list((await session.scalars(statement)).all())
        checkpoint = await self.load_checkpoint_in_session(
            session, scope=scope, as_of=as_of
        )
        if checkpoint is not None and not include_checkpoint_prefix:
            rows = [
                row
                for row in rows
                if row.occurred_at > checkpoint.event_cut
                or row.source_revision > checkpoint.source_revision
            ]
        for row in rows:
            if _json_digest(row.payload) != row.payload_hash:
                raise RecoverySchemaError(
                    f"stored {row.event_kind} payload checksum mismatch"
                )
        facts, conflicts, issues, cursor = _facts_from_rows(
            scope=scope,
            rows=rows,
            checkpoint=checkpoint,
            prefix_complete=(checkpoint is None or include_checkpoint_prefix),
        )
        if checkpoint is not None and not include_checkpoint_prefix:
            facts = replace(
                facts,
                coverage=(facts.coverage or checkpoint.coverage),
                fill_load_provenance=(
                    facts.fill_load_provenance or checkpoint.coverage.load_provenance
                    if checkpoint.coverage is not None
                    else facts.fill_load_provenance
                ),
                has_synthetic_fills=(
                    facts.has_synthetic_fills or checkpoint.has_synthetic_fills
                ),
                has_late_events=(facts.has_late_events or checkpoint.has_late_events),
                integrity_issues=tuple(
                    dict.fromkeys(
                        (*checkpoint.integrity_issues, *facts.integrity_issues)
                    )
                ),
                fact_conflicts=(
                    facts.fact_conflicts
                    + (
                        (
                            AccountFactConflict(
                                event_kind="checkpoint",
                                event_id=checkpoint.checkpoint_id,
                                details="checkpoint records unresolved conflicts",
                                event_at=checkpoint.event_cut,
                            ),
                        )
                        if checkpoint.has_conflicts
                        else ()
                    )
                ),
                prefix_facts_complete=False,
            )
        return DurableJournalCut(
            scope=scope,
            as_of=as_of,
            facts=facts,
            checkpoint=checkpoint,
            revision=max(
                max((row.source_revision for row in rows), default=0),
                checkpoint.source_revision if checkpoint is not None else 0,
            ),
            conflicts=conflicts,
            integrity_issues=issues,
            cursor_provenance=cursor,
        )

    async def list_scopes_in_session(
        self,
        session: AsyncSession,
        *,
        environment: str,
        account_label: str,
    ) -> tuple[AccountFactStreamScope, ...]:
        scopes: set[AccountFactStreamScope] = set()
        for model in (
            PositionFactJournalEventRow,
            PositionRecoveryCheckpointRow,
        ):
            statement = select(
                model.environment,
                model.account_label,
                model.symbol,
                model.position_side,
                model.stream_id,
                model.stream_epoch,
            ).where(
                model.environment == environment,
                model.account_label == account_label,
            )
            for row in (await session.execute(statement)).all():
                scopes.add(_scope_from_columns(row))

        legacy_symbols: dict[str, set[str]] = defaultdict(set)
        fill_statement = select(AccountFillEventRow).where(
            AccountFillEventRow.environment == environment,
            AccountFillEventRow.account_label == account_label,
        )
        for row in (await session.scalars(fill_statement)).all():
            raw_side = _raw_position_side(row.raw_payload)
            side = raw_side or "BOTH"
            if side in {"BOTH", "LONG", "SHORT"}:
                legacy_symbols[row.symbol].add(side)

        snapshot_statement = select(AccountPositionSnapshotRow).where(
            AccountPositionSnapshotRow.environment == environment,
            AccountPositionSnapshotRow.account_label == account_label,
        )
        for row in (await session.scalars(snapshot_statement)).all():
            side = str(row.position_side).upper()
            if side in {"BOTH", "LONG", "SHORT"}:
                legacy_symbols[row.symbol].add(side)

        for symbol, sides in legacy_symbols.items():
            for side in sides:
                scopes.add(
                    AccountFactStreamScope(
                        environment=environment,
                        account_label=account_label,
                        symbol=symbol,
                        position_side=side,
                        stream_id=LEGACY_STREAM_ID,
                        stream_epoch=LEGACY_STREAM_EPOCH,
                    )
                )
        return tuple(sorted(scopes, key=lambda item: item.canonical_id))

    async def _load_legacy_recovery_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut:
        key = PositionKey(
            environment=scope.environment,
            account_label=scope.account_label,
            symbol=scope.symbol,
            position_side=scope.position_side,
        )
        fill_statement = select(AccountFillEventRow).where(
            AccountFillEventRow.environment == scope.environment,
            AccountFillEventRow.account_label == scope.account_label,
            AccountFillEventRow.symbol == scope.symbol,
            AccountFillEventRow.trade_at <= as_of,
        )
        fills: list[AccountFillEvent] = []
        issues = [
            "Legacy account tables have no stream epoch or continuous coverage proof"
        ]
        for row in (await session.scalars(fill_statement)).all():
            raw_side = _raw_position_side(row.raw_payload)
            if raw_side is not None and raw_side not in {"BOTH", "LONG", "SHORT"}:
                issues.append(
                    f"Legacy fill {row.trade_id} has unknown positionSide {raw_side}"
                )
                continue
            if raw_side is None:
                if scope.position_side.value != "BOTH":
                    continue
                issues.append(
                    f"Legacy fill {row.trade_id} lacks positionSide; "
                    "assigned only to BOTH"
                )
            elif raw_side != scope.position_side.value:
                continue
            fills.append(_fill_from_row(row))

        snapshot_statement = select(AccountPositionSnapshotRow).where(
            AccountPositionSnapshotRow.environment == scope.environment,
            AccountPositionSnapshotRow.account_label == scope.account_label,
            AccountPositionSnapshotRow.symbol == scope.symbol,
            AccountPositionSnapshotRow.position_side == scope.position_side.value,
            AccountPositionSnapshotRow.observed_at <= as_of,
        )
        snapshots = tuple(
            _snapshot_from_row(row)
            for row in (await session.scalars(snapshot_statement)).all()
        )
        cursor_statement = select(AccountFillReconciliationCursorRow).where(
            AccountFillReconciliationCursorRow.environment == scope.environment,
            AccountFillReconciliationCursorRow.account_label == scope.account_label,
            AccountFillReconciliationCursorRow.symbol == scope.symbol,
            AccountFillReconciliationCursorRow.last_checked_at <= as_of,
        )
        cursor_row = await session.scalar(cursor_statement)
        cursor = _cursor_from_row(cursor_row) if cursor_row is not None else None
        if cursor is not None:
            issues.append(
                "Legacy reconciliation cursor lacks side, stream epoch, and "
                "proven load start"
            )
        facts = AccountFacts(
            position_key=key,
            fills=tuple(sorted(fills, key=lambda item: (item.trade_at, item.trade_id))),
            snapshots=tuple(
                sorted(
                    snapshots, key=lambda item: (item.observed_at, item.position_side)
                )
            ),
            stream_scope=scope,
            integrity_issues=tuple(dict.fromkeys(issues)),
            fill_cursor_provenance=cursor,
        )
        return DurableJournalCut(
            scope=scope,
            as_of=as_of,
            facts=facts,
            revision=0,
            integrity_issues=facts.integrity_issues,
            cursor_provenance=cursor,
        )


def _fact_event_specs(
    facts: AccountFacts,
    *,
    delta: JournalFactDelta | None = None,
) -> list[tuple[str, str, datetime | None, dict[str, object]]]:
    codec = PositionRecoveryCodec
    specs: list[tuple[str, str, datetime | None, dict[str, object]]] = []
    fills = facts.fills if delta is None else delta.fills
    snapshots = facts.snapshots if delta is None else delta.snapshots
    boundaries = facts.exit_boundaries if delta is None else delta.exit_boundaries
    for fill in fills:
        specs.append(("fill", fill.trade_id, fill.trade_at, codec.encode_fill(fill)))
    for fill in facts.conflicting_fills:
        specs.append(
            ("fill_conflict", fill.trade_id, fill.trade_at, codec.encode_fill(fill))
        )
    for fill in facts.late_fills:
        specs.append(
            ("late_fill", fill.trade_id, fill.trade_at, codec.encode_fill(fill))
        )
    for snapshot in snapshots:
        natural_id = f"{snapshot.position_side}:{snapshot.observed_at.isoformat()}"
        specs.append(
            (
                "snapshot",
                natural_id,
                snapshot.observed_at,
                codec.encode_snapshot(snapshot),
            )
        )
    for boundary in boundaries:
        natural_id = f"{boundary.order_id}:{boundary.submitted_at.isoformat()}"
        specs.append(
            (
                "boundary",
                natural_id,
                boundary.submitted_at,
                codec.encode_boundary(boundary),
            )
        )
    if facts.coverage is not None:
        coverage = facts.coverage
        occurred_at = coverage.evidence_observed_at or coverage.end_at
        specs.append(
            (
                "coverage",
                f"revision:{coverage.confirmed_revision}"
                if coverage.confirmed_revision is not None
                else _json_digest(codec.encode_coverage(coverage)),
                occurred_at,
                codec.encode_coverage(coverage),
            )
        )
    if facts.checkpoint is not None:
        specs.append(
            (
                "legacy_checkpoint",
                facts.checkpoint.checkpoint_id,
                facts.checkpoint.event_cut,
                codec.encode_legacy_checkpoint(facts.checkpoint),
            )
        )
    for conflict in facts.fact_conflicts:
        specs.append(
            (
                "fact_conflict",
                f"{conflict.event_kind}:{conflict.event_id}",
                conflict.event_at
                if conflict.event_at is not None
                else _max_fact_time(facts),
                codec.encode_conflict(conflict),
            )
        )
    for issue in facts.integrity_issues:
        specs.append(
            (
                "integrity_issue",
                _json_digest(issue),
                _max_fact_time(facts),
                {"issue": issue},
            )
        )
    if facts.fill_cursor_provenance is not None:
        cursor = facts.fill_cursor_provenance
        payload = codec.encode_cursor(cursor)
        specs.append(
            (
                "fill_cursor",
                _json_digest(payload),
                cursor.last_checked_at,
                payload,
            )
        )
    if facts.fill_load_provenance is not None:
        provenance = facts.fill_load_provenance
        payload = codec.encode_fill_load_provenance(provenance)
        specs.append(
            (
                "fill_load_provenance",
                _json_digest(payload),
                provenance.observed_at,
                payload,
            )
        )
    state_payload: dict[str, object] = {
        "schema_version": 1,
        "has_synthetic_fills": facts.has_synthetic_fills,
        "has_late_events": facts.has_late_events,
        "integrity_issues": list(facts.integrity_issues),
    }
    specs.append(
        (
            "facts_state",
            _json_digest([state_payload, facts.compute_facts_hash()]),
            _max_fact_time(facts),
            state_payload,
        )
    )
    return specs


def _facts_from_rows(
    *,
    scope: AccountFactStreamScope,
    rows: list[PositionFactJournalEventRow],
    checkpoint: PositionRecoveryCheckpoint | None,
    prefix_complete: bool = True,
) -> tuple[
    AccountFacts,
    tuple[AccountFactConflict, ...],
    tuple[str, ...],
    AccountFillReconciliationCursor | None,
]:
    groups: dict[tuple[str, str], list[PositionFactJournalEventRow]] = defaultdict(list)
    for row in rows:
        groups[(row.event_kind, row.event_id)].append(row)
    selected: dict[str, list[PositionFactJournalEventRow]] = defaultdict(list)
    conflicts: list[AccountFactConflict] = []
    issues: list[str] = []
    for (kind, event_id), variants in groups.items():
        ordered = sorted(
            variants, key=lambda item: (item.recorded_at, item.payload_hash)
        )
        selected[kind].append(ordered[0])
        hashes = {item.payload_hash for item in variants}
        if len(hashes) > 1:
            conflicts.append(
                AccountFactConflict(
                    event_kind=kind,
                    event_id=event_id,
                    details=(
                        "same durable event identity was stored with different payloads"
                    ),
                    event_at=ordered[-1].occurred_at,
                )
            )
            issues.append(f"Conflicting durable payloads for {kind} {event_id}")

    def rows_for(kind: str) -> list[PositionFactJournalEventRow]:
        return sorted(
            selected.get(kind, []),
            key=lambda item: (item.occurred_at, item.event_id, item.payload_hash),
        )

    def latest_recorded(kind: str) -> PositionFactJournalEventRow | None:
        candidates = selected.get(kind, [])
        return max(
            candidates,
            key=lambda item: (item.source_revision, item.recorded_at, item.event_id),
            default=None,
        )

    fills = tuple(
        PositionRecoveryCodec.decode_fill(row.payload) for row in rows_for("fill")
    )
    fill_conflicts = tuple(
        PositionRecoveryCodec.decode_fill(row.payload)
        for row in rows_for("fill_conflict")
    )
    late_fill_rows = list(rows_for("late_fill"))
    if checkpoint is not None:
        late_fill_rows.extend(
            row
            for row in rows_for("fill")
            if row.occurred_at <= checkpoint.event_cut
            and row.source_revision > checkpoint.source_revision
        )
    late_fills = tuple(
        {
            fill.trade_id: fill
            for row in late_fill_rows
            if (fill := PositionRecoveryCodec.decode_fill(row.payload))
        }.values()
    )
    snapshots = tuple(
        PositionRecoveryCodec.decode_snapshot(row.payload)
        for row in rows_for("snapshot")
    )
    boundaries = tuple(
        PositionRecoveryCodec.decode_boundary(row.payload)
        for row in rows_for("boundary")
    )
    coverage_row = latest_recorded("coverage")
    coverage = (
        PositionRecoveryCodec.decode_coverage(coverage_row.payload)
        if coverage_row is not None
        else checkpoint.coverage
        if checkpoint is not None
        else None
    )
    legacy_rows = rows_for("legacy_checkpoint")
    legacy_checkpoint = (
        PositionRecoveryCodec.decode_legacy_checkpoint(legacy_rows[-1].payload)
        if legacy_rows
        else None
    )
    domain_conflicts = tuple(
        PositionRecoveryCodec.decode_conflict(row.payload)
        for row in rows_for("fact_conflict")
    )
    late_non_fill_rows = (
        [
            row
            for row in rows
            if row.event_kind != "facts_state"
            and row.event_kind not in {"fill", "late_fill"}
            and row.occurred_at <= checkpoint.event_cut
            and row.source_revision > checkpoint.source_revision
        ]
        if checkpoint is not None
        else []
    )
    if late_non_fill_rows:
        domain_conflicts = (
            *domain_conflicts,
            *(
                AccountFactConflict(
                    event_kind=row.event_kind,
                    event_id=row.event_id,
                    details="durable fact arrived after the recovery checkpoint cut",
                    event_at=row.occurred_at,
                )
                for row in late_non_fill_rows
            ),
        )
        issues.extend(
            f"Late durable fact {row.event_kind} {row.event_id} at checkpoint cut"
            for row in late_non_fill_rows
        )
    if checkpoint is not None and any(
        row.source_revision > checkpoint.source_revision
        and row.occurred_at <= checkpoint.event_cut
        for row in rows_for("fill")
    ):
        issues.append("Late fill arrived at or before recovery checkpoint cut")
    if fill_conflicts:
        conflicts.extend(
            AccountFactConflict(
                event_kind="fill",
                event_id=fill.trade_id,
                details="journal contains a divergent fill identity",
                event_at=fill.trade_at,
            )
            for fill in fill_conflicts
        )
    if fill_conflicts:
        issues.append("Journal contains divergent fill identities")
    issues.extend(
        str(row.payload["issue"])
        for row in rows_for("integrity_issue")
        if isinstance(row.payload.get("issue"), str)
    )
    state_row = latest_recorded("facts_state")
    synthetic_flag = bool(checkpoint and checkpoint.has_synthetic_fills)
    late_flag = bool(checkpoint and checkpoint.has_late_events)
    state: dict[str, object] = {}
    if state_row is not None:
        state = state_row.payload
        if type(state.get("schema_version")) is not int or state["schema_version"] != 1:
            raise RecoverySchemaError("unsupported stored journal facts-state schema")
        stored_synthetic = state.get("has_synthetic_fills")
        if type(stored_synthetic) is not bool:
            raise RecoverySchemaError("stored facts-state synthetic flag is invalid")
        stored_late = state.get("has_late_events")
        if type(stored_late) is not bool:
            raise RecoverySchemaError("stored facts-state late flag is invalid")
        synthetic_flag = stored_synthetic
        late_flag = stored_late
        state_issues = state.get("integrity_issues")
        if not isinstance(state_issues, list) or any(
            not isinstance(item, str) for item in state_issues
        ):
            raise RecoverySchemaError("stored facts-state issue list is invalid")
        issues.extend(state_issues)
    cursors = rows_for("fill_cursor")
    cursor = (
        PositionRecoveryCodec.decode_cursor(cursors[-1].payload) if cursors else None
    )
    load_provenance_row = latest_recorded("fill_load_provenance")
    load_provenance = (
        PositionRecoveryCodec.decode_fill_load_provenance(load_provenance_row.payload)
        if load_provenance_row is not None
        else checkpoint.coverage.load_provenance
        if checkpoint is not None and checkpoint.coverage is not None
        else None
    )
    key = PositionKey(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side,
    )
    facts = AccountFacts(
        position_key=key,
        fills=fills,
        snapshots=snapshots,
        exit_boundaries=boundaries,
        coverage=coverage,
        checkpoint=legacy_checkpoint,
        has_synthetic_fills=synthetic_flag,
        conflicting_fills=fill_conflicts,
        has_late_events=late_flag or bool(late_fills),
        stream_scope=scope,
        recovery_checkpoint=checkpoint,
        fact_conflicts=tuple([*domain_conflicts, *conflicts]),
        integrity_issues=tuple(dict.fromkeys(issues)),
        late_fills=late_fills,
        fill_cursor_provenance=cursor,
        fill_load_provenance=load_provenance,
        prefix_facts_complete=prefix_complete,
    )
    if cursor is not None and (
        cursor.environment != scope.environment
        or cursor.account_label != scope.account_label
        or cursor.symbol != scope.symbol
    ):
        raise RecoverySchemaError("stored fill cursor does not match stream scope")
    if coverage is not None and coverage.stream_scope != scope:
        raise RecoverySchemaError("stored coverage scope does not match stream scope")
    return facts, tuple(conflicts), facts.integrity_issues, cursor


def _scope_values(scope: AccountFactStreamScope) -> dict[str, str]:
    return {
        "environment": scope.environment,
        "account_label": scope.account_label,
        "symbol": scope.symbol,
        "position_side": scope.position_side.value,
        "stream_id": scope.stream_id,
        "stream_epoch": scope.stream_epoch,
    }


def _validate_new_load_origin(provenance: AccountFillLoadProvenance) -> None:
    if provenance.scan_origin_from_id is not None:
        if provenance.request_from_id != provenance.scan_origin_from_id:
            raise JournalFactConflict("new fill-load scan did not start at its origin")
    elif provenance.request_from_id is not None:
        raise JournalFactConflict(
            "new time-origin fill-load scan must start without a cursor"
        )


def _scope_identity(scope: AccountFactStreamScope) -> tuple[str, ...]:
    values = _scope_values(scope)
    return tuple(
        values[name]
        for name in (
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
        )
    )


def _scope_conditions(model: Any, scope: AccountFactStreamScope) -> tuple[Any, ...]:
    values = _scope_values(scope)
    return tuple(getattr(model, name) == value for name, value in values.items())


def _scope_from_columns(row: Any) -> AccountFactStreamScope:
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide

    return AccountFactStreamScope(
        environment=row.environment,
        account_label=row.account_label,
        symbol=row.symbol,
        position_side=FuturesPositionSide(row.position_side),
        stream_id=row.stream_id,
        stream_epoch=row.stream_epoch,
    )


def _json_digest(value: object) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("journal payload is not canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _max_fact_time(facts: AccountFacts) -> datetime | None:
    values: list[datetime] = [
        *(fill.trade_at for fill in facts.fills),
        *(fill.trade_at for fill in facts.conflicting_fills),
        *(fill.trade_at for fill in facts.late_fills),
        *(snapshot.observed_at for snapshot in facts.snapshots),
        *(boundary.submitted_at for boundary in facts.exit_boundaries),
    ]
    if facts.coverage is not None:
        values.append(facts.coverage.evidence_observed_at or facts.coverage.end_at)
    if facts.checkpoint is not None:
        values.append(facts.checkpoint.event_cut)
    if facts.fill_cursor_provenance is not None:
        values.append(facts.fill_cursor_provenance.last_checked_at)
    if facts.fill_load_provenance is not None:
        values.append(facts.fill_load_provenance.observed_at)
    if values:
        return max(values)
    if facts.recovery_checkpoint is not None:
        return facts.recovery_checkpoint.event_cut
    return None


_raw_position_side = extract_fill_position_side


def _fill_from_row(row: AccountFillEventRow) -> AccountFillEvent:
    return AccountFillEvent(
        environment=row.environment,
        account_label=row.account_label,
        symbol=row.symbol,
        trade_id=row.trade_id,
        order_id=row.order_id,
        side=row.side,
        price=row.price,
        quantity=row.quantity,
        realized_pnl=row.realized_pnl,
        fee=row.fee,
        fee_asset=row.fee_asset,
        trade_at=row.trade_at,
        raw_payload=row.raw_payload,
    )


def _snapshot_from_row(row: AccountPositionSnapshotRow) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment=row.environment,
        account_label=row.account_label,
        symbol=row.symbol,
        position_side=row.position_side,
        position_amt=row.position_amt,
        entry_price=row.entry_price,
        mark_price=row.mark_price,
        unrealized_pnl=row.unrealized_pnl,
        notional=row.notional,
        leverage=row.leverage,
        margin_type=row.margin_type,
        observed_at=row.observed_at,
        raw_payload=row.raw_payload,
    )


def _cursor_from_row(
    row: AccountFillReconciliationCursorRow,
) -> AccountFillReconciliationCursor:
    return AccountFillReconciliationCursor(
        environment=row.environment,
        account_label=row.account_label,
        symbol=row.symbol,
        from_id=row.from_id,
        start_time_ms=row.start_time_ms,
        last_checked_at=row.last_checked_at,
    )


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
