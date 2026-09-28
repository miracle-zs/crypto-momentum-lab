"""Async PostgreSQL repository for durable DecisionTrace auditing and replay.

Implements R2 requirements from Astra Architecture Blueprint 2026-09-25:
- Immutable DecisionTraces persisted to decision_traces table;
- Evaluated MarketRevisionRefs linked and guaranteed durable in market_revision_refs;
- Non-blocking execution pool with best-effort commit policy.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.persistence.postgres.market_book_repository import (
    _observed_at_from_lineage,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionTraceRow,
    MarketRevisionRefRow,
)

log = structlog.get_logger()


class PostgresDecisionTraceRepository:
    """Async repository persisting DecisionTraces on the observability session pool."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_decision_trace(self, trace: DecisionTrace) -> None:
        """Persists a single DecisionTrace and ensures its market refs exist."""
        await self.save_decision_traces((trace,))

    async def save_decision_traces(
        self,
        traces: Sequence[DecisionTrace],
    ) -> None:
        """Batch-persists DecisionTraces and their market revision refs."""
        if not traces:
            return
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    # Diagnostic plane: do not block on synchronous commit
                    await session.execute(text("SET LOCAL synchronous_commit = OFF"))
                    await self.save_decision_traces_in_session(
                        session, traces, require_revision_payload=False
                    )
        except Exception as exc:
            log.warning(
                "save_decision_traces_failed", count=len(traces), error=str(exc)
            )
            raise

    async def save_decision_traces_in_session(
        self,
        session: AsyncSession,
        traces: Sequence[DecisionTrace],
        *,
        require_revision_payload: bool = True,
    ) -> None:
        """Persist complete immutable traces using the caller's transaction.

        This entrypoint is for the durable decision unit of work. It never
        commits and never changes ``synchronous_commit``. Existing identifiers
        are accepted only when every persisted field and referenced market
        revision matches byte-for-byte at the JSON value level.
        """
        if not traces:
            return

        now_utc = datetime.now(UTC)
        trace_rows_by_id: dict[str, dict[str, Any]] = {}
        revision_rows_by_id: dict[str, dict[str, Any]] = {}
        embedded_revision_ids: set[str] = set()
        for trace in traces:
            payload = dict(trace.trace_payload)
            if trace.frame_digest and "frame_digest" not in payload:
                payload["frame_digest"] = trace.frame_digest
            if trace.input_hash and "input_hash" not in payload:
                payload["input_hash"] = trace.input_hash
            trace_row = {
                "decision_id": trace.decision_id,
                "strategy_name": trace.strategy_name,
                "account_label": trace.account_label,
                "decision_time": trace.decision_time,
                "intent_produced": trace.intent_produced,
                "intent_id": trace.intent_id,
                "rejection_reason": trace.rejection_reason,
                "evaluated_revision_ids": [
                    ref.revision_id for ref in trace.evaluated_market_refs
                ],
                "trace_payload": payload,
                "created_at": now_utc,
            }
            previous_trace = trace_rows_by_id.get(trace.decision_id)
            if previous_trace is not None and any(
                previous_trace[name] != trace_row[name]
                for name in trace_row
                if name != "created_at"
            ):
                raise ValueError(
                    f"DecisionTrace {trace.decision_id} conflicts within one commit"
                )
            trace_rows_by_id.setdefault(trace.decision_id, trace_row)

            embedded_state = payload.get("market_state")
            for ref in trace.evaluated_market_refs:
                has_market_payload = isinstance(embedded_state, dict)
                ref_payload = (
                    dict(embedded_state)
                    if has_market_payload
                    else {"reference_only": True}
                )
                lineage = {"source_epoch": ref.source_epoch}
                if ref.observed_at is not None:
                    lineage["observed_at"] = ref.observed_at.isoformat()
                visibility = getattr(ref.visibility_mode, "value", ref.visibility_mode)
                ref_row = {
                    "revision_id": ref.revision_id,
                    "scope": ref.scope,
                    "symbol": ref.symbol,
                    "interval": ref.interval,
                    "bucket_start": ref.bucket_start,
                    "bucket_end": ref.bucket_end,
                    "content_hash": ref.content_hash,
                    "published_at": ref.published_at,
                    "source_epoch": ref.source_epoch,
                    "visibility_mode": str(visibility),
                    "is_canonical": str(visibility) == "canonical",
                    "payload": ref_payload,
                    "lineage": lineage,
                }
                previous_ref = revision_rows_by_id.get(ref.revision_id)
                previous_has_payload = ref.revision_id in embedded_revision_ids
                identity_fields = (
                    "scope",
                    "symbol",
                    "interval",
                    "bucket_start",
                    "bucket_end",
                    "content_hash",
                    "published_at",
                    "source_epoch",
                    "visibility_mode",
                    "is_canonical",
                )
                if previous_ref is not None and (
                    any(
                        previous_ref[name] != ref_row[name]
                        for name in identity_fields
                    )
                    or (
                        has_market_payload
                        and previous_has_payload
                        and previous_ref["payload"] != ref_row["payload"]
                    )
                ):
                    raise ValueError(
                        f"MarketRevisionRef {ref.revision_id} conflicts within one commit"
                    )
                if previous_ref is None or (
                    has_market_payload and not previous_has_payload
                ):
                    revision_rows_by_id[ref.revision_id] = ref_row
                if has_market_payload:
                    embedded_revision_ids.add(ref.revision_id)

        trace_rows = list(trace_rows_by_id.values())
        incoming_trace_ids = list(trace_rows_by_id)
        trace_values = (
            "strategy_name",
            "account_label",
            "decision_time",
            "intent_produced",
            "intent_id",
            "rejection_reason",
            "evaluated_revision_ids",
            "trace_payload",
        )
        incoming_by_id = {row["decision_id"]: row for row in trace_rows}
        incoming_revisions = revision_rows_by_id
        existing_revisions = (
            await session.execute(
                select(MarketRevisionRefRow).where(
                    MarketRevisionRefRow.revision_id.in_(incoming_revisions)
                )
            )
        ).scalars().all()
        ref_identity_values = (
            "scope",
            "symbol",
            "interval",
            "bucket_start",
            "bucket_end",
            "content_hash",
        )
        existing_revision_ids: set[str] = set()
        for existing in existing_revisions:
            incoming = incoming_revisions[existing.revision_id]
            existing_revision_ids.add(existing.revision_id)
            if any(
                getattr(existing, name) != incoming[name]
                for name in ref_identity_values
            ):
                raise ValueError(
                    "Immutable audit conflict: MarketRevisionRef "
                    f"'{existing.revision_id}' already exists with conflicting contents"
                )

        missing_reference_only = {
            revision_id
            for revision_id in incoming_revisions
            if revision_id not in embedded_revision_ids
            and revision_id not in existing_revision_ids
        }
        if missing_reference_only and require_revision_payload:
            raise ValueError(
                "DecisionTrace references market revisions with no durable payload: "
                + ", ".join(sorted(missing_reference_only))
            )
        missing_revisions = [
            row
            for row in revision_rows_by_id.values()
            if row["revision_id"] not in existing_revision_ids
            and (
                row["revision_id"] in embedded_revision_ids
                or not require_revision_payload
            )
        ]
        if missing_revisions:
            await session.execute(
                insert(MarketRevisionRefRow)
                .values(missing_revisions)
                .on_conflict_do_nothing(index_elements=["revision_id"])
            )
        await session.execute(
            insert(DecisionTraceRow)
            .values(trace_rows)
            .on_conflict_do_nothing(index_elements=["decision_id"])
        )

        # Re-read after inserts. PostgreSQL waits on a concurrent uniqueness
        # conflict; this statement then observes the winner and validates it.
        persisted_traces = (
            await session.execute(
                select(DecisionTraceRow).where(
                    DecisionTraceRow.decision_id.in_(incoming_trace_ids)
                )
            )
        ).scalars().all()
        persisted_trace_ids = {row.decision_id for row in persisted_traces}
        if persisted_trace_ids != set(incoming_trace_ids):
            raise ValueError("DecisionTrace insert did not persist every decision")
        for existing in persisted_traces:
            incoming = incoming_by_id[existing.decision_id]
            if any(getattr(existing, name) != incoming[name] for name in trace_values):
                raise ValueError(
                    "Immutable audit conflict: DecisionTrace "
                    f"'{existing.decision_id}' already exists with conflicting contents"
                )

        if missing_revisions:
            persisted_revisions = (
                await session.execute(
                    select(MarketRevisionRefRow).where(
                        MarketRevisionRefRow.revision_id.in_(
                            [row["revision_id"] for row in missing_revisions]
                        )
                    )
                )
            ).scalars().all()
            persisted_revision_ids = {row.revision_id for row in persisted_revisions}
            if persisted_revision_ids != {
                row["revision_id"] for row in missing_revisions
            }:
                raise ValueError("MarketRevisionRef insert did not persist every ref")
            for existing in persisted_revisions:
                incoming = incoming_revisions[existing.revision_id]
                if any(
                    getattr(existing, name) != incoming[name]
                    for name in ref_identity_values
                ):
                    raise ValueError(
                        "Immutable audit conflict: MarketRevisionRef "
                        f"'{existing.revision_id}' has conflicting contents"
                    )

    async def load_decision_trace(self, decision_id: str) -> DecisionTrace | None:
        """Loads a DecisionTrace and resolves its evaluated MarketRevisionRefs."""
        async with self._session_factory() as session:
            stmt = select(DecisionTraceRow).where(
                DecisionTraceRow.decision_id == decision_id
            )
            res = await session.execute(stmt)
            row = res.scalar_one_or_none()
            if row is None:
                return None

            rev_ids = [str(r) for r in (row.evaluated_revision_ids or [])]
            refs: list[MarketRevisionRef] = []
            if rev_ids:
                stmt_rev = select(MarketRevisionRefRow).where(
                    MarketRevisionRefRow.revision_id.in_(rev_ids)
                )
                rev_res = await session.execute(stmt_rev)
                rev_map = {r.revision_id: r for r in rev_res.scalars().all()}

                for rid in rev_ids:
                    rrow = rev_map.get(rid)
                    if rrow is None:
                        raise UnreproducibleError(
                            f"Decision trace {decision_id} is unreproducible: "
                            f"missing revision {rid}"
                        )
                    refs.append(
                        MarketRevisionRef(
                            scope=rrow.scope,
                            symbol=rrow.symbol,
                            interval=rrow.interval,
                            bucket_start=rrow.bucket_start,
                            bucket_end=rrow.bucket_end,
                            revision_id=rrow.revision_id,
                            content_hash=rrow.content_hash,
                            published_at=rrow.published_at,
                            observed_at=_observed_at_from_lineage(rrow.lineage),
                            source_epoch=rrow.source_epoch,
                            visibility_mode=MarketVisibilityMode(rrow.visibility_mode),
                        )
                    )

            return DecisionTrace(
                decision_id=row.decision_id,
                strategy_name=row.strategy_name,
                account_label=row.account_label,
                decision_time=row.decision_time,
                evaluated_market_refs=tuple(refs),
                intent_produced=row.intent_produced,
                intent_id=row.intent_id,
                rejection_reason=row.rejection_reason,
                input_hash=str(row.trace_payload.get("input_hash", "")),
                frame_digest=str(row.trace_payload.get("frame_digest", "")),
                trace_payload=dict(row.trace_payload),
            )

    async def count_decision_traces(self, account_label: str | None = None) -> int:
        """Returns the total number of decision traces recorded."""
        async with self._session_factory() as session:
            stmt = select(func.count(DecisionTraceRow.decision_id))
            if account_label is not None:
                stmt = stmt.where(DecisionTraceRow.account_label == account_label)
            res = await session.execute(stmt)
            count = res.scalar()
            return int(count or 0)

    async def load_latest_decision_trace(
        self, strategy_name: str, account_label: str
    ) -> DecisionTrace | None:
        """Loads the most recent DecisionTrace recorded for strategy and account."""
        decision_id: str | None = None
        async with self._session_factory() as session:
            stmt = (
                select(DecisionTraceRow.decision_id)
                .where(
                    DecisionTraceRow.strategy_name == strategy_name,
                    DecisionTraceRow.account_label == account_label,
                )
                .order_by(DecisionTraceRow.decision_time.desc())
                .limit(1)
            )
            res = await session.execute(stmt)
            decision_id = res.scalar_one_or_none()
        if decision_id is None:
            return None
        return await self.load_decision_trace(decision_id)


__all__ = ["PostgresDecisionTraceRepository"]
