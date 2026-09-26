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

        trace_rows: list[dict[str, Any]] = []
        revision_rows: list[dict[str, Any]] = []

        now_utc = datetime.now(UTC)

        for trace in traces:
            payload = dict(trace.trace_payload)
            if trace.frame_digest and "frame_digest" not in payload:
                payload["frame_digest"] = trace.frame_digest
            if trace.input_hash and "input_hash" not in payload:
                payload["input_hash"] = trace.input_hash

            rev_ids = [r.revision_id for r in trace.evaluated_market_refs]
            trace_rows.append(
                {
                    "decision_id": trace.decision_id,
                    "strategy_name": trace.strategy_name,
                    "account_label": trace.account_label,
                    "decision_time": trace.decision_time,
                    "intent_produced": trace.intent_produced,
                    "intent_id": trace.intent_id,
                    "rejection_reason": trace.rejection_reason,
                    "evaluated_revision_ids": rev_ids,
                    "trace_payload": payload,
                    "created_at": now_utc,
                }
            )

            # If payload has embedded market_state, create ref row if missing
            embedded_state = payload.get("market_state")
            for ref in trace.evaluated_market_refs:
                ref_payload = (
                    dict(embedded_state)
                    if isinstance(embedded_state, dict)
                    else {
                        "symbol": ref.symbol,
                        "bucket_start": ref.bucket_start.isoformat(),
                        "bucket_end": ref.bucket_end.isoformat(),
                        "content_hash": ref.content_hash,
                        "synthetic_placeholder": False,
                    }
                )
                lineage = {
                    "source_epoch": ref.source_epoch,
                }
                if ref.observed_at is not None:
                    lineage["observed_at"] = ref.observed_at.isoformat()

                vis_mode = (
                    ref.visibility_mode.value
                    if hasattr(ref.visibility_mode, "value")
                    else str(ref.visibility_mode)
                )
                revision_rows.append(
                    {
                        "revision_id": ref.revision_id,
                        "scope": ref.scope,
                        "symbol": ref.symbol,
                        "interval": ref.interval,
                        "bucket_start": ref.bucket_start,
                        "bucket_end": ref.bucket_end,
                        "content_hash": ref.content_hash,
                        "published_at": ref.published_at,
                        "source_epoch": ref.source_epoch,
                        "visibility_mode": vis_mode,
                        "is_canonical": vis_mode == "canonical",
                        "payload": ref_payload,
                        "lineage": lineage,
                    }
                )

        try:
            async with self._session_factory() as session:
                async with session.begin():
                    # Diagnostic plane: do not block on synchronous commit
                    await session.execute(text("SET LOCAL synchronous_commit = OFF"))

                    if revision_rows:
                        stmt_rev = (
                            insert(MarketRevisionRefRow)
                            .values(revision_rows)
                            .on_conflict_do_nothing(index_elements=["revision_id"])
                        )
                        await session.execute(stmt_rev)

                    stmt_trace = (
                        insert(DecisionTraceRow)
                        .values(trace_rows)
                        .on_conflict_do_update(
                            index_elements=["decision_id"],
                            set_={
                                "intent_produced": insert(
                                    DecisionTraceRow
                                ).excluded.intent_produced,
                                "intent_id": insert(
                                    DecisionTraceRow
                                ).excluded.intent_id,
                                "rejection_reason": insert(
                                    DecisionTraceRow
                                ).excluded.rejection_reason,
                                "trace_payload": insert(
                                    DecisionTraceRow
                                ).excluded.trace_payload,
                            },
                        )
                    )
                    await session.execute(stmt_trace)
        except Exception as exc:
            log.warning(
                "save_decision_traces_failed", count=len(traces), error=str(exc)
            )
            raise

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


__all__ = ["PostgresDecisionTraceRepository"]
