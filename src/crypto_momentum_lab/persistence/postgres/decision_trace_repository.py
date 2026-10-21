"""Read historical DecisionTrace records for audit and replay."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
    load_summary_market_refs,
    policy_state_reference_ids,
    resolve_policy_states,
)
from crypto_momentum_lab.persistence.postgres.market_book_repository import (
    _observed_at_from_lineage,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionPolicyEvidenceRow,
    DecisionTraceRow,
    MarketRevisionRefRow,
)


class PostgresDecisionTraceRepository:
    """Async repository reading historical DecisionTraces."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

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

            states = await _load_policy_evidence(
                session, policy_state_reference_ids(row.trace_payload)
            )
            payload = resolve_policy_states(row.trace_payload, states)
            if payload.get("evidence_level") == "summary":
                refs = load_summary_market_refs(payload, decision_id=row.decision_id)
            else:
                rev_ids = [str(r) for r in row.evaluated_revision_ids]
                refs = []
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
                input_hash=str(payload.get("input_hash", "")),
                frame_digest=str(payload.get("frame_digest", "")),
                trace_payload=payload,
            )

__all__ = ["PostgresDecisionTraceRepository"]

async def _load_policy_evidence(
    session: AsyncSession, digests: set[str]
) -> dict[str, dict[str, Any]]:
    if not digests:
        return {}
    rows = (
        (
            await session.execute(
                select(DecisionPolicyEvidenceRow)
                .where(DecisionPolicyEvidenceRow.state_digest.in_(sorted(digests)))
                .with_for_update(read=True, key_share=True)
            )
        )
        .scalars()
        .all()
    )
    return {row.state_digest: dict(row.state_payload) for row in rows}
