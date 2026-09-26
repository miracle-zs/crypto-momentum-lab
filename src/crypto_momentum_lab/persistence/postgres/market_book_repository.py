"""PostgresMarketBookRepository for durable market revisions, datasets, and traces.

Implements MarketBookRepository protocol defined in domain/market/market_book.py.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import (
    DatasetManifest,
    DecisionTrace,
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.market_data.hub import (
    market_state_from_payload,
    market_state_to_payload,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DatasetManifestRow,
    DecisionTraceRow,
    MarketRevisionRefRow,
)


class PostgresMarketBookRepository:
    """Postgres-backed storage for market revisions, pointers, and manifests."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def save_envelope(self, envelope: MarketEnvelope) -> None:
        with self._session_factory() as session:
            payload = market_state_to_payload(envelope.state)
            row = MarketRevisionRefRow(
                revision_id=envelope.ref.revision_id,
                scope=envelope.ref.scope,
                symbol=envelope.ref.symbol,
                interval=envelope.ref.interval,
                bucket_start=envelope.ref.bucket_start,
                bucket_end=envelope.ref.bucket_end,
                content_hash=envelope.ref.content_hash,
                published_at=envelope.ref.published_at,
                source_epoch=envelope.ref.source_epoch,
                visibility_mode=envelope.ref.visibility_mode.value,
                is_canonical=(
                    envelope.ref.visibility_mode == MarketVisibilityMode.CANONICAL
                ),
                payload=payload,
                lineage=envelope.lineage,
            )
            session.merge(row)
            session.commit()

    def load_envelope(self, revision_id: str) -> MarketEnvelope | None:
        with self._session_factory() as session:
            row = session.get(MarketRevisionRefRow, revision_id)
            if row is None:
                return None
            state = market_state_from_payload(row.payload)
            ref = MarketRevisionRef(
                scope=row.scope,
                symbol=row.symbol,
                interval=row.interval,
                bucket_start=row.bucket_start,
                bucket_end=row.bucket_end,
                revision_id=row.revision_id,
                content_hash=row.content_hash,
                published_at=row.published_at,
                source_epoch=row.source_epoch,
                visibility_mode=MarketVisibilityMode(row.visibility_mode),
            )
            return MarketEnvelope(
                ref=ref,
                state=state,
                lineage=row.lineage,
                data_complete=state.data_complete,
                missing_count=state.missing_agg_trade_count,
            )

    def get_canonical_ref(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> MarketRevisionRef | None:
        with self._session_factory() as session:
            stmt = (
                select(MarketRevisionRefRow)
                .where(
                    MarketRevisionRefRow.scope == scope,
                    MarketRevisionRefRow.symbol == symbol,
                    MarketRevisionRefRow.interval == interval,
                    MarketRevisionRefRow.bucket_start == bucket_start,
                    MarketRevisionRefRow.is_canonical.is_(True),
                )
                .order_by(MarketRevisionRefRow.published_at.desc())
                .limit(1)
            )
            row = session.execute(stmt).scalar_one_or_none()
            if row is None:
                return None
            return MarketRevisionRef(
                scope=row.scope,
                symbol=row.symbol,
                interval=row.interval,
                bucket_start=row.bucket_start,
                bucket_end=row.bucket_end,
                revision_id=row.revision_id,
                content_hash=row.content_hash,
                published_at=row.published_at,
                source_epoch=row.source_epoch,
                visibility_mode=MarketVisibilityMode(row.visibility_mode),
            )

    def set_canonical_ref(
        self,
        scope: str,
        symbol: str,
        interval: str,
        bucket_start: datetime,
        ref: MarketRevisionRef,
    ) -> None:
        with self._session_factory() as session:
            # Clear previous canonical flags for this bucket
            session.execute(
                update(MarketRevisionRefRow)
                .where(
                    MarketRevisionRefRow.scope == scope,
                    MarketRevisionRefRow.symbol == symbol,
                    MarketRevisionRefRow.interval == interval,
                    MarketRevisionRefRow.bucket_start == bucket_start,
                )
                .values(is_canonical=False)
            )
            # Set target revision as canonical
            session.execute(
                update(MarketRevisionRefRow)
                .where(MarketRevisionRefRow.revision_id == ref.revision_id)
                .values(is_canonical=True)
            )
            session.commit()

    def get_revisions_for_bucket(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> tuple[MarketRevisionRef, ...]:
        with self._session_factory() as session:
            stmt = (
                select(MarketRevisionRefRow)
                .where(
                    MarketRevisionRefRow.scope == scope,
                    MarketRevisionRefRow.symbol == symbol,
                    MarketRevisionRefRow.interval == interval,
                    MarketRevisionRefRow.bucket_start == bucket_start,
                )
                .order_by(MarketRevisionRefRow.published_at.asc())
            )
            rows = session.execute(stmt).scalars().all()
            return tuple(
                MarketRevisionRef(
                    scope=row.scope,
                    symbol=row.symbol,
                    interval=row.interval,
                    bucket_start=row.bucket_start,
                    bucket_end=row.bucket_end,
                    revision_id=row.revision_id,
                    content_hash=row.content_hash,
                    published_at=row.published_at,
                    source_epoch=row.source_epoch,
                    visibility_mode=MarketVisibilityMode(row.visibility_mode),
                )
                for row in rows
            )

    def save_manifest(self, manifest: DatasetManifest) -> None:
        with self._session_factory() as session:
            row = DatasetManifestRow(
                manifest_id=manifest.manifest_id,
                scope=manifest.scope,
                symbols=",".join(manifest.symbols),
                interval=manifest.interval,
                start_time=manifest.start_time,
                end_time=manifest.end_time,
                visibility_mode=manifest.visibility_mode.value,
                schema_version=manifest.schema_version,
                feature_algorithm_version=manifest.feature_algorithm_version,
                manifest_hash=manifest.manifest_hash,
                coverage_ratio=manifest.coverage_ratio,
                revision_ids=[r.revision_id for r in manifest.revision_refs],
                holes=[[h[0].isoformat(), h[1].isoformat()] for h in manifest.holes],
                created_at=manifest.created_at,
            )
            session.merge(row)
            session.commit()

    def load_manifest(self, manifest_id: str) -> DatasetManifest | None:
        with self._session_factory() as session:
            row = session.get(DatasetManifestRow, manifest_id)
            if row is None:
                return None
            rev_ids = row.revision_ids or []
            # Batch load revision rows
            stmt = select(MarketRevisionRefRow).where(
                MarketRevisionRefRow.revision_id.in_(rev_ids)
            )
            rev_rows = {r.revision_id: r for r in session.execute(stmt).scalars().all()}

            refs = []
            for rid in rev_ids:
                rrow = rev_rows.get(str(rid))
                if rrow is None:
                    raise UnreproducibleError(
                        f"Manifest {manifest_id} is unreproducible: "
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
                        source_epoch=rrow.source_epoch,
                        visibility_mode=MarketVisibilityMode(rrow.visibility_mode),
                    )
                )

            hole_pairs: list[tuple[str, str]] = [
                (str(hole[0]), str(hole[1]))  # type: ignore[index]
                for hole in (row.holes or [])
            ]
            holes = tuple(
                (
                    datetime.fromisoformat(a),
                    datetime.fromisoformat(b),
                )
                for a, b in hole_pairs
            )
            return DatasetManifest(
                manifest_id=row.manifest_id,
                scope=row.scope,
                symbols=tuple(row.symbols.split(",")),
                interval=row.interval,
                start_time=row.start_time,
                end_time=row.end_time,
                visibility_mode=MarketVisibilityMode(row.visibility_mode),
                revision_refs=tuple(refs),
                schema_version=row.schema_version,
                feature_algorithm_version=row.feature_algorithm_version,
                manifest_hash=row.manifest_hash,
                created_at=row.created_at,
                coverage_ratio=row.coverage_ratio,
                holes=holes,
            )

    def save_decision_trace(self, trace: DecisionTrace) -> None:
        with self._session_factory() as session:
            row = DecisionTraceRow(
                decision_id=trace.decision_id,
                strategy_name=trace.strategy_name,
                account_label=trace.account_label,
                decision_time=trace.decision_time,
                intent_produced=trace.intent_produced,
                intent_id=trace.intent_id,
                rejection_reason=trace.rejection_reason,
                evaluated_revision_ids=[
                    r.revision_id for r in trace.evaluated_market_refs
                ],
                trace_payload=trace.trace_payload,
                created_at=datetime.now(UTC),
            )
            session.merge(row)
            session.commit()

    def load_decision_trace(self, decision_id: str) -> DecisionTrace | None:
        with self._session_factory() as session:
            row = session.get(DecisionTraceRow, decision_id)
            if row is None:
                return None
            rev_ids = row.evaluated_revision_ids or []
            stmt = select(MarketRevisionRefRow).where(
                MarketRevisionRefRow.revision_id.in_(rev_ids)
            )
            rev_rows = {r.revision_id: r for r in session.execute(stmt).scalars().all()}
            refs = []
            for rid in rev_ids:
                rrow = rev_rows.get(str(rid))
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
                trace_payload=row.trace_payload,
            )
