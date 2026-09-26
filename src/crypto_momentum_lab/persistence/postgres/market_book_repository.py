"""PostgresMarketBookRepository for durable market revisions, datasets, and traces.

Implements MarketBookRepository protocol defined in domain/market/market_book.py.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

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


def _observed_at_from_lineage(lineage: dict[str, object] | None) -> datetime | None:
    if lineage and "observed_at" in lineage:
        val = lineage["observed_at"]
        if isinstance(val, str):
            return datetime.fromisoformat(val)
    return None


class PostgresMarketBookRepository:
    """Postgres-backed storage for market revisions, pointers, and manifests."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def save_envelope(self, envelope: MarketEnvelope) -> None:
        with self._session_factory() as session:
            payload = market_state_to_payload(envelope.state)
            lineage = dict(envelope.lineage)
            if envelope.ref.observed_at is not None:
                lineage["observed_at"] = envelope.ref.observed_at.isoformat()
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
                lineage=lineage,
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
                observed_at=_observed_at_from_lineage(row.lineage),
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
                observed_at=_observed_at_from_lineage(row.lineage),
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
                    observed_at=_observed_at_from_lineage(row.lineage),
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
            # Batch load revision rows in chunks to prevent exceeding
            # PostgreSQL parameter limit (65535)
            rev_rows: dict[str, MarketRevisionRefRow] = {}
            chunk_size = 5000
            for i in range(0, len(rev_ids), chunk_size):
                chunk = rev_ids[i : i + chunk_size]
                stmt = select(MarketRevisionRefRow).where(
                    MarketRevisionRefRow.revision_id.in_(chunk)
                )
                for r in session.execute(stmt).scalars().all():
                    rev_rows[r.revision_id] = r

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
                        observed_at=_observed_at_from_lineage(rrow.lineage),
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
        payload = dict(trace.trace_payload)
        if trace.frame_digest and "frame_digest" not in payload:
            payload["frame_digest"] = trace.frame_digest
        if trace.input_hash and "input_hash" not in payload:
            payload["input_hash"] = trace.input_hash
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
                trace_payload=payload,
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
                        observed_at=_observed_at_from_lineage(rrow.lineage),
                        source_epoch=rrow.source_epoch,
                        visibility_mode=MarketVisibilityMode(rrow.visibility_mode),
                    )
                )

            payload = row.trace_payload or {}
            input_hash = str(payload.get("input_hash", ""))
            frame_digest = str(payload.get("frame_digest", ""))

            return DecisionTrace(
                decision_id=row.decision_id,
                strategy_name=row.strategy_name,
                account_label=row.account_label,
                decision_time=row.decision_time,
                evaluated_market_refs=tuple(refs),
                intent_produced=row.intent_produced,
                intent_id=row.intent_id,
                rejection_reason=row.rejection_reason,
                input_hash=input_hash,
                frame_digest=frame_digest,
                trace_payload=payload,
            )

    def get_canonical_refs_in_range(
        self,
        scope: str,
        symbols: tuple[str, ...],
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[tuple[str, datetime], MarketRevisionRef]:
        with self._session_factory() as session:
            stmt = (
                select(MarketRevisionRefRow)
                .where(
                    MarketRevisionRefRow.scope == scope,
                    MarketRevisionRefRow.symbol.in_(symbols),
                    MarketRevisionRefRow.interval == interval,
                    MarketRevisionRefRow.bucket_start >= start_time,
                    MarketRevisionRefRow.bucket_start < end_time,
                    MarketRevisionRefRow.is_canonical.is_(True),
                )
                .order_by(
                    MarketRevisionRefRow.bucket_start.asc(),
                    MarketRevisionRefRow.published_at.desc(),
                )
            )
            rows = session.execute(stmt).scalars().all()
            result: dict[tuple[str, datetime], MarketRevisionRef] = {}
            for row in rows:
                key = (row.symbol, row.bucket_start)
                if key not in result:
                    result[key] = MarketRevisionRef(
                        scope=row.scope,
                        symbol=row.symbol,
                        interval=row.interval,
                        bucket_start=row.bucket_start,
                        bucket_end=row.bucket_end,
                        revision_id=row.revision_id,
                        content_hash=row.content_hash,
                        published_at=row.published_at,
                        observed_at=_observed_at_from_lineage(row.lineage),
                        source_epoch=row.source_epoch,
                        visibility_mode=MarketVisibilityMode(row.visibility_mode),
                    )
            return result

    def list_manifests(
        self, scope: str | None = None, limit: int = 100
    ) -> list[DatasetManifest]:
        with self._session_factory() as session:
            stmt = select(DatasetManifestRow)
            if scope is not None:
                stmt = stmt.where(DatasetManifestRow.scope == scope)
            stmt = stmt.order_by(DatasetManifestRow.start_time.desc()).limit(limit)
            rows = session.execute(stmt).scalars().all()
            return [
                DatasetManifest(
                    manifest_id=row.manifest_id,
                    scope=row.scope,
                    symbols=tuple(row.symbols.split(",")),
                    interval=row.interval,
                    start_time=row.start_time,
                    end_time=row.end_time,
                    visibility_mode=MarketVisibilityMode(row.visibility_mode),
                    revision_refs=(),
                    schema_version=row.schema_version,
                    feature_algorithm_version=row.feature_algorithm_version,
                    manifest_hash=row.manifest_hash,
                    created_at=row.created_at,
                    coverage_ratio=row.coverage_ratio,
                    holes=tuple(
                        (
                            datetime.fromisoformat(str(h[0])),  # type: ignore[index]
                            datetime.fromisoformat(str(h[1])),  # type: ignore[index]
                        )
                        for h in (row.holes or [])
                    ),
                )
                for row in rows
            ]

    def get_distinct_dates_and_symbols(
        self, scope: str, interval: str = "15s"
    ) -> list[tuple[date, tuple[str, ...]]]:
        with self._session_factory() as session:
            stmt = (
                select(
                    MarketRevisionRefRow.bucket_start,
                    MarketRevisionRefRow.symbol,
                )
                .where(
                    MarketRevisionRefRow.scope == scope,
                    MarketRevisionRefRow.interval == interval,
                    MarketRevisionRefRow.is_canonical.is_(True),
                )
                .order_by(MarketRevisionRefRow.bucket_start.asc())
            )
            rows = session.execute(stmt).all()
            date_to_symbols: dict[date, set[str]] = {}
            for b_start, sym in rows:
                d = b_start.date()
                date_to_symbols.setdefault(d, set()).add(sym)
            return [
                (d, tuple(sorted(syms)))
                for d, syms in sorted(date_to_symbols.items())
            ]
