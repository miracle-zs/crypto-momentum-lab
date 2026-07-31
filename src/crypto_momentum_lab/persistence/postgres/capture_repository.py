from collections.abc import Iterable
from datetime import date, datetime

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    MarketDataState,
    QualityEvent,
)
from crypto_momentum_lab.persistence.postgres.models import (
    MarketDataProcessStateRow,
    MarketDataQualityEventRow,
    RawArchiveManifestRow,
)


class PostgresCaptureRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_manifest(self, manifest: ArchiveManifest) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                existing = await session.scalar(
                    select(RawArchiveManifestRow).where(
                        RawArchiveManifestRow.relative_path
                        == str(manifest.relative_path)
                    )
                )
                if existing is not None:
                    if existing.sha256 != manifest.sha256:
                        raise ValueError(
                            "archive manifest checksum conflict"
                        )
                    return
                session.add(_manifest_row(manifest))

    async def load_manifest_paths_before(
        self,
        utc_date: date,
    ) -> tuple[str, ...]:
        async with self._session_factory() as session:
            paths = await session.scalars(
                select(RawArchiveManifestRow.relative_path).where(
                    RawArchiveManifestRow.utc_date < utc_date
                )
            )
            return tuple(paths)

    async def delete_manifests(
        self,
        relative_paths: Iterable[str],
    ) -> int:
        paths = tuple(dict.fromkeys(relative_paths))
        if not paths:
            return 0
        deleted_count = 0
        async with self._session_factory() as session:
            async with session.begin():
                for start in range(0, len(paths), 1000):
                    result = await session.execute(
                        delete(RawArchiveManifestRow).where(
                            RawArchiveManifestRow.relative_path.in_(
                                paths[start : start + 1000]
                            )
                        )
                    )
                    deleted_count += int(getattr(result, "rowcount", 0) or 0)
        return deleted_count

    async def save_quality_event(self, event: QualityEvent) -> None:
        statement = insert(MarketDataQualityEventRow).values(
            **_quality_values(event)
        )
        statement = statement.on_conflict_do_nothing(
            index_elements=["event_id"]
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(statement)

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                session.add(
                    MarketDataProcessStateRow(
                        state=state.value,
                        occurred_at=occurred_at,
                        reason=reason,
                    )
                )

    async def count_quality_events(self) -> int:
        async with self._session_factory() as session:
            count = await session.scalar(
                select(func.count()).select_from(MarketDataQualityEventRow)
            )
        return int(count or 0)

    async def latest_process_state(self) -> MarketDataState | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(MarketDataProcessStateRow)
                .order_by(
                    MarketDataProcessStateRow.occurred_at.desc(),
                    MarketDataProcessStateRow.state_id.desc(),
                )
                .limit(1)
            )
        return None if row is None else MarketDataState(row.state)


def _manifest_row(manifest: ArchiveManifest) -> RawArchiveManifestRow:
    return RawArchiveManifestRow(
        manifest_id=manifest.manifest_id,
        relative_path=str(manifest.relative_path),
        sha256=manifest.sha256,
        schema_version=manifest.schema_version,
        exchange=manifest.exchange,
        environment=manifest.environment,
        route=manifest.route.value,
        stream=manifest.stream.value,
        symbol=manifest.symbol,
        utc_date=manifest.utc_date,
        utc_hour=manifest.utc_hour,
        connection_session_id=manifest.connection_session_id,
        subscription_generation_min=manifest.subscription_generation_min,
        subscription_generation_max=manifest.subscription_generation_max,
        row_count=manifest.row_count,
        compressed_bytes=manifest.compressed_bytes,
        first_exchange_event_at=manifest.first_exchange_event_at,
        last_exchange_event_at=manifest.last_exchange_event_at,
        first_received_at=manifest.first_received_at,
        last_received_at=manifest.last_received_at,
        capture_version=manifest.capture_version,
        recovery_status=manifest.recovery_status,
        known_gap_count=manifest.known_gap_count,
        created_at=manifest.created_at,
    )


def _quality_values(event: QualityEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "category": event.category.value,
        "occurred_at": event.occurred_at,
        "route": None if event.route is None else event.route.value,
        "stream": None if event.stream is None else event.stream.value,
        "symbol": event.symbol,
        "connection_session_id": event.connection_session_id,
        "local_sequence": event.local_sequence,
        "details": event.details,
    }
