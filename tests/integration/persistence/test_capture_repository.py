from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import func, select

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    MarketDataState,
    QualityCategory,
    QualityEvent,
)
from crypto_momentum_lab.persistence.postgres.models import (
    MarketDataProcessStateRow,
    MarketDataQualityEventRow,
)


async def test_manifest_is_idempotent_but_checksum_conflict_fails(
    capture_repository,
) -> None:
    manifest = fixture_manifest()
    await capture_repository.save_manifest(manifest)
    await capture_repository.save_manifest(manifest)

    with pytest.raises(ValueError, match="checksum conflict"):
        await capture_repository.save_manifest(
            replace(manifest, sha256="b" * 64)
        )


async def test_quality_and_process_state_are_persisted(
    capture_repository,
) -> None:
    at = datetime(2026, 6, 15, 2, 0, tzinfo=UTC)
    event = QualityEvent(
        event_id=UUID(int=10),
        category=QualityCategory.SILENCE,
        occurred_at=at,
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        connection_session_id=UUID(int=1),
        local_sequence=5,
        details={"seconds": 31},
    )
    await capture_repository.save_quality_event(event)
    await capture_repository.save_process_state(
        state=MarketDataState.DEGRADED,
        occurred_at=at,
        reason="silence",
    )

    async with capture_repository._session_factory() as session:
        quality_count = await session.scalar(
            select(func.count()).select_from(MarketDataQualityEventRow)
        )
        process_state = await session.scalar(
            select(MarketDataProcessStateRow)
            .order_by(
                MarketDataProcessStateRow.occurred_at.desc(),
                MarketDataProcessStateRow.state_id.desc(),
            )
            .limit(1)
        )

    assert quality_count == 1
    assert (
        process_state is not None
        and process_state.state == MarketDataState.DEGRADED.value
    )


def fixture_manifest() -> ArchiveManifest:
    at = datetime(2026, 6, 15, 2, 0, tzinfo=UTC)
    return ArchiveManifest(
        manifest_id=UUID(int=1),
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        utc_date=at.date(),
        utc_hour=at.hour,
        relative_path=Path(
            "exchange=binance-usdm/date=2026-06-15/"
            "stream=aggTrade/symbol=BTCUSDT/hour=02/"
            "00000000-0000-0000-0000-000000000001-"
            "00000000000000000001.jsonl.zst"
        ),
        connection_session_id=UUID(int=1),
        subscription_generation_min=1,
        subscription_generation_max=1,
        row_count=1,
        compressed_bytes=100,
        first_exchange_event_at=at,
        last_exchange_event_at=at,
        first_received_at=at,
        last_received_at=at,
        sha256="a" * 64,
        capture_version="test",
        recovery_status="complete",
        known_gap_count=0,
        created_at=at,
    )
