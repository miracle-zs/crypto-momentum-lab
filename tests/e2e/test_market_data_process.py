import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.apps.market_data import main
from crypto_momentum_lab.domain.market.models import MarketDataState
from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.models import RawArchiveManifestRow
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


@pytest.mark.e2e
async def test_market_data_runtime_archives_and_updates_subscriptions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    async_database_url: str,
    repository,
    capture_repository,
    fake_binance_server,
) -> None:
    fake_binance_server.close_first_connection = False
    await repository.save_snapshot(_active_snapshot("BTCUSDT"))
    config_path = _write_config(
        tmp_path,
        market_url=fake_binance_server.market_url,
        public_url=fake_binance_server.public_url,
    )
    monkeypatch.setenv("CML_DATABASE_URL", async_database_url)

    async with main.build_market_data_runtime(config_path) as runtime:
        assert runtime.initial_symbols == frozenset({"BTCUSDT"})
        await runtime.capture.start(
            symbols=runtime.initial_symbols,
            streams=runtime.enabled_streams,
            generation=1,
        )
        capture_task = asyncio.create_task(runtime.capture.run())
        try:
            await fake_binance_server.wait_for_subscriptions(
                _subscription_names("BTCUSDT")
            )
            await runtime.capture.apply_symbols(
                frozenset({"ETHUSDT"}),
                streams=runtime.enabled_streams,
                generation=2,
            )
            await fake_binance_server.wait_for_subscriptions(
                _subscription_names("ETHUSDT")
            )
        finally:
            await runtime.capture.stop()
            await asyncio.wait_for(capture_task, timeout=5)

    archive_root = tmp_path / "raw"
    files = tuple(archive_root.rglob("*.jsonl.zst"))
    assert any("symbol=BTCUSDT" in str(path) for path in files)
    assert any("symbol=ETHUSDT" in str(path) for path in files)
    assert not tuple(archive_root.rglob("*.tmp"))

    manifest_symbols = await _manifest_symbols(async_database_url)
    assert {"BTCUSDT", "ETHUSDT"}.issubset(manifest_symbols)
    assert await capture_repository.latest_process_state() is (
        MarketDataState.STOPPED
    )
    assert _control_event_index(
        fake_binance_server.control_events,
        method="SUBSCRIBE",
        name="ethusdt@aggTrade",
    ) < _control_event_index(
        fake_binance_server.control_events,
        method="UNSUBSCRIBE",
        name="btcusdt@aggTrade",
    )


def _write_config(
    tmp_path: Path,
    *,
    market_url: str,
    public_url: str,
) -> Path:
    universe_path = tmp_path / "universe.yaml"
    capture_path = tmp_path / "capture.yaml"
    environment_path = tmp_path / "environment.yaml"
    archive_root = tmp_path / "raw"
    universe_path.write_text(
        "\n".join(
            [
                "top_count: 20",
                "retention_rank: 30",
                "retention_hours: 2",
                "activation_minute: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    capture_path.write_text(
        "\n".join(
            [
                f"market_websocket_url: {market_url}",
                f"public_websocket_url: {public_url}",
                "enabled_streams:",
                "  - aggTrade",
                "  - bookTicker",
                "  - forceOrder",
                "  - markPrice@1s",
                "  - kline_1m",
                "max_subscriptions_per_connection: 100",
                "control_messages_per_second: 5",
                "connection_lifetime_seconds: 82800",
                "open_timeout_seconds: 1",
                "ping_interval_seconds: 20",
                "ping_timeout_seconds: 20",
                "silence_timeout_seconds: 5",
                "queue_max_events: 1000",
                "queue_max_bytes: 10485760",
                "shutdown_timeout_seconds: 5",
                "archive:",
                f"  root: {archive_root}",
                "  zstd_level: 3",
                "  rotation_uncompressed_bytes: 10485760",
                "  max_open_writers: 32",
                "  group_commit_max_events: 1",
                "  group_commit_max_milliseconds: 1",
                "  warning_free_bytes: 300",
                "  halt_free_bytes: 100",
                "  recovery_free_bytes: 200",
                "  disk_check_interval_seconds: 1",
                "  pending_manifest_max_age_seconds: 30",
                "",
            ]
        ),
        encoding="utf-8",
    )
    environment_path.write_text(
        "\n".join(
            [
                "environment: test",
                "binance_base_url: https://example.test",
                f"universe_config: {universe_path}",
                f"capture_config: {capture_path}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return environment_path


def _active_snapshot(symbol: str) -> UniverseSnapshot:
    observed_at = datetime(2026, 6, 14, 23, 1, tzinfo=UTC)
    candidate = MarketCandidate(
        symbol,
        Decimal("100"),
        Decimal("110"),
        observed_at,
    )
    rank = RankEntry(
        symbol,
        Decimal("0.1"),
        1,
        RankingSide.GAINER,
    )
    return UniverseSnapshot(
        snapshot_id=uuid4(),
        observed_at=observed_at,
        utc_day=observed_at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(candidate,),
            gainers=(rank,),
            losers=(),
            target_symbols=frozenset({symbol}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                symbol,
                MembershipStatus.TARGET,
                RankingSide.GAINER,
                None,
            ),
        ),
    )


def _subscription_names(symbol: str) -> set[str]:
    normalized = symbol.lower()
    return {
        f"{normalized}@aggTrade",
        f"{normalized}@bookTicker",
        f"{normalized}@forceOrder",
        f"{normalized}@markPrice@1s",
        f"{normalized}@kline_1m",
    }


async def _manifest_symbols(async_database_url: str) -> set[str]:
    engine = create_async_database_engine(async_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            rows = (
                await session.execute(select(RawArchiveManifestRow.symbol))
            ).scalars()
            return {symbol for symbol in rows if symbol is not None}
    finally:
        await engine.dispose()


def _control_event_index(
    events: list[tuple[str, tuple[str, ...]]],
    *,
    method: str,
    name: str,
) -> int:
    for index, (event_method, names) in enumerate(events):
        if event_method == method and name in names:
            return index
    raise AssertionError(f"{method} {name} was not recorded")
