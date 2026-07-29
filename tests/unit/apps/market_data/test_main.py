import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from typer.testing import CliRunner

from crypto_momentum_lab.apps.market_data import main
from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop

runner = CliRunner()


def fixture_snapshot() -> UniverseSnapshot:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    candidate = MarketCandidate(
        "BTCUSDT",
        Decimal("100"),
        Decimal("110"),
        at,
    )
    rank = RankEntry(
        "BTCUSDT",
        Decimal("0.1"),
        1,
        RankingSide.GAINER,
    )
    return UniverseSnapshot(
        snapshot_id=UUID("00000000-0000-0000-0000-000000000001"),
        observed_at=at,
        utc_day=at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(candidate,),
            gainers=(rank,),
            losers=(),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                "BTCUSDT",
                MembershipStatus.TARGET,
                RankingSide.GAINER,
                None,
            ),
        ),
    )


def test_refresh_command_prints_snapshot_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[datetime] = []

    async def fake_refresh_once(config_path, observed_at):
        calls.append(observed_at)
        return fixture_snapshot()

    monkeypatch.setattr(main, "refresh_once", fake_refresh_once)
    result = runner.invoke(
        main.app,
        [
            "refresh-universe",
            "--config",
            "configs/environments/research.yaml",
            "--at",
            "2026-06-14T11:01:00Z",
        ],
    )

    assert result.exit_code == 0
    assert calls == [datetime(2026, 6, 14, 11, 1, tzinfo=UTC)]
    assert "target=1" in result.stdout
    assert "monitoring=1" in result.stdout
    assert "excluded=0" in result.stdout


def test_refresh_command_rejects_invalid_timestamp() -> None:
    result = runner.invoke(
        main.app,
        ["refresh-universe", "--at", "not-a-time"],
    )

    assert result.exit_code != 0


def test_parse_paper_exit_run_ids_normalizes_csv() -> None:
    assert main.parse_paper_exit_run_ids(
        " run-1,run-2, run-1, ,"
    ) == frozenset({"run-1", "run-2"})


def test_run_market_data_uses_combined_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[Path] = []

    async def fake_run(config_path: Path) -> None:
        called.append(config_path)

    monkeypatch.setattr(main, "run_market_data", fake_run)
    result = runner.invoke(
        main.app,
        [
            "run-market-data",
            "--config",
            "configs/environments/research.yaml",
        ],
    )

    assert result.exit_code == 0
    assert called == [Path("configs/environments/research.yaml")]


async def test_scheduler_propagates_cancellation_cleanly() -> None:
    class FakeService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            raise AssertionError("refresh must not run after cancellation")

    async def cancelled_sleep(seconds: float) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_scheduler_loop(
            FakeService(),
            activation_minute=1,
            clock=lambda: datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
            sleeper=cancelled_sleep,
        )


async def test_scheduler_retries_failed_refresh_without_losing_schedule() -> None:
    refresh_calls: list[datetime] = []
    sleep_calls: list[float] = []

    class FlakyService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            refresh_calls.append(observed_at)
            if len(refresh_calls) == 1:
                raise RuntimeError("temporary database failure")
            raise asyncio.CancelledError

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 4:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_scheduler_loop(
            FlakyService(),
            activation_minute=1,
            retry_delay_seconds=0.25,
            clock=lambda: datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
            sleeper=record_sleep,
        )

    assert refresh_calls == [
        datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
        datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
    ]
    assert 0.25 in sleep_calls


async def test_paper_exit_reconcile_retries_transient_failure() -> None:
    calls = 0
    sleep_calls: list[float] = []

    class FlakyObserver:
        async def refresh_protected_symbols(self) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary database failure")
            raise asyncio.CancelledError

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 4:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await main.reconcile_paper_exit_subscriptions(
            FlakyObserver(),
            interval_seconds=0.1,
            retry_delay_seconds=0.25,
            sleeper=record_sleep,
        )

    assert calls == 2
    assert 0.25 in sleep_calls


async def test_capture_observer_applies_membership_symbols() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=1,
    )
    snapshot = fixture_snapshot()

    await observer.snapshot_updated(snapshot)

    assert capture.calls == [
        (
            frozenset({"BTCUSDT"}),
            (CaptureStream.AGG_TRADE,),
            2,
        )
    ]


async def test_capture_observer_keeps_open_position_symbols_subscribed() -> None:
    class FakeCapture:
        def __init__(self) -> None:
            self.calls = []

        async def apply_symbols(self, symbols, *, streams, generation) -> None:
            self.calls.append((symbols, streams, generation))

    protected_symbols = frozenset({"OLDUSDT"})

    async def load_protected_symbols() -> frozenset[str]:
        return protected_symbols

    capture = FakeCapture()
    observer = main.CaptureUniverseObserver(
        capture,
        streams=(CaptureStream.AGG_TRADE,),
        initial_generation=3,
        protected_symbol_loader=load_protected_symbols,
    )

    await observer.snapshot_updated(fixture_snapshot())
    await observer.refresh_protected_symbols()

    assert capture.calls == [
        (
            frozenset({"BTCUSDT", "OLDUSDT"}),
            (CaptureStream.AGG_TRADE,),
            4,
        )
    ]

    protected_symbols = frozenset()
    await observer.refresh_protected_symbols()

    assert capture.calls[-1] == (
        frozenset({"BTCUSDT"}),
        (CaptureStream.AGG_TRADE,),
        5,
    )


async def test_logging_refresh_service_times_out_stalled_refresh() -> None:
    class StalledRefreshService:
        async def refresh(self, *, observed_at: datetime) -> UniverseSnapshot:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    service = main.LoggingRefreshService(
        StalledRefreshService(),
        timeout_seconds=0.001,
    )

    with pytest.raises(TimeoutError):
        await service.refresh(
            observed_at=datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
        )


async def test_market_data_watchdog_rejects_missing_startup_data() -> None:
    now = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    clock_values = iter((now, now + timedelta(seconds=121)))

    async def no_sleep(_: float) -> None:
        return None

    with pytest.raises(main.MarketDataStaleError, match="no market data"):
        await main.monitor_market_data_freshness(
            latest_observed_at=lambda: None,
            startup_grace_seconds=120,
            stale_after_seconds=120,
            check_interval_seconds=1,
            clock=lambda: next(clock_values),
            sleeper=no_sleep,
        )


async def test_market_data_watchdog_rejects_stale_stream() -> None:
    now = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)

    async def no_sleep(_: float) -> None:
        return None

    with pytest.raises(main.MarketDataStaleError, match="market data stale"):
        await main.monitor_market_data_freshness(
            latest_observed_at=lambda: now - timedelta(seconds=121),
            startup_grace_seconds=120,
            stale_after_seconds=120,
            check_interval_seconds=1,
            clock=lambda: now,
            sleeper=no_sleep,
        )
