import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from typer.testing import CliRunner

from crypto_momentum_lab.apps.market_data import main
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
